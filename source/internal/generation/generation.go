// Package generation stores and reads immutable snapshots of the manifest.
//
// pg-cdc rewrites manifest.json on every flush and compaction, so the live
// manifest only ever describes the newest state. An analyst who wants to read
// a previously published version ("last week's numbers") needs the manifest as
// it existed then, plus the exact base and delta files that constituted it.
//
// Archive writes a numbered, immutable record of the live manifest together
// with the segment inventory (base file + delta files) for every active table.
// Reads compiled from a record (see CompileRecord) use only the recorded
// segments: if a segment has been removed from storage the read fails and
// names the missing segment. It never substitutes a file from a newer
// generation, because that would silently mix two different points in time.
package generation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

const (
	// Dir is the storage prefix holding saved manifest generations.
	Dir = "manifest-generations"

	// RecordVersion is the on-disk schema version of a generation record.
	RecordVersion = 1

	kind = "pg-cdc-manifest-generation"
)

// ErrNotFound is returned when a requested generation has never been saved
// (or has been pruned). Callers must not fall back to the live manifest.
var ErrNotFound = errors.New("manifest generation not found")

// ErrExists is returned when a write would overwrite an existing generation.
// Generations are immutable; the writer retries with the next number.
var ErrExists = errors.New("manifest generation already exists")

// Record is one immutable, saved manifest generation. It carries the manifest
// itself so the record is self-contained (an analyst can hand over a copy) and
// the exact base + delta segments that make up that point in time.
type Record struct {
	Kind       string                   `json:"kind"`
	Version    int                      `json:"version"`
	Generation int64                    `json:"generation"`
	SavedAt    time.Time                `json:"saved_at"`
	Manifest   *manifest.Manifest       `json:"manifest"`
	Tables     map[string]TableSegments `json:"tables"`
}

// TableSegments is the file inventory for one active table at the moment the
// generation was saved. Paths are relative to the storage root, matching
// ports.Sink.
type TableSegments struct {
	Path             string   `json:"path"`
	BaseEpoch        int64    `json:"base_epoch"`
	LatestDeltaEpoch int64    `json:"latest_delta_epoch"`
	RowCount         int64    `json:"row_count,omitempty"`
	Base             string   `json:"base"`
	Deltas           []string `json:"deltas"`
}

// SegmentCount returns the number of Parquet files in the record.
func (r *Record) SegmentCount() int {
	n := 0
	for _, t := range r.Tables {
		if t.Base != "" {
			n++
		}
		n += len(t.Deltas)
	}
	return n
}

// Filename returns the storage path for generation gen.
func Filename(gen int64) string {
	return path.Join(Dir, fmt.Sprintf("gen-%06d.json", gen))
}

// BasePath returns the storage path of a table's base file for an epoch.
func BasePath(tablePath string, epoch int64) string {
	return path.Join(tablePath, "base", fmt.Sprintf("base_%06d.parquet", epoch))
}

// DeltaPath returns the storage path of a table's delta file for an epoch.
func DeltaPath(tablePath string, epoch int64) string {
	return path.Join(tablePath, "deltas", fmt.Sprintf("epoch=%06d.parquet", epoch))
}

// parseBaseEpoch parses "base_000042.parquet" into 42.
func ParseBaseEpoch(file string) (int64, error) {
	base := path.Base(file)
	if !strings.HasPrefix(base, "base_") || !strings.HasSuffix(base, ".parquet") {
		return 0, fmt.Errorf("not a base filename: %q", file)
	}
	n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(base, "base_"), ".parquet"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse base epoch from %q: %w", file, err)
	}
	return n, nil
}

// parseDeltaEpoch parses "epoch=000042.parquet" into 42.
func ParseDeltaEpoch(file string) (int64, error) {
	base := path.Base(file)
	if !strings.HasPrefix(base, "epoch=") || !strings.HasSuffix(base, ".parquet") {
		return 0, fmt.Errorf("not a delta filename: %q", file)
	}
	n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(base, "epoch="), ".parquet"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse delta epoch from %q: %w", file, err)
	}
	return n, nil
}

// index maps saved generation numbers to their storage paths.
func index(ctx context.Context, sink ports.Sink) (map[int64]string, error) {
	files, err := sink.ListFiles(ctx, Dir+"/")
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", Dir, err)
	}
	out := make(map[int64]string, len(files))
	for _, f := range files {
		if path.Dir(f) != Dir {
			continue
		}
		base := path.Base(f)
		if !strings.HasPrefix(base, "gen-") || !strings.HasSuffix(base, ".json") {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(base, "gen-"), ".json"), 10, 64)
		if err != nil {
			continue
		}
		out[n] = f
	}
	return out, nil
}

// List returns the saved generation numbers in ascending order.
func List(ctx context.Context, sink ports.Sink) ([]int64, error) {
	idx, err := index(ctx, sink)
	if err != nil {
		return nil, err
	}
	gens := make([]int64, 0, len(idx))
	for n := range idx {
		gens = append(gens, n)
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })
	return gens, nil
}

// NextGeneration returns the number a newly archived generation will get.
func NextGeneration(ctx context.Context, sink ports.Sink) (int64, error) {
	gens, err := List(ctx, sink)
	if err != nil {
		return 0, err
	}
	if len(gens) == 0 {
		return 1, nil
	}
	return gens[len(gens)-1] + 1, nil
}

// Load reads the saved generation gen. A missing generation returns an error
// wrapping ErrNotFound that lists what is available — callers must surface it
// rather than falling back to the live manifest.
func Load(ctx context.Context, sink ports.Sink, gen int64) (*Record, error) {
	if gen < 1 {
		return nil, fmt.Errorf("invalid generation %d: generations start at 1", gen)
	}
	idx, err := index(ctx, sink)
	if err != nil {
		return nil, err
	}
	p, ok := idx[gen]
	if !ok {
		return nil, fmt.Errorf("%w: generation %d is not in storage (available: %v); refusing to fall back to the live manifest",
			ErrNotFound, gen, available(idx))
	}
	return LoadPath(ctx, sink, p)
}

func available(idx map[int64]string) []int64 {
	gens := make([]int64, 0, len(idx))
	for n := range idx {
		gens = append(gens, n)
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })
	if len(gens) == 0 {
		return nil
	}
	return gens
}

// LoadPath reads a saved generation from an explicit storage path (for
// example a copy of a generation record handed to the analyst). Only
// generation records are accepted: a bare manifest.json is rejected, because
// it does not record which delta files existed when it was written, so reading
// from it could silently skip deleted segments.
func LoadPath(ctx context.Context, sink ports.Sink, p string) (*Record, error) {
	rc, err := sink.ReadFile(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("read saved manifest %q: %w", p, err)
	}
	defer func() { _ = rc.Close() }()

	var r Record
	dec := json.NewDecoder(rc)
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("parse saved manifest %q: %w", p, err)
	}
	if r.Kind != kind {
		return nil, fmt.Errorf("%q is not a saved manifest generation (expected kind %q; save one with `pg-cdc generations save` while that state was live)", p, kind)
	}
	if r.Version != RecordVersion {
		return nil, fmt.Errorf("saved manifest %q has unsupported record version %d (want %d)", p, r.Version, RecordVersion)
	}
	if r.Generation < 1 {
		return nil, fmt.Errorf("saved manifest %q has invalid generation %d", p, r.Generation)
	}
	if r.Manifest == nil {
		return nil, fmt.Errorf("saved manifest %q has no manifest body", p)
	}
	return &r, nil
}

// Archive snapshots the live manifest as a new immutable generation. It
// inventories the segment files the manifest currently refers to, verifies the
// manifest did not change underneath it (a concurrent flush or compaction
// would otherwise produce a record whose inventory never coexisted), and
// persists the record under the next free generation number.
func Archive(ctx context.Context, sink ports.Sink) (*Record, error) {
	for attempt := 0; attempt < 3; attempt++ {
		before, err := sink.ReadManifest(ctx)
		if err != nil {
			return nil, fmt.Errorf("read manifest: %w", err)
		}
		tables, err := inventory(ctx, sink, before)
		if err != nil {
			return nil, err
		}
		after, err := sink.ReadManifest(ctx)
		if err != nil {
			return nil, fmt.Errorf("re-read manifest: %w", err)
		}
		if !sameState(before, after) {
			continue // a writer moved the manifest mid-archive; retry
		}

		gen, err := NextGeneration(ctx, sink)
		if err != nil {
			return nil, err
		}
		r := &Record{
			Kind:       kind,
			Version:    RecordVersion,
			Generation: gen,
			SavedAt:    time.Now().UTC(),
			Manifest:   before,
			Tables:     tables,
		}
		// Validate the record before it is persisted, so a generation can
		// never be saved in a state its own reader would reject.
		plan, err := PlanFromRecord(r)
		if err != nil {
			return nil, fmt.Errorf("build generation %d: %w", gen, err)
		}
		if err := persist(ctx, sink, r); err != nil {
			if errors.Is(err, ErrExists) {
				continue // another archiver took this number; try the next
			}
			return nil, err
		}
		// Confirm the record just written verifies against storage. If a
		// compaction raced in and removed one of the segments between
		// inventory and persist, the record is unusable — remove it rather
		// than leave a pinned read that can only ever fail.
		if err := Verify(ctx, sink, plan); err != nil {
			_ = sink.DeleteFile(ctx, Filename(gen))
			return nil, fmt.Errorf("generation %d could not be saved consistently: %w", gen, err)
		}
		return r, nil
	}
	return nil, fmt.Errorf("manifest changed repeatedly while archiving a generation; retry when the streamer is quieter")
}

// persist writes r, refusing to overwrite an existing generation.
func persist(ctx context.Context, sink ports.Sink, r *Record) error {
	idx, err := index(ctx, sink)
	if err != nil {
		return err
	}
	if _, exists := idx[r.Generation]; exists {
		return fmt.Errorf("%w: generation %d", ErrExists, r.Generation)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		return fmt.Errorf("serialize generation %d: %w", r.Generation, err)
	}
	if err := sink.WriteFile(ctx, Filename(r.Generation), &buf); err != nil {
		return fmt.Errorf("write generation %d: %w", r.Generation, err)
	}
	return nil
}

// inventory records the base + delta segments each active table currently has.
// The base named by BaseEpoch must exist; a manifest that points at a missing
// base is not archivable (and a read of it would be incomplete).
func inventory(ctx context.Context, sink ports.Sink, m *manifest.Manifest) (map[string]TableSegments, error) {
	active := m.ActiveTables()
	out := make(map[string]TableSegments, len(active))
	var missing []MissingSegment

	names := make([]string, 0, len(active))
	for name := range active {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		t := active[name]
		if t.Path == "" {
			return nil, fmt.Errorf("table %s has no storage path in the manifest", name)
		}
		if t.BaseEpoch > t.LatestDeltaEpoch {
			return nil, fmt.Errorf("table %s has base_epoch %d > latest_delta_epoch %d", name, t.BaseEpoch, t.LatestDeltaEpoch)
		}

		files, err := sink.ListFiles(ctx, t.Path+"/")
		if err != nil {
			return nil, fmt.Errorf("list segments for %s: %w", name, err)
		}
		present := make(map[string]struct{}, len(files))
		for _, f := range files {
			present[f] = struct{}{}
		}

		base := BasePath(t.Path, t.BaseEpoch)
		if _, ok := present[base]; !ok {
			missing = append(missing, MissingSegment{Table: name, Kind: "base", Epoch: t.BaseEpoch, Path: base})
		}

		var deltas []string
		for _, f := range files {
			if path.Dir(f) != path.Join(t.Path, "deltas") {
				continue
			}
			n, err := ParseDeltaEpoch(f)
			if err != nil {
				continue // not a delta file (stray object)
			}
			if n > t.BaseEpoch && n <= t.LatestDeltaEpoch {
				deltas = append(deltas, f)
			}
		}
		sort.Slice(deltas, func(i, j int) bool {
			a, _ := ParseDeltaEpoch(deltas[i])
			b, _ := ParseDeltaEpoch(deltas[j])
			return a < b
		})

		out[name] = TableSegments{
			Path:             t.Path,
			BaseEpoch:        t.BaseEpoch,
			LatestDeltaEpoch: t.LatestDeltaEpoch,
			RowCount:         t.RowCount,
			Base:             base,
			Deltas:           deltas,
		}
	}

	if len(missing) > 0 {
		return nil, &MissingSegmentsError{Segments: missing}
	}
	return out, nil
}

// sameState reports whether two manifest reads describe the same published
// state, i.e. no writer advanced any table between the reads.
func sameState(a, b *manifest.Manifest) bool {
	if !a.UpdatedAt.Equal(b.UpdatedAt) || len(a.Tables) != len(b.Tables) {
		return false
	}
	for name, ta := range a.Tables {
		tb, ok := b.Tables[name]
		if !ok || ta.Status != tb.Status || ta.Path != tb.Path ||
			ta.BaseEpoch != tb.BaseEpoch || ta.LatestDeltaEpoch != tb.LatestDeltaEpoch {
			return false
		}
	}
	return true
}

// ProtectedSegments returns every segment path referenced by a saved
// generation. Compaction consults this set before deleting merged delta files:
// a delta that is still part of a saved generation must survive, otherwise
// that generation becomes unreadable.
func ProtectedSegments(ctx context.Context, sink ports.Sink) (map[string]struct{}, error) {
	gens, err := List(ctx, sink)
	if err != nil {
		return nil, err
	}
	protected := map[string]struct{}{}
	for _, gen := range gens {
		r, err := Load(ctx, sink, gen)
		if err != nil {
			return nil, err
		}
		for _, t := range r.Tables {
			if t.Base != "" {
				protected[t.Base] = struct{}{}
			}
			for _, d := range t.Deltas {
				protected[d] = struct{}{}
			}
		}
	}
	return protected, nil
}

// Prune deletes saved generation records older than cutoff and returns the
// generation numbers removed. Records are the only thing deleted here; the
// Parquet segments they referenced are reclaimed by the next compaction once
// no remaining generation refers to them.
func Prune(ctx context.Context, sink ports.Sink, cutoff time.Time) ([]int64, error) {
	gens, err := List(ctx, sink)
	if err != nil {
		return nil, err
	}
	var removed []int64
	for _, gen := range gens {
		r, err := Load(ctx, sink, gen)
		if err != nil {
			return removed, err
		}
		if !r.SavedAt.Before(cutoff) {
			continue
		}
		if err := sink.DeleteFile(ctx, Filename(gen)); err != nil {
			return removed, fmt.Errorf("delete generation %d: %w", gen, err)
		}
		removed = append(removed, gen)
	}
	return removed, nil
}
