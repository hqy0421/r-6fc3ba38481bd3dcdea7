package generation

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// Plan is the resolved set of Parquet files that make up one manifest state.
// Every file is named explicitly: a read compiles the plan into a DuckDB
// relation, so a file that appears in storage but was not part of the pinned
// state can never be picked up.
type Plan struct {
	// Generation is 0 for the live manifest and the saved generation number
	// for a pinned read.
	Generation int64
	Tables     map[string]TablePlan
}

// TablePlan lists, in read order, the base file followed by the delta files
// that advance it to the plan's epoch.
type TablePlan struct {
	Path             string
	BaseEpoch        int64
	LatestDeltaEpoch int64
	Files            []string
	// Deltas is the delta subset of Files (base excluded), oldest first.
	Deltas []string
	// RowCount is the recorded row count for a pinned plan (0 for live).
	RowCount int64
}

// MissingSegment names one segment file the plan expects but storage does not
// have. Kind is "base" or "delta".
type MissingSegment struct {
	Table string
	Kind  string
	Epoch int64
	Path  string
}

// MissingSegmentsError reports every segment of a pinned plan that is absent
// from storage. It is deliberately fatal to the read: substituting a newer
// base or a newer delta would return a different point in time while looking
// like the pinned one.
type MissingSegmentsError struct {
	Generation int64
	Segments   []MissingSegment
}

func (e *MissingSegmentsError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "generation %d is missing %d segment file(s); refusing to substitute files from another generation:", e.Generation, len(e.Segments))
	for _, s := range e.Segments {
		fmt.Fprintf(&b, "\n  - %s: %s epoch %d missing at %s", s.Table, s.Kind, s.Epoch, s.Path)
	}
	return b.String()
}

// PlanFromRecord validates a saved record and returns the read plan it fixes.
func PlanFromRecord(r *Record) (*Plan, error) {
	if r == nil || r.Manifest == nil {
		return nil, fmt.Errorf("saved generation has no manifest")
	}
	p := &Plan{Generation: r.Generation, Tables: make(map[string]TablePlan, len(r.Tables))}

	for name, seg := range r.Tables {
		mt, ok := r.Manifest.Tables[name]
		if !ok {
			return nil, fmt.Errorf("generation %d: table %s is inventoried but absent from the saved manifest", r.Generation, name)
		}
		if mt.Status != "active" {
			return nil, fmt.Errorf("generation %d: table %s is inventoried but has status %q in the saved manifest", r.Generation, name, mt.Status)
		}
		if seg.Path != mt.Path {
			return nil, fmt.Errorf("generation %d: table %s path %q does not match manifest path %q", r.Generation, name, seg.Path, mt.Path)
		}
		if seg.BaseEpoch != mt.BaseEpoch || seg.LatestDeltaEpoch != mt.LatestDeltaEpoch {
			return nil, fmt.Errorf("generation %d: table %s epochs (%d,%d) do not match manifest (%d,%d)",
				r.Generation, name, seg.BaseEpoch, seg.LatestDeltaEpoch, mt.BaseEpoch, mt.LatestDeltaEpoch)
		}

		wantBase := BasePath(seg.Path, seg.BaseEpoch)
		if seg.Base == "" || seg.Base != wantBase {
			return nil, fmt.Errorf("generation %d: table %s base %q is not the base for epoch %d (%s)",
				r.Generation, name, seg.Base, seg.BaseEpoch, wantBase)
		}

		tp := TablePlan{
			Path:             seg.Path,
			BaseEpoch:        seg.BaseEpoch,
			LatestDeltaEpoch: seg.LatestDeltaEpoch,
			RowCount:         seg.RowCount,
			Files:            []string{seg.Base},
		}

		seen := make(map[string]struct{}, len(seg.Deltas))
		last := int64(-1)
		for _, d := range seg.Deltas {
			if path.Dir(d) != path.Join(seg.Path, "deltas") {
				return nil, fmt.Errorf("generation %d: table %s delta %q is outside %s/deltas", r.Generation, name, d, seg.Path)
			}
			n, err := ParseDeltaEpoch(d)
			if err != nil {
				return nil, fmt.Errorf("generation %d: table %s: %w", r.Generation, name, err)
			}
			if n <= seg.BaseEpoch || n > seg.LatestDeltaEpoch {
				return nil, fmt.Errorf("generation %d: table %s delta %q (epoch %d) is outside (%d, %d]",
					r.Generation, name, d, n, seg.BaseEpoch, seg.LatestDeltaEpoch)
			}
			if n <= last {
				return nil, fmt.Errorf("generation %d: table %s deltas are not in strictly ascending epoch order (%d after %d)",
					r.Generation, name, n, last)
			}
			if _, dup := seen[d]; dup {
				return nil, fmt.Errorf("generation %d: table %s lists delta %q twice", r.Generation, name, d)
			}
			seen[d] = struct{}{}
			last = n
			tp.Deltas = append(tp.Deltas, d)
			tp.Files = append(tp.Files, d)
		}

		p.Tables[name] = tp
	}
	return p, nil
}

// LoadPlan loads a saved generation and compiles its read plan.
func LoadPlan(ctx context.Context, sink ports.Sink, gen int64) (*Record, *Plan, error) {
	r, err := Load(ctx, sink, gen)
	if err != nil {
		return nil, nil, err
	}
	p, err := PlanFromRecord(r)
	if err != nil {
		return nil, nil, err
	}
	return r, p, nil
}

// Verify checks that every file the plan names exists in storage. It reports
// all missing segments at once so an operator can see the full damage rather
// than fixing them one run at a time.
func Verify(ctx context.Context, sink ports.Sink, p *Plan) error {
	names := make([]string, 0, len(p.Tables))
	for name := range p.Tables {
		names = append(names, name)
	}
	sort.Strings(names)

	segments := make([]MissingSegment, 0)
	for _, name := range names {
		tp := p.Tables[name]
		files, err := sink.ListFiles(ctx, tp.Path+"/")
		if err != nil {
			return fmt.Errorf("list %s: %w", tp.Path, err)
		}
		present := make(map[string]struct{}, len(files))
		for _, f := range files {
			present[f] = struct{}{}
		}
		if _, ok := present[tp.Files[0]]; !ok {
			segments = append(segments, MissingSegment{Table: name, Kind: "base", Epoch: tp.BaseEpoch, Path: tp.Files[0]})
		}
		for _, d := range tp.Deltas {
			if _, ok := present[d]; !ok {
				n, _ := ParseDeltaEpoch(d)
				segments = append(segments, MissingSegment{Table: name, Kind: "delta", Epoch: n, Path: d})
			}
		}
	}
	if len(segments) > 0 {
		return &MissingSegmentsError{Generation: p.Generation, Segments: segments}
	}
	return nil
}

// LivePlan resolves the live manifest into an explicit file list. The base is
// always the single file named by BaseEpoch — never an older or newer base —
// and deltas are limited to (BaseEpoch, LatestDeltaEpoch]. Live reads tolerate
// per-table epoch gaps (the streamer only writes a delta for tables that had
// events), so no missing-delta check is possible here; a pinned plan is used
// when the caller needs that guarantee.
func LivePlan(ctx context.Context, sink ports.Sink, m *manifest.Manifest) (*Plan, error) {
	active := m.ActiveTables()
	p := &Plan{Generation: 0, Tables: make(map[string]TablePlan, len(active))}

	for name, t := range active {
		if t.Path == "" {
			return nil, fmt.Errorf("table %s has no storage path in the manifest", name)
		}
		if t.BaseEpoch > t.LatestDeltaEpoch {
			return nil, fmt.Errorf("table %s has base_epoch %d > latest_delta_epoch %d", name, t.BaseEpoch, t.LatestDeltaEpoch)
		}
		tp := TablePlan{
			Path:             t.Path,
			BaseEpoch:        t.BaseEpoch,
			LatestDeltaEpoch: t.LatestDeltaEpoch,
			RowCount:         t.RowCount,
			Files:            []string{BasePath(t.Path, t.BaseEpoch)},
		}

		files, err := sink.ListFiles(ctx, t.Path+"/deltas/")
		if err != nil {
			return nil, fmt.Errorf("list deltas for %s: %w", name, err)
		}
		type delta struct {
			path  string
			epoch int64
		}
		var deltas []delta
		for _, f := range files {
			n, err := ParseDeltaEpoch(f)
			if err != nil {
				continue // stray object, not a delta
			}
			if n > t.BaseEpoch && n <= t.LatestDeltaEpoch {
				deltas = append(deltas, delta{path: f, epoch: n})
			}
		}
		sort.Slice(deltas, func(i, j int) bool { return deltas[i].epoch < deltas[j].epoch })
		for _, d := range deltas {
			tp.Deltas = append(tp.Deltas, d.path)
			tp.Files = append(tp.Files, d.path)
		}
		p.Tables[name] = tp
	}
	return p, nil
}
