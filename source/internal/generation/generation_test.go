package generation

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/burnside-project/burnside-go/manifest"
)

// memSink is an in-memory ports.Sink covering the read/write/list/delete
// surface generation logic needs.
type memSink struct {
	files   map[string][]byte
	deleted []string
}

func newMemSink() *memSink { return &memSink{files: map[string][]byte{}} }

func (m *memSink) WriteFile(_ context.Context, p string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.files[p] = b
	return nil
}

func (m *memSink) ReadFile(_ context.Context, p string) (io.ReadCloser, error) {
	b, ok := m.files[p]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *memSink) ListFiles(_ context.Context, prefix string) ([]string, error) {
	var out []string
	for p := range m.files {
		if strings.HasPrefix(p, prefix) {
			out = append(out, p)
		}
	}
	return out, nil
}

func (m *memSink) DeleteFile(_ context.Context, p string) error {
	delete(m.files, p)
	m.deleted = append(m.deleted, p)
	return nil
}

func (m *memSink) Exists(_ context.Context, p string) (bool, error) {
	_, ok := m.files[p]
	return ok, nil
}

func (m *memSink) ReadManifest(context.Context) (*manifest.Manifest, error) {
	b, ok := m.files["manifest.json"]
	if !ok {
		return nil, os.ErrNotExist
	}
	return manifest.Read(bytes.NewReader(b))
}

func (m *memSink) WriteManifest(_ context.Context, mm *manifest.Manifest) error {
	var buf bytes.Buffer
	if err := mm.Write(&buf); err != nil {
		return err
	}
	m.files["manifest.json"] = buf.Bytes()
	return nil
}

func (m *memSink) Backend() string { return "memory" }

func mkManifest(base, latest int64) *manifest.Manifest {
	return &manifest.Manifest{
		Version: 1,
		Tables: map[string]manifest.Table{
			"public.orders": {
				Status:           "active",
				Path:             "public.orders",
				PrimaryKey:       []string{"id"},
				BaseEpoch:        base,
				LatestDeltaEpoch: latest,
				RowCount:         3,
			},
		},
		Schemas:   map[string]manifest.Schema{"public.orders": {Version: 1}},
		UpdatedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
}

func seed(m *memSink, base, latest int64) {
	_ = m.WriteManifest(context.Background(), mkManifest(base, latest))
	m.files[BasePath("public.orders", base)] = []byte("base")
	for e := base + 1; e <= latest; e++ {
		m.files[DeltaPath("public.orders", e)] = []byte("delta")
	}
}

func TestArchiveRecordsExactSegments(t *testing.T) {
	ctx := context.Background()
	s := newMemSink()
	// base 2, deltas 3..5 present; also a stale delta 1 from before the base.
	m := mkManifest(2, 5)
	_ = s.WriteManifest(ctx, m)
	s.files[BasePath("public.orders", 2)] = []byte("b")
	s.files[DeltaPath("public.orders", 1)] = []byte("old")
	for _, e := range []int64{3, 4, 5} {
		s.files[DeltaPath("public.orders", e)] = []byte("d")
	}

	r, err := Archive(ctx, s)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if r.Generation != 1 {
		t.Errorf("generation = %d, want 1", r.Generation)
	}
	seg := r.Tables["public.orders"]
	if seg.Base != "public.orders/base/base_000002.parquet" {
		t.Errorf("base = %q", seg.Base)
	}
	want := []string{
		"public.orders/deltas/epoch=000003.parquet",
		"public.orders/deltas/epoch=000004.parquet",
		"public.orders/deltas/epoch=000005.parquet",
	}
	if len(seg.Deltas) != len(want) {
		t.Fatalf("deltas = %v, want %v", seg.Deltas, want)
	}
	for i := range want {
		if seg.Deltas[i] != want[i] {
			t.Errorf("deltas[%d] = %q, want %q", i, seg.Deltas[i], want[i])
		}
	}
}

func TestArchiveRefusesMissingBase(t *testing.T) {
	ctx := context.Background()
	s := newMemSink()
	_ = s.WriteManifest(ctx, mkManifest(2, 2)) // no base file written

	_, err := Archive(ctx, s)
	if err == nil {
		t.Fatal("expected error for missing base")
	}
	var mse *MissingSegmentsError
	if !errors.As(err, &mse) {
		t.Fatalf("expected MissingSegmentsError, got %T: %v", err, err)
	}
	if len(mse.Segments) != 1 || mse.Segments[0].Kind != "base" {
		t.Errorf("segments = %+v", mse.Segments)
	}
}

func TestArchiveIsImmutable(t *testing.T) {
	ctx := context.Background()
	s := newMemSink()
	seed(s, 0, 1)

	r1, err := Archive(ctx, s)
	if err != nil {
		t.Fatalf("archive 1: %v", err)
	}
	// Advance the live manifest and archive again.
	seed(s, 0, 2)
	r2, err := Archive(ctx, s)
	if err != nil {
		t.Fatalf("archive 2: %v", err)
	}
	if r2.Generation != r1.Generation+1 {
		t.Errorf("generations %d then %d; want consecutive", r1.Generation, r2.Generation)
	}
	// The first record must still describe the old state.
	got, err := Load(ctx, s, r1.Generation)
	if err != nil {
		t.Fatalf("load 1: %v", err)
	}
	if len(got.Tables["public.orders"].Deltas) != 1 {
		t.Errorf("generation 1 now has %d deltas; record was overwritten", len(got.Tables["public.orders"].Deltas))
	}
}

func TestLoadMissingGenerationDoesNotFallBack(t *testing.T) {
	ctx := context.Background()
	s := newMemSink()
	seed(s, 0, 1)
	if _, err := Archive(ctx, s); err != nil {
		t.Fatalf("archive: %v", err)
	}

	_, err := Load(ctx, s, 99)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if !strings.Contains(err.Error(), "refusing to fall back") {
		t.Errorf("error should state it refuses to fall back: %v", err)
	}
}

func TestLoadRejectsBareManifest(t *testing.T) {
	ctx := context.Background()
	s := newMemSink()
	seed(s, 0, 1)

	_, err := LoadPath(ctx, s, "manifest.json")
	if err == nil {
		t.Fatal("expected bare manifest.json to be rejected")
	}
	if !strings.Contains(err.Error(), "not a saved manifest generation") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestVerifyReportsMissingDelta(t *testing.T) {
	ctx := context.Background()
	s := newMemSink()
	seed(s, 0, 3)

	r, err := Archive(ctx, s)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	p, err := PlanFromRecord(r)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if err := Verify(ctx, s, p); err != nil {
		t.Fatalf("verify before deletion: %v", err)
	}

	// Simulate a compaction that deleted a delta the generation still needs.
	delete(s.files, DeltaPath("public.orders", 2))

	err = Verify(ctx, s, p)
	if err == nil {
		t.Fatal("expected missing-segment error")
	}
	var mse *MissingSegmentsError
	if !errors.As(err, &mse) {
		t.Fatalf("expected MissingSegmentsError, got %T", err)
	}
	if len(mse.Segments) != 1 || mse.Segments[0].Epoch != 2 || mse.Segments[0].Kind != "delta" {
		t.Errorf("segments = %+v", mse.Segments)
	}
	// It must name the exact path — no substitution.
	if !strings.Contains(err.Error(), "epoch=000002.parquet") {
		t.Errorf("error should name the missing file: %v", err)
	}
}

func TestPlanFromRecordRejectsInconsistentRecord(t *testing.T) {
	good := func() *Record {
		mm := mkManifest(1, 2)
		return &Record{
			Kind:       kind,
			Version:    RecordVersion,
			Generation: 1,
			Manifest:   mm,
			Tables: map[string]TableSegments{
				"public.orders": {
					Path:             "public.orders",
					BaseEpoch:        1,
					LatestDeltaEpoch: 2,
					Base:             BasePath("public.orders", 1),
					Deltas:           []string{DeltaPath("public.orders", 2)},
				},
			},
		}
	}

	if _, err := PlanFromRecord(good()); err != nil {
		t.Fatalf("baseline record should plan: %v", err)
	}

	cases := map[string]func(*Record){
		"delta outside window": func(r *Record) {
			r.Tables["public.orders"] = TableSegments{
				Path: "public.orders", BaseEpoch: 1, LatestDeltaEpoch: 2,
				Base: BasePath("public.orders", 1),
				Deltas: []string{DeltaPath("public.orders", 9)},
			}
		},
		"delta at base epoch": func(r *Record) {
			r.Tables["public.orders"] = TableSegments{
				Path: "public.orders", BaseEpoch: 1, LatestDeltaEpoch: 2,
				Base: BasePath("public.orders", 1),
				Deltas: []string{DeltaPath("public.orders", 1)},
			}
		},
		"base mismatch": func(r *Record) {
			seg := r.Tables["public.orders"]
			seg.Base = BasePath("public.orders", 0)
			r.Tables["public.orders"] = seg
		},
		"epoch mismatch with manifest": func(r *Record) {
			seg := r.Tables["public.orders"]
			seg.LatestDeltaEpoch = 3
			r.Tables["public.orders"] = seg
		},
		"out of order deltas": func(r *Record) {
			r.Tables["public.orders"] = TableSegments{
				Path: "public.orders", BaseEpoch: 0, LatestDeltaEpoch: 3,
				Base:   BasePath("public.orders", 0),
				Deltas: []string{DeltaPath("public.orders", 3), DeltaPath("public.orders", 2)},
			}
			r.Manifest.Tables["public.orders"] = manifest.Table{Status: "active", Path: "public.orders", BaseEpoch: 0, LatestDeltaEpoch: 3}
		},
	}
	for name, mutate := range cases {
		r := good()
		mutate(r)
		if _, err := PlanFromRecord(r); err == nil {
			t.Errorf("%s: expected plan error", name)
		}
	}
}

func TestProtectedSegmentsAndPrune(t *testing.T) {
	ctx := context.Background()
	s := newMemSink()
	seed(s, 0, 2)
	r, err := Archive(ctx, s)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}

	protected, err := ProtectedSegments(ctx, s)
	if err != nil {
		t.Fatalf("protected: %v", err)
	}
	for _, want := range []string{
		BasePath("public.orders", 0),
		DeltaPath("public.orders", 1),
		DeltaPath("public.orders", 2),
	} {
		if _, ok := protected[want]; !ok {
			t.Errorf("segment %q not protected", want)
		}
	}

	// Nothing to prune yet: record is newer than the cutoff.
	removed, err := Prune(ctx, s, time.Now().Add(-time.Hour).UTC())
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if len(removed) != 0 {
		t.Errorf("pruned %v; expected none", removed)
	}

	// Prune with a future cutoff removes the record.
	removed, err = Prune(ctx, s, time.Now().Add(time.Hour).UTC())
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if len(removed) != 1 || removed[0] != r.Generation {
		t.Errorf("removed = %v, want [%d]", removed, r.Generation)
	}
}

func TestLivePlanNamesExactBase(t *testing.T) {
	ctx := context.Background()
	s := newMemSink()
	seed(s, 2, 4)
	// A newer base left behind by a failed compaction must not be picked up.
	s.files[BasePath("public.orders", 9)] = []byte("stray")

	m, err := s.ReadManifest(ctx)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	p, err := LivePlan(ctx, s, m)
	if err != nil {
		t.Fatalf("live plan: %v", err)
	}
	tp := p.Tables["public.orders"]
	if tp.Files[0] != BasePath("public.orders", 2) {
		t.Errorf("base = %q, want base_000002", tp.Files[0])
	}
	if len(tp.Deltas) != 2 {
		t.Errorf("deltas = %v, want epochs 3,4", tp.Deltas)
	}
	for _, f := range tp.Files {
		if strings.Contains(f, "base_000009") {
			t.Errorf("live plan picked up stray base: %v", tp.Files)
		}
	}
}

func TestArchiveFailsWhenManifestKeepsMoving(t *testing.T) {
	ctx := context.Background()
	s := &flappingSink{memSink: newMemSink()}
	seed(s.memSink, 0, 1)

	// Every read reports a different UpdatedAt, so no inventory is ever
	// consistent with a stable manifest. Archive must give up loudly rather
	// than persist a record whose segment list never coexisted with its
	// manifest.
	_, err := Archive(ctx, s)
	if err == nil {
		t.Fatal("expected archive to give up on a constantly-moving manifest")
	}
	if !strings.Contains(err.Error(), "manifest changed repeatedly") {
		t.Errorf("unexpected error: %v", err)
	}
	if s.reads != 6 { // 2 reads per attempt × 3 attempts
		t.Errorf("manifest reads = %d, want 6", s.reads)
	}
	if _, ok := s.files["manifest-generations/gen-000001.json"]; ok {
		t.Error("a generation record was persisted despite the instability")
	}
}

// flappingSink returns a manifest whose UpdatedAt changes on every read, to
// simulate a writer that never lets the archive settle.
type flappingSink struct {
	*memSink
	reads int
}

func (f *flappingSink) ReadManifest(ctx context.Context) (*manifest.Manifest, error) {
	f.reads++
	m, err := f.memSink.ReadManifest(ctx)
	if err != nil {
		return nil, err
	}
	m.UpdatedAt = m.UpdatedAt.Add(time.Duration(f.reads) * time.Second)
	return m, nil
}

func TestGenFilenameFormat(t *testing.T) {
	if got := Filename(7); got != "manifest-generations/gen-000007.json" {
		t.Errorf("Filename(7) = %q", got)
	}
	if got := filepath.Base(Filename(7)); got != "gen-000007.json" {
		t.Errorf("base = %q", got)
	}
}
