package internal

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/filesystem"
	"github.com/burnside-project/pg-cdc-core/internal/generation"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// testColumns is the schema used by the compaction generation tests.
func testColumns() []manifest.Column {
	return []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
		{Name: "name", PgType: "text", ParquetType: "UTF8", Nullable: true},
	}
}

func writeBase(t *testing.T, dir string, epoch int64, rows []writer.Row) {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(generation.BasePath("public.orders", epoch)))
	if err := writer.WriteBaseParquet(p, rows, testColumns()); err != nil {
		t.Fatalf("write base %d: %v", epoch, err)
	}
}

func writeDelta(t *testing.T, dir string, epoch int64, rows []writer.Row) {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(generation.DeltaPath("public.orders", epoch)))
	if err := writer.WriteDeltaParquet(p, rows, testColumns(), epoch); err != nil {
		t.Fatalf("write delta %d: %v", epoch, err)
	}
}

func testManifest(latest int64) *manifest.Manifest {
	return &manifest.Manifest{
		Version: 1,
		Tables: map[string]manifest.Table{
			"public.orders": {
				Status: "active", Path: "public.orders", PrimaryKey: []string{"id"},
				BaseEpoch: 0, LatestDeltaEpoch: latest, RowCount: 1,
			},
		},
		Schemas: map[string]manifest.Schema{
			"public.orders": {Version: 1, Columns: testColumns()},
		},
	}
}

func exists(t *testing.T, dir, rel string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, filepath.FromSlash(rel)))
	return err == nil
}

// TestCompactKeepsDeltasReferencedBySavedGeneration is the regression guard for
// the pinned-read contract: compaction used to delete every merged delta, which
// would leave a saved generation pointing at files that no longer exist. Deltas
// a saved generation references must survive so the pinned read still resolves.
func TestCompactKeepsDeltasReferencedBySavedGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}

	writeBase(t, dir, 0, []writer.Row{{"id": int64(1), "name": "widget"}})
	writeDelta(t, dir, 1, []writer.Row{{"id": int64(2), "name": "gadget", "__op": "I"}})
	writeDelta(t, dir, 2, []writer.Row{{"id": int64(3), "name": "gizmo", "__op": "I"}})

	// Save the state at latest=2, then advance the streamer to latest=3.
	if err := sink.WriteManifest(ctx, testManifest(2)); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if _, err := generation.Archive(ctx, sink); err != nil {
		t.Fatalf("archive: %v", err)
	}
	writeDelta(t, dir, 3, []writer.Row{{"id": int64(4), "name": "doohickey", "__op": "I"}})
	if err := sink.WriteManifest(ctx, testManifest(3)); err != nil {
		t.Fatalf("advance manifest: %v", err)
	}

	// Compact: merges deltas 1,2,3 into base_000003.
	if err := Compact(ctx, sink); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// The generation's deltas must still be present.
	for _, e := range []int64{1, 2} {
		if !exists(t, dir, generation.DeltaPath("public.orders", e)) {
			t.Errorf("delta epoch %d referenced by generation 1 was deleted by compaction", e)
		}
	}
	// Delta 3 was not part of any saved generation, so it is free to disappear.
	if exists(t, dir, generation.DeltaPath("public.orders", 3)) {
		t.Errorf("delta epoch 3 (not in any generation) should have been deleted")
	}

	// The pinned generation must still resolve and verify.
	_, plan, err := generation.LoadPlan(ctx, sink, 1)
	if err != nil {
		t.Fatalf("load pinned plan: %v", err)
	}
	if err := generation.Verify(ctx, sink, plan); err != nil {
		t.Fatalf("pinned generation no longer verifies after compaction: %v", err)
	}
}

// TestReadBaseUsesManifestEpochNotLatestFile guards the second half of the
// contract: compaction reads exactly base_<BaseEpoch>.parquet, never "the
// newest file in base/". A stray newer base must be ignored.
func TestReadBaseUsesManifestEpochNotLatestFile(t *testing.T) {
	dir := t.TempDir()
	writeBase(t, dir, 0, []writer.Row{{"id": int64(1), "name": "old"}})
	writeBase(t, dir, 5, []writer.Row{{"id": int64(99), "name": "stray"}})

	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}

	table := manifest.Table{Path: "public.orders", BaseEpoch: 0}
	rows, err := readBase(context.Background(), sink, table)
	if err != nil {
		t.Fatalf("readBase: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "old" {
		t.Fatalf("readBase returned %v; expected the base named by BaseEpoch", rows)
	}
}

// TestReadBaseMissingNamesTheGap ensures a base the manifest names but storage
// lacks is reported as missing (with the available epochs), not silently
// replaced by whatever base is present.
func TestReadBaseMissingNamesTheGap(t *testing.T) {
	dir := t.TempDir()
	writeBase(t, dir, 5, []writer.Row{{"id": int64(99), "name": "stray"}})

	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}

	table := manifest.Table{Path: "public.orders", BaseEpoch: 0}
	_, err = readBase(context.Background(), sink, table)
	if err == nil {
		t.Fatal("expected missing-base error")
	}
	if msg := err.Error(); !strings.Contains(msg, "base_000000.parquet") || !strings.Contains(msg, "not found") {
		t.Errorf("error should name the missing base, got: %v", err)
	}
}
