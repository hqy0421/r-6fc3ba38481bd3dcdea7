package mcpserver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/filesystem"
	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/generation"
)

// newDiskServer builds a server backed by a real filesystem sink rooted at dir,
// so saved generations exist as real files.
func newDiskServer(t *testing.T, dir string) *server {
	t.Helper()
	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("filesystem sink: %v", err)
	}
	return &server{
		cfg:  config.Config{Storage: config.StorageConfig{Type: "filesystem", Path: dir}},
		sink: sink,
	}
}

// writeFile is a small helper for seeding storage in tests.
func writeFile(t *testing.T, dir, rel, body string) {
	t.Helper()
	full := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

// seedGenerationState writes a live manifest at base 0, deltas 1..latest, then
// archives it as generation 1, then advances the live state to deltas 1..final
// so a pinned read can be distinguished from a live one.
func seedGenerationState(t *testing.T, dir string, pinnedLatest, liveLatest int64) *generation.Record {
	t.Helper()
	ctx := context.Background()
	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}

	m := func(latest int64) *manifest.Manifest {
		return &manifest.Manifest{
			Version: 1,
			Tables: map[string]manifest.Table{
				"public.orders": {
					Status: "active", Path: "public.orders", PrimaryKey: []string{"id"},
					BaseEpoch: 0, LatestDeltaEpoch: latest, RowCount: 10,
				},
			},
			Schemas: map[string]manifest.Schema{
				"public.orders": {Version: 1, Columns: []manifest.Column{
					{Name: "id", PgType: "bigint", ParquetType: "INT64"},
				}},
			},
		}
	}

	writeFile(t, dir, "public.orders/base/base_000000.parquet", "base")
	for e := int64(1); e <= liveLatest; e++ {
		writeFile(t, dir, generation.DeltaPath("public.orders", e), "delta")
	}

	if err := sink.WriteManifest(ctx, m(pinnedLatest)); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	r, err := generation.Archive(ctx, sink)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if err := sink.WriteManifest(ctx, m(liveLatest)); err != nil {
		t.Fatalf("advance manifest: %v", err)
	}
	return r
}

func TestListGenerations(t *testing.T) {
	dir := t.TempDir()
	seedGenerationState(t, dir, 2, 4)
	s := newDiskServer(t, dir)

	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_generations"}}`)
	text, isErr := resultText(t, resp)
	if isErr {
		t.Fatalf("unexpected error: %s", text)
	}
	if !strings.Contains(text, "generation 1") {
		t.Errorf("expected generation 1 in output, got: %s", text)
	}
}

func TestPinnedQueryMissingGenerationErrors(t *testing.T) {
	dir := t.TempDir()
	seedGenerationState(t, dir, 2, 4)
	s := newDiskServer(t, dir)

	// Generation 7 was never saved. The tool must error — not read the live
	// manifest, and not read generation 1.
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_tables","arguments":{"generation":7}}}`)
	text, isErr := resultText(t, resp)
	if !isErr {
		t.Fatalf("expected error for unknown generation, got: %s", text)
	}
	if !strings.Contains(text, "not in storage") || !strings.Contains(text, "refusing to fall back") {
		t.Errorf("error should refuse fallback, got: %s", text)
	}
}

func TestPinnedListTablesUsesGenerationState(t *testing.T) {
	dir := t.TempDir()
	seedGenerationState(t, dir, 2, 5)
	s := newDiskServer(t, dir)

	// Live view reports latest_delta=5.
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_tables"}}`)
	text, _ := resultText(t, resp)
	if !strings.Contains(text, "latest_delta=5") {
		t.Errorf("live view should report delta 5, got: %s", text)
	}

	// Pinned view reports latest_delta=2 and only the generation's segments.
	resp = rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_tables","arguments":{"generation":1}}}`)
	text, isErr := resultText(t, resp)
	if isErr {
		t.Fatalf("pinned list_tables error: %s", text)
	}
	if !strings.Contains(text, "latest_delta=2") {
		t.Errorf("pinned view should report delta 2, got: %s", text)
	}
	if !strings.Contains(text, "files=3") { // base + deltas 1,2
		t.Errorf("pinned view should report 3 files, got: %s", text)
	}
}

func TestPinnedReadFailsWhenSegmentMissing(t *testing.T) {
	dir := t.TempDir()
	seedGenerationState(t, dir, 2, 4)
	s := newDiskServer(t, dir)

	// Remove a delta the pinned generation needs.
	if err := os.Remove(filepath.Join(dir, filepath.FromSlash(generation.DeltaPath("public.orders", 1)))); err != nil {
		t.Fatalf("remove delta: %v", err)
	}

	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_tables","arguments":{"generation":1}}}`)
	text, isErr := resultText(t, resp)
	if !isErr {
		t.Fatalf("expected missing-segment error, got: %s", text)
	}
	if !strings.Contains(text, "epoch 1 missing") && !strings.Contains(text, "epoch=000001.parquet") {
		t.Errorf("error should name the missing segment, got: %s", text)
	}
	// Critically, it must not silently use the newer live state.
	if strings.Contains(text, "latest_delta=4") {
		t.Errorf("missing segment caused a fallback to live state: %s", text)
	}
}

func TestBuildViewDDLNamesOnlyRecordedFiles(t *testing.T) {
	dir := t.TempDir()
	r := seedGenerationState(t, dir, 2, 4)
	s := newDiskServer(t, dir)

	st, err := s.loadReadState(context.Background(), r.Generation)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	ddl := buildViewDDL(st, dir)

	// Every segment recorded in the generation must appear.
	for _, want := range []string{"base_000000.parquet", "epoch=000001.parquet", "epoch=000002.parquet"} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL missing recorded segment %s:\n%s", want, ddl)
		}
	}
	// Deltas written after the generation was saved must not.
	for _, unwanted := range []string{"epoch=000003.parquet", "epoch=000004.parquet"} {
		if strings.Contains(ddl, unwanted) {
			t.Errorf("DDL leaked a later segment %s:\n%s", unwanted, ddl)
		}
	}
	// No directory glob — that is how later files would sneak in.
	if strings.Contains(ddl, "**") || strings.Contains(ddl, "*.parquet") {
		t.Errorf("DDL uses a glob rather than explicit files:\n%s", ddl)
	}
}

func TestLiveViewDDLNamesExactBaseNotStray(t *testing.T) {
	dir := t.TempDir()
	seedGenerationState(t, dir, 2, 4)
	// A newer base left behind by a compaction whose manifest write was lost.
	writeFile(t, dir, generation.BasePath("public.orders", 9), "stray-base")
	s := newDiskServer(t, dir)

	st, err := s.loadReadState(context.Background(), 0)
	if err != nil {
		t.Fatalf("load live state: %v", err)
	}
	ddl := buildViewDDL(st, dir)
	if strings.Contains(ddl, "base_000009.parquet") {
		t.Errorf("live DDL picked up a stray newer base:\n%s", ddl)
	}
	if !strings.Contains(ddl, "base_000000.parquet") {
		t.Errorf("live DDL should name the manifest's base:\n%s", ddl)
	}
}

func TestNegativeGenerationRejected(t *testing.T) {
	dir := t.TempDir()
	seedGenerationState(t, dir, 2, 4)
	s := newDiskServer(t, dir)

	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_tables","arguments":{"generation":-1}}}`)
	text, isErr := resultText(t, resp)
	if !isErr || !strings.Contains(text, "generation must be positive") {
		t.Errorf("expected rejection of negative generation, got isErr=%v: %s", isErr, text)
	}
}
