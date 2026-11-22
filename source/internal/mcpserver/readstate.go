package mcpserver

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/generation"
)

// readState is the resolved view a query tool reads from: either the live
// manifest or a saved manifest generation. Every table is bound to an explicit
// file list, so a query can only ever see the segments that belonged to the
// requested state.
type readState struct {
	manifest *manifest.Manifest
	plan     *generation.Plan
	// label describes the state for messages: "live manifest" or
	// "manifest generation 7".
	label string
}

// loadReadState resolves the read state a tool call asked for. generation <= 0
// reads the live manifest; a positive generation reads that saved generation
// strictly, and a generation that does not exist is an error — never a
// silent fallback to live.
func (s *server) loadReadState(ctx context.Context, gen int64) (*readState, error) {
	if gen <= 0 {
		m, err := s.sink.ReadManifest(ctx)
		if err != nil {
			return nil, fmt.Errorf("read manifest: %w (run `pg-cdc init` first?)", err)
		}
		plan, err := generation.LivePlan(ctx, s.sink, m)
		if err != nil {
			return nil, err
		}
		return &readState{manifest: m, plan: plan, label: "live manifest"}, nil
	}

	r, plan, err := generation.LoadPlan(ctx, s.sink, gen)
	if err != nil {
		return nil, err
	}
	if err := generation.Verify(ctx, s.sink, plan); err != nil {
		return nil, err
	}
	return &readState{manifest: r.Manifest, plan: plan, label: fmt.Sprintf("manifest generation %d", gen)}, nil
}

// tableFiles returns the base + delta files for one table, or false if the
// state does not include the table.
func (s *readState) tableFiles(name string) (generation.TablePlan, bool) {
	tp, ok := s.plan.Tables[name]
	return tp, ok
}

// parquetList renders a DuckDB list literal of explicit file paths. Paths are
// absolute on the filesystem, so the caller resolves them against the storage
// root first.
func parquetList(paths []string) string {
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = quoteSQL(p)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// orderedNames returns the plan's table names, sorted for stable output.
func (s *readState) orderedNames() []string {
	names := make([]string, 0, len(s.plan.Tables))
	for n := range s.plan.Tables {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
