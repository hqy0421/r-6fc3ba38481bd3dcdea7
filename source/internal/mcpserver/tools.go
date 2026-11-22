package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/burnside-project/pg-cdc-core/internal/generation"
)

type tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

func toolDefinitions() []tool {
	genArg := map[string]any{
		"type": "integer",
		"description": "Saved manifest generation to read (see list_generations). " +
			"Omit or 0 for the live manifest. A generation that does not exist is an error; " +
			"the tool never falls back to the live manifest.",
	}
	return []tool{
		{
			Name:        "list_tables",
			Description: "List active tables from the pg-cdc manifest with row counts, tags, and epochs.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"generation": genArg,
				},
			},
		},
		{
			Name:        "list_generations",
			Description: "List saved manifest generations (immutable snapshots of the manifest) available for pinned reads.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			Name:        "describe_table",
			Description: "Show the schema (columns, Postgres types, nullability) for a single table.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{
						"type":        "string",
						"description": "Fully-qualified table name (e.g. \"public.orders\").",
					},
					"generation": genArg,
				},
				"required": []string{"name"},
			},
		},
		{
			Name:        "query",
			Description: "Run a read-only SELECT against the Parquet snapshot for a table. Returns rows as JSON.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"sql":        map[string]any{"type": "string", "description": "SELECT statement against pg-cdc Parquet tables."},
					"limit":      map[string]any{"type": "integer", "description": "Max rows to return (default 100, max 1000)."},
					"generation": genArg,
				},
				"required": []string{"sql"},
			},
		},
		{
			Name:        "recent_changes",
			Description: "Return the N most recent rows from the streaming delta files for a table.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name":       map[string]any{"type": "string", "description": "Fully-qualified table name."},
					"limit":      map[string]any{"type": "integer", "description": "Max rows (default 20, max 200)."},
					"generation": genArg,
				},
				"required": []string{"name"},
			},
		},
	}
}

type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func (s *server) callTool(ctx context.Context, raw json.RawMessage) map[string]any {
	var p toolCallParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return errorContent(fmt.Errorf("invalid tools/call params: %w", err))
	}
	switch p.Name {
	case "list_tables":
		return s.listTables(ctx, p.Arguments)
	case "list_generations":
		return s.listGenerations(ctx)
	case "describe_table":
		return s.describeTable(ctx, p.Arguments)
	case "query":
		return s.query(ctx, p.Arguments)
	case "recent_changes":
		return s.recentChanges(ctx, p.Arguments)
	default:
		return errorContent(fmt.Errorf("unknown tool: %s", p.Name))
	}
}

// genArg parses the optional generation field from a tool's arguments.
// Absent means live; an explicitly negative value is rejected so a malformed
// pin can't quietly become a live read.
func genArg(raw json.RawMessage) (int64, error) {
	var probe struct {
		Generation *int64 `json:"generation"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &probe); err != nil {
			return 0, fmt.Errorf("invalid arguments: %w", err)
		}
	}
	if probe.Generation == nil {
		return 0, nil
	}
	if *probe.Generation < 0 {
		return 0, fmt.Errorf("generation must be positive (or omit it for the live manifest), got %d", *probe.Generation)
	}
	return *probe.Generation, nil
}

func (s *server) listTables(ctx context.Context, args json.RawMessage) map[string]any {
	gen, err := genArg(args)
	if err != nil {
		return errorContent(err)
	}
	st, err := s.loadReadState(ctx, gen)
	if err != nil {
		return errorContent(err)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%d active tables in %s (sink: %s)\n\n", len(st.plan.Tables), st.label, s.sink.Backend())
	for _, n := range st.orderedNames() {
		t := st.manifest.Tables[n]
		tp := st.plan.Tables[n]
		fmt.Fprintf(&b, "- %s · rows=%d · base_epoch=%d · latest_delta=%d",
			n, t.RowCount, tp.BaseEpoch, tp.LatestDeltaEpoch)
		if len(t.Tags) > 0 {
			fmt.Fprintf(&b, " · tags=[%s]", strings.Join(t.Tags, ","))
		}
		fmt.Fprintf(&b, " · files=%d", len(tp.Files))
		b.WriteString("\n")
	}
	return textContent(b.String())
}

func (s *server) listGenerations(ctx context.Context) map[string]any {
	gens, err := generation.List(ctx, s.sink)
	if err != nil {
		return errorContent(fmt.Errorf("list saved generations: %w", err))
	}
	if len(gens) == 0 {
		return textContent("no saved manifest generations (create one with `pg-cdc generations save`)")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d saved manifest generation(s):\n\n", len(gens))
	for _, gen := range gens {
		r, err := generation.Load(ctx, s.sink, gen)
		if err != nil {
			return errorContent(err)
		}
		fmt.Fprintf(&b, "- generation %d · saved_at=%s · tables=%d · segments=%d\n",
			gen, r.SavedAt.Format(time.RFC3339), len(r.Tables), r.SegmentCount())
	}
	return textContent(b.String())
}

func (s *server) describeTable(ctx context.Context, args json.RawMessage) map[string]any {
	var a struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return errorContent(fmt.Errorf("invalid arguments: %w", err))
	}
	if a.Name == "" {
		return errorContent(fmt.Errorf("name is required"))
	}
	gen, err := genArg(args)
	if err != nil {
		return errorContent(err)
	}
	st, err := s.loadReadState(ctx, gen)
	if err != nil {
		return errorContent(err)
	}
	m := st.manifest
	t, ok := m.Tables[a.Name]
	if !ok {
		return errorContent(fmt.Errorf("table not found in %s: %s", st.label, a.Name))
	}
	tp, ok := st.tableFiles(a.Name)
	if !ok {
		return errorContent(fmt.Errorf("table not found in %s: %s", st.label, a.Name))
	}
	sch, ok := m.Schemas[a.Name]
	if !ok {
		return errorContent(fmt.Errorf("no schema recorded for %s", a.Name))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s · %s · status=%s · rows=%d · schema_version=%d\n\n",
		a.Name, st.label, t.Status, t.RowCount, sch.Version)
	fmt.Fprintf(&b, "segments: base_epoch=%d · latest_delta=%d · files=%d\n",
		tp.BaseEpoch, tp.LatestDeltaEpoch, len(tp.Files))
	if len(t.PrimaryKey) > 0 {
		fmt.Fprintf(&b, "primary key: %s\n", strings.Join(t.PrimaryKey, ", "))
	}
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "columns:")
	for _, c := range sch.Columns {
		nullable := "NOT NULL"
		if c.Nullable {
			nullable = "NULL"
		}
		fmt.Fprintf(&b, "  %-30s %-20s %s\n", c.Name, c.PgType, nullable)
	}
	return textContent(b.String())
}

func textContent(s string) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": s}},
		"isError": false,
	}
}

func errorContent(err error) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": err.Error()}},
		"isError": true,
	}
}
