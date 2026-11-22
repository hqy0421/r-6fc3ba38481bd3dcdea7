package main

import (
	"fmt"
	"time"

	"github.com/burnside-project/pg-cdc-core/internal/generation"
	"github.com/burnside-project/pg-cdc-core/internal/registry"
	"github.com/spf13/cobra"
)

func generationsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generations",
		Short: "Save and inspect immutable manifest generations for pinned reads",
		Long: `A manifest generation is an immutable copy of the live manifest together
with the exact base and delta Parquet segments every active table was made of
at that moment.

Readers (the MCP server and ` + "`pg-cdc compact`" + `) resolve a generation to that
recorded segment list. If a recorded segment is missing from storage the read
fails and names it — it never substitutes a newer base or delta, because that
would silently return a different point in time.`,
	}
	cmd.AddCommand(generationsSaveCmd())
	cmd.AddCommand(generationsListCmd())
	cmd.AddCommand(generationsPruneCmd())
	return cmd
}

func generationsSaveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "save",
		Short: "Snapshot the live manifest as the next immutable generation",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			sink, err := registry.NewSink(cfg.Storage)
			if err != nil {
				return err
			}
			r, err := generation.Archive(cmd.Context(), sink)
			if err != nil {
				return err
			}
			fmt.Printf("Saved manifest generation %d (%s)\n", r.Generation, generation.Filename(r.Generation))
			fmt.Printf("  tables:   %d\n", len(r.Tables))
			fmt.Printf("  segments: %d (base + deltas)\n", r.SegmentCount())
			fmt.Printf("  saved_at: %s\n", r.SavedAt.Format(time.RFC3339))
			return nil
		},
	}
}

func generationsListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List saved manifest generations",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			sink, err := registry.NewSink(cfg.Storage)
			if err != nil {
				return err
			}
			gens, err := generation.List(cmd.Context(), sink)
			if err != nil {
				return err
			}
			if len(gens) == 0 {
				fmt.Println("no saved manifest generations (create one with `pg-cdc generations save`)")
				return nil
			}
			fmt.Printf("%d saved manifest generation(s):\n", len(gens))
			for _, gen := range gens {
				r, err := generation.Load(cmd.Context(), sink, gen)
				if err != nil {
					return err
				}
				fmt.Printf("  %6d  saved_at=%s  tables=%d  segments=%d\n",
					gen, r.SavedAt.Format(time.RFC3339), len(r.Tables), r.SegmentCount())
			}
			return nil
		},
	}
}

func generationsPruneCmd() *cobra.Command {
	var olderThan time.Duration
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Delete saved generation records older than a retention window",
		Long: `Removes generation records only. The Parquet segments they referenced are
reclaimed by the next compaction once no remaining generation refers to them.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if olderThan <= 0 {
				return fmt.Errorf("--older-than must be positive (e.g. 168h for one week)")
			}
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			sink, err := registry.NewSink(cfg.Storage)
			if err != nil {
				return err
			}
			cutoff := time.Now().UTC().Add(-olderThan)
			removed, err := generation.Prune(cmd.Context(), sink, cutoff)
			if err != nil {
				return err
			}
			if len(removed) == 0 {
				fmt.Printf("no generations older than %s (cutoff %s)\n", olderThan, cutoff.Format(time.RFC3339))
				return nil
			}
			fmt.Printf("removed %d generation(s) saved before %s: %v\n", len(removed), cutoff.Format(time.RFC3339), removed)
			return nil
		},
	}
	cmd.Flags().DurationVar(&olderThan, "older-than", 0, "retention window (e.g. 168h)")
	return cmd
}
