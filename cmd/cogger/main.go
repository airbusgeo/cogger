package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/airbusgeo/cogger"
	"github.com/spf13/cobra"

	"github.com/google/tiff"
	_ "github.com/google/tiff/bigtiff"
)

func main() {
	ctx, cncl := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cncl()
	exitCode := 0
	if err := newRootCommand().ExecuteContext(ctx); err != nil {
		exitCode = 1
	}
	os.Exit(exitCode)
}

func newRootCommand() *cobra.Command {
	outfile := "out.tif"
	skipGhostAreas := false
	keepBigtiff := false
	forceBigtiff := false
	keptOverviewsS := ""
	keptMasksS := ""
	var keptMasks []int = nil
	var keptOverviews []int = nil
	cmd := &cobra.Command{
		Use:   "cogger [main.tif] [overview.tif]...",
		Short: "cogger is a tool for creating Cloud Optimized GeoTIFFs",
		Args:  cobra.MinimumNArgs(1),
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			flags := cmd.Flags()
			if flags.Changed("keep-masks") {
				if keptMasksS == "" {
					keptMasks = []int{}
				} else {
					ks := strings.Split(keptMasksS, ",")
					keptMasks = make([]int, len(ks))
					for k := range ks {
						kv, err := strconv.Atoi(ks[k])
						if err != nil {
							return fmt.Errorf("invalid mask index %s: %w", ks[k], err)
						}
						keptMasks[k] = kv
					}
				}
			}
			if flags.Changed("keep-overviews") {
				if keptOverviewsS == "" {
					keptOverviews = []int{}
				} else {
					ks := strings.Split(keptOverviewsS, ",")
					keptOverviews = make([]int, len(ks))
					for k := range ks {
						kv, err := strconv.Atoi(ks[k])
						if err != nil {
							return fmt.Errorf("invalid overview index %s: %w", ks[k], err)
						}
						keptOverviews[k] = kv
					}
				}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			readers := make([]tiff.ReadAtReadSeeker, len(args))
			for i, input := range args {
				topFile, err := os.Open(input)
				if err != nil {
					return fmt.Errorf("open %s: %w", args[0], err)
				}
				defer topFile.Close()
				readers[i] = topFile
			}
			out, err := os.Create(outfile)
			if err != nil {
				return fmt.Errorf("create %s: %w", outfile, err)
			}
			cfg := cogger.DefaultConfig()
			if keepBigtiff {
				tif0, err := tiff.Parse(readers[0], nil, nil)
				if err != nil {
					return fmt.Errorf("parse %s: %w", args[0], err)
				}
				if tif0.Version() == 0x2B {
					cfg.BigTIFF = true
				}
				readers[0].Seek(0, io.SeekStart)
			}
			if forceBigtiff {
				cfg.BigTIFF = true
			}
			if skipGhostAreas {
				cfg.WithGDALGhostArea = false
			}
			cfg.KeptMasks = keptMasks
			cfg.KeptOverviews = keptOverviews
			err = cfg.Rewrite(out, readers...)
			if err != nil {
				return fmt.Errorf("cogger.rewrite: %w", err)
			}
			err = out.Close()
			if err != nil {
				return fmt.Errorf("close %s: %w", outfile, err)
			}
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&outfile, "output", outfile, "destination file")
	flags.BoolVar(&skipGhostAreas, "skip-gdal-ghost-areas", skipGhostAreas, "omit writing gdal ghost areas")
	flags.BoolVar(&keepBigtiff, "keep-bigtiff", keepBigtiff, "produce a bigtiff file if the input is bigtiff")
	flags.BoolVar(&forceBigtiff, "force-bigtiff", forceBigtiff, "produce a bigtiff output even if the size is less than 4Gb")
	flags.StringVar(&keptOverviewsS, "keep-overviews", "", "comma separated list of overview levels to keep")
	flags.StringVar(&keptMasksS, "keep-masks", "", "comma separated list of mask levels to keep")
	cmd.MarkFlagsMutuallyExclusive("keep-bigtiff", "force-bigtiff")
	flags.SortFlags = false

	return cmd
}
