package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/airbusgeo/cogger"
	"github.com/airbusgeo/godal"
	shellwords "github.com/mattn/go-shellwords"
	"github.com/tbonfort/gobs"

	"github.com/spf13/cobra"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()
	godal.RegisterInternalDrivers()
	godal.RegisterRaster(godal.JP2KAK, godal.DIMAP)
	rootCmd := newMCOGCommand()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		//fmt.Println(err)
		os.Exit(1)
	}
}

func newMCOGCommand() *cobra.Command {
	var parrallelism int
	var srcDatasetName, dstDatasetName string
	var blocksize int
	var copts []string
	var configOpts []string
	var mainSwitches string
	var ovrSwitches string
	var pixelCount int
	var debug bool
	creationOptions := map[string]string{
		"TILED":    "YES",
		"COMPRESS": "LZW",
	}

	var gtSwitches, ovrgtSwitches []string
	cmd := &cobra.Command{
		Use:   "mcog",
		Short: "mcog",
		Args:  cobra.NoArgs,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		SilenceUsage: true,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			gtSwitches, err = shellwords.Parse(mainSwitches)
			if err != nil {
				return fmt.Errorf("invalid mainSwitches: %w", err)
			}
			if err := checkSwitches(gtSwitches, false); err != nil {
				return err
			}

			ovrgtSwitches, err = shellwords.Parse(ovrSwitches)
			if err != nil {
				return fmt.Errorf("invalid ovrSwitches: %w", err)
			}
			if err := checkSwitches(ovrgtSwitches, false); err != nil {
				return err
			}
			ovrHasResampling := false
			for _, gs := range ovrgtSwitches {
				if gs == "-r" {
					ovrHasResampling = true
					break
				}
			}
			if !ovrHasResampling {
				ovrgtSwitches = append(ovrgtSwitches, "-r", "bilinear")
			}
			for _, co := range copts {
				k, v, _ := strings.Cut(co, "=")
				if k == "BLOCKXSIZE" || k == "BLOCKYSIZE" {
					return fmt.Errorf("BLOCKXSIZE/BLOCKYSIZE creation option not allowed, use --blocksize")
				}
				if v == "" {
					delete(creationOptions, k)
				} else {
					creationOptions[k] = v
				}
			}
			creationOptions["BLOCKXSIZE"] = fmt.Sprintf("%d", blocksize)
			creationOptions["BLOCKYSIZE"] = fmt.Sprintf("%d", blocksize)

			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&srcDatasetName, "src", "", "source dataset")
	flags.StringVar(&dstDatasetName, "dst", "", "destination dataset")
	flags.IntVar(&parrallelism, "parrallelism", 4, "number of concurrent gdal processes")
	flags.IntVar(&blocksize, "blocksize", 256, "dst dataset tiff internal blocksize")
	flags.StringArrayVar(&copts, "co", nil, "tif creation options, eg, \"COMPRESS=LZW,INTERLEAVE=2\"")
	flags.StringArrayVar(&configOpts, "config", nil, "gdal configuration options")
	flags.StringVar(&mainSwitches, "mainSwitches", "", "gdal_translate switches for main dataset. e.g: \"-b 1 -b 3 -b 2 -a_srs epsg:4326\"")
	flags.StringVar(&ovrSwitches, "ovrSwitches", "", "gdal_translate switches for overview datasets")
	flags.IntVar(&pixelCount, "pixelCount", 8192*8192, "target pixel count for individual strips")
	flags.BoolVar(&debug, "debug", false, "debug mode")

	cmd.MarkFlagRequired("src")
	cmd.MarkFlagRequired("dst")

	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		tdir, err := os.MkdirTemp(".", "tmpcog-*")
		if err != nil {
			return fmt.Errorf("create temp dir: %w", err)
		}
		if !debug {
			defer os.RemoveAll(tdir)
		}

		srcDataset, err := godal.Open(srcDatasetName, godal.RasterOnly())
		if err != nil {
			return fmt.Errorf("open %s: %w", srcDatasetName, err)
		}
		srcStruct := srcDataset.Structure()
		if _, err := srcDataset.GeoTransform(); err != nil {
			return fmt.Errorf("datasets with no geotransform not supported yet")
		}
		srcDataset.Close()

		opts := []cogger.StripperOption{
			cogger.TargetPixelCount(pixelCount),
			cogger.InternalTileSize(blocksize, blocksize),
			//cogger.SkipFullresParallelisation(),
		}
		if srcStruct.BlockSizeX == srcStruct.BlockSizeY && srcStruct.BlockSizeX >= blocksize {
			opts = append(opts,
				cogger.FullresStripHeightMuliple(srcStruct.BlockSizeY))
		}

		tiler, err := cogger.NewStripper(srcStruct.SizeX, srcStruct.SizeY, opts...)
		if err != nil {
			return fmt.Errorf("newtiler: %w", err)
		}

		p := gobs.NewPool(parrallelism)
		batch := p.Batch()

		tifname := func(base string) string {
			return filepath.Join(tdir, base+".tif")
		}

		wf := tiler.Workflow(ctx)
		for step := range wf.Steps() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			step := step
			batch.Submit(func() error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				fmt.Println("start strip", step.DstName, "from", step.SrcNames)
				st := time.Now()
				var srcs []string
				var switches []string
				if len(step.SrcNames) == 0 {
					srcs = []string{srcDatasetName}
					switches = gtSwitches
				} else {
					srcs = make([]string, len(step.SrcNames))
					for i, sn := range step.SrcNames {
						srcs[i] = tifname(sn)
					}
					switches = ovrgtSwitches
				}
				dst := tifname(step.DstName)
				if err := gdal_translate(ctx, srcs, dst, switches, creationOptions, configOpts,
					step.DstWidth, step.DstHeight, step.ULX, step.ULY, step.SrcWidth, step.SrcHeight); err != nil {
					return fmt.Errorf("gdal_translate: %w", err)
				}
				fmt.Println("done strip", step.DstName, time.Since(st).Seconds())
				wf.Ack(step)
				return nil
			})
		}

		if err := batch.Wait(); err != nil {
			return err
		}

		// cogify all strips
		return nil
	}

	return cmd
}

func checkSwitches(sw []string, isOvr bool) error {
	for _, s := range sw {
		switch s {
		case "-sds", "-of", "-te", "-outsize", "-tr", "-srcwin", "-projwin", "-a_ullr", "-a_gt":
			return fmt.Errorf("%s switch not allowed, use a vrt over source dataset", s)
		case "-ot", "-if", "-mask", "-expand", "-b", "-scale", "-unscale", "-exponent",
			"-a_nodata", "-gcp":
			if isOvr {
				return fmt.Errorf("%s switch not allowed for overviews", s)
			}
		}
	}
	return nil
}

func gdal_translate(ctx context.Context, srcs []string, dst string, switches []string,
	creationOptions map[string]string,
	configOptions []string,
	width, height int, ulX, ulY, srcWidth, srcHeight float64) error {
	var srcDataset *godal.Dataset
	var err error
	if len(srcs) == 1 {
		srcDataset, err = godal.Open(srcs[0], godal.RasterOnly())
		if err != nil {
			return fmt.Errorf("godal.open %s: %w", srcs[0], err)
		}
	} else {
		srcDataset, err = godal.BuildVRT("", srcs, nil)
		if err != nil {
			return fmt.Errorf("create source vrt: %w", err)
		}
	}
	defer srcDataset.Close()
	switches = append(switches,
		"-outsize", fmt.Sprintf("%d", width), fmt.Sprintf("%d", height),
		"-srcwin",
		fmt.Sprintf("%g", ulX),
		fmt.Sprintf("%g", ulY),
		fmt.Sprintf("%g", srcWidth),
		fmt.Sprintf("%g", srcHeight))

	copts := make([]string, 0, len(creationOptions))
	for k, v := range creationOptions {
		copts = append(copts, fmt.Sprintf("%s=%s", k, v))
	}
	dstDS, err := srcDataset.Translate(dst, switches,
		godal.CreationOption(copts...),
		godal.ConfigOption(configOptions...),
		godal.GTiff)
	if err != nil {
		return fmt.Errorf("godal.translate: %w", err)
	}
	if err = dstDS.Close(); err != nil {
		return fmt.Errorf("close strip %s: %w", dst, err)
	}
	return nil
}
