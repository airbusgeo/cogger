package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/airbusgeo/cogger"
	"github.com/airbusgeo/godal"
	"github.com/airbusgeo/osio"
	"github.com/airbusgeo/osio/gcs"
	"github.com/google/tiff"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/tbonfort/gobs"
	adst "go.airbusds-geo.com/gcp/storage"
	"go.airbusds-geo.com/log"
	k8sv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	k8smeta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"sigs.k8s.io/yaml"
)

var stcl *storage.Client
var adstcl *adst.Client
var gcsa *osio.Adapter

var copts []string
var configOpts []string
var verbose bool
var blocksize string
var numCachedBlcocks int
var startTime time.Time
var workBucket string
var width, height int
var ulx, uly, srcWidth, srcHeight float64
var shell bool
var rpc bool
var mainSwitches string
var ovrSwitches string
var slaveSwitches string
var pixelCount int
var jobid string

var defaultImage string = "build-error-this-variable-should-have-been-set-on-build"
var dockerImage string

var rootCmd = &cobra.Command{
	Use:   "tiler",
	Short: "cog tiling cli",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
	SilenceUsage: true,

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		startTime = time.Now()
		if !verbose {
			os.Setenv("LOGLEVEL", "info")
			log.Structured()
		}
		ctx := cmd.Context()
		var err error

		if stcl, err = storage.NewClient(ctx); err != nil {
			return fmt.Errorf("storage.newclient: %w", err)
		}
		if adstcl, err = adst.New(ctx, adst.WithStorageClient(stcl)); err != nil {
			return fmt.Errorf("ads storage.new: %w", err)
		}

		gcsh, err := gcs.Handle(ctx, gcs.GCSClient(stcl))
		if err != nil {
			return fmt.Errorf("gcs.handle: %w", err)
		}
		gcsa, err = osio.NewAdapter(gcsh, osio.BlockSize(blocksize), osio.NumCachedBlocks(numCachedBlcocks))
		if err != nil {
			return fmt.Errorf("osio.new: %w", err)
		}
		if err := godal.RegisterVSIHandler("gs://", gcsa); err != nil {
			return fmt.Errorf("register osio: %w", err)
		}
		godal.RegisterAll()
		return nil
	},
	PersistentPostRun: func(cmd *cobra.Command, _ []string) {
		log.Logger(cmd.Context()).Sugar().Debugf("command %s took %.1fs",
			cmd.Name(), time.Since(startTime).Seconds())

	},
}

func init() {
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "verbose output")
	rootCmd.PersistentFlags().StringVar(&workBucket, "workingBucket", "cogger-scratch", "temporary work bucket")
	rootCmd.PersistentFlags().StringVar(&blocksize, "blocksize", "512k", "gs cache blocksize")
	rootCmd.PersistentFlags().IntVar(&numCachedBlcocks, "numblocks", 1000, "number of gs cached blocks")
	rootCmd.AddCommand(masterCmd, slaveCmd, vrtCmd, coggerCmd)

	masterCmd.Flags().StringArrayVar(&copts, "co", nil, "tif creation options")
	masterCmd.Flags().StringArrayVar(&configOpts, "config", nil, "gdal configuration options")
	masterCmd.Flags().StringVar(&jobid, "jobID", "", "(advanced) use predefined job identifier")
	masterCmd.Flags().StringVar(&mainSwitches, "mainSwitches", "", "gdal_translate switches for main dataset. e.g: \"-b 1 -b 3 -b 2 -a_srs epsg 4326\"")
	masterCmd.Flags().StringVar(&ovrSwitches, "ovrSwitches", "", "gdal_translate switches for overview datasets")
	masterCmd.Flags().StringVar(&dockerImage, "dockerImage", defaultImage, "docker image for workers")
	masterCmd.Flags().BoolVar(&shell, "shell", false, "output shell script instead of argo workflow")
	masterCmd.Flags().IntVar(&pixelCount, "pixelCount", 8192*8192, "target number of pixels per strip")

	slaveCmd.Flags().StringArrayVar(&copts, "co", nil, "tif creation options")
	slaveCmd.Flags().StringArrayVar(&configOpts, "config", nil, "gdal configuration options")
	slaveCmd.Flags().StringVar(&ovrSwitches, "switches", "", "gdal_translate switches")
	slaveCmd.Flags().IntVar(&width, "w", 0, "output width")
	slaveCmd.MarkFlagRequired("w")
	slaveCmd.Flags().IntVar(&height, "h", 0, "output height")
	slaveCmd.MarkFlagRequired("h")
	slaveCmd.Flags().Float64Var(&ulx, "ulx", 0, "source x origin (in pixels)")
	slaveCmd.MarkFlagRequired("ulx")
	slaveCmd.Flags().Float64Var(&uly, "uly", 0, "source y origin (in pixels)")
	slaveCmd.MarkFlagRequired("uly")
	slaveCmd.Flags().Float64Var(&srcWidth, "sw", 0, "source width (in pixels)")
	slaveCmd.MarkFlagRequired("sw")
	slaveCmd.Flags().Float64Var(&srcHeight, "sh", 0, "source height (in pixels)")
	slaveCmd.MarkFlagRequired("sh")
	slaveCmd.Flags().BoolVar(&rpc, "rpc", false, "rpc georeferencing")

	coggerCmd.Flags().IntVar(&pixelCount, "pixelCount", 8192*8192, "target number of pixels per strip")
	coggerCmd.MarkFlagRequired("pixelCount")
	coggerCmd.Flags().IntVar(&width, "w", 0, "output width")
	coggerCmd.MarkFlagRequired("w")
	coggerCmd.Flags().IntVar(&height, "h", 0, "output height")
	coggerCmd.MarkFlagRequired("h")

}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		//fmt.Println(err)
		os.Exit(1)
	}
}

func int32Ptr(val int32) *int32 {
	a := val
	return &a
}
func intOrStringPtr(val int) *intstr.IntOrString {
	a := intstr.FromInt(val)
	return &a
}

func printCommand(cmd []string) string {
	sb := strings.Builder{}
	for i, c := range cmd {
		if i != 0 {
			fmt.Fprintf(&sb, " ")
		}
		fmt.Fprintf(&sb, "%q", c)
	}
	return sb.String()
}

func resourcePtr(val string) *resource.Quantity {
	res := resource.MustParse(val)
	return &res
}

var masterCmd = &cobra.Command{
	Use:   "master gs://bucket/dstcog.tif srcfile",
	Short: "create workflow for cogifying srcfile to cog on gs://",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		dstDatasetName := args[0]
		srcDatasetName := args[1]

		if jobid == "" {
			jobid = uuid.New().String()
		}

		srcDataset, err := godal.Open(srcDatasetName, godal.RasterOnly())
		if err != nil {
			return fmt.Errorf("open %s: %w", srcDatasetName, err)
		}
		defer srcDataset.Close()
		srcStruct := srcDataset.Structure()

		rpcdata := srcDataset.Metadatas(godal.Domain("RPC"))
		if len(rpcdata) > 0 {
			rpc = true
		}

		tiler, err := cogger.NewTiler(srcStruct.SizeX, srcStruct.SizeY, cogger.TargetPixelCount(pixelCount))
		if err != nil {
			return fmt.Errorf("newtiler: %w", err)
		}

		pyramid := tiler.Tiling()

		srcfile := srcDatasetName

		wf := &wfv1.Workflow{
			ObjectMeta: k8smeta.ObjectMeta{
				GenerateName: "cogger-",
			},
			TypeMeta: k8smeta.TypeMeta{
				APIVersion: "argoproj.io/v1alpha1",
				Kind:       "Workflow",
			},
			Spec: wfv1.WorkflowSpec{
				TTLStrategy: &wfv1.TTLStrategy{
					SecondsAfterSuccess: int32Ptr(3600),
				},
				Entrypoint: "cogger",
				TemplateDefaults: &wfv1.Template{
					Volumes: []k8sv1.Volume{
						{
							Name: "scratch",
							VolumeSource: k8sv1.VolumeSource{
								EmptyDir: &k8sv1.EmptyDirVolumeSource{
									SizeLimit: resourcePtr("200M"),
								},
							},
						},
					},
					Container: &k8sv1.Container{
						ImagePullPolicy: k8sv1.PullAlways,
						Resources: k8sv1.ResourceRequirements{
							Requests: k8sv1.ResourceList{
								k8sv1.ResourceCPU:    resource.MustParse("2"),
								k8sv1.ResourceMemory: resource.MustParse("1G"),
							},
						},
						WorkingDir: "/scratch",
						VolumeMounts: []k8sv1.VolumeMount{
							{
								Name:      "scratch",
								MountPath: "/scratch",
							},
						},
					},
				},
				Templates: []wfv1.Template{
					{Name: "cogger"},
				},
			},
		}

		zstrips := [][]string{}
		var z int
		var img cogger.Image
		var lastCommands [][]string
		for z, img = range pyramid {
			if z > 0 {
				rpc = false
			}
			strips := []string{}
			slaveCommands := [][]string{}
			for s, strip := range img.Strips {
				stripfile := fmt.Sprintf("gs://%s/%s/%d-%d.tif", workBucket, jobid, z, s)
				command := []string{"tiler", "slave", stripfile, srcfile,
					"--w", fmt.Sprintf("%d", strip.TargetWidth),
					"--h", fmt.Sprintf("%d", strip.TargetHeight),
					"--ulx", fmt.Sprintf("%g", strip.SrcTopLeftX),
					"--uly", fmt.Sprintf("%g", strip.SrcTopLeftY),
					"--sw", fmt.Sprintf("%g", strip.SrcBottomRightX-strip.SrcTopLeftX),
					"--sh", fmt.Sprintf("%g", strip.SrcBottomRightY-strip.SrcTopLeftY),
					fmt.Sprintf("--rpc=%v", rpc)}
				for _, co := range copts {
					command = append(command, "--co", co)
				}
				for _, co := range configOpts {
					command = append(command, "--config", co)
				}
				if z == 0 && mainSwitches != "" {
					if _, err = getSwitches(mainSwitches, false); err != nil {
						return err
					}
					command = append(command, "--switches", mainSwitches)
				} else if ovrSwitches != "" {
					if _, err = getSwitches(ovrSwitches, true); err != nil {
						return err
					}
					command = append(command, "--switches", ovrSwitches)
				}
				slaveCommands = append(slaveCommands, command)
				strips = append(strips, stripfile)
				if shell {
					fmt.Println(printCommand(command))
				}
			}
			zstrips = append(zstrips, strips)
			ps := wfv1.ParallelSteps{}
			if len(slaveCommands) > 1 {
				for s, sl := range slaveCommands {
					sstep := wfv1.WorkflowStep{
						Name: fmt.Sprintf("Strip-Z%d-%d", z, s),
						Inline: &wfv1.Template{
							RetryStrategy: &wfv1.RetryStrategy{
								Limit: intOrStringPtr(5),
							},
							Container: &k8sv1.Container{
								Name:    "slave",
								Image:   dockerImage,
								Command: sl,
							},
						},
					}
					ps.Steps = append(ps.Steps, sstep)
					/*
						fmt.Printf("gdal_translate -co TILED=YES -co COMPRESS=JPEG -outsize %d %d -srcwin %g %g %g %g %s %s\n",
							strip.TargetWidth, strip.TargetHeight,
							strip.SrcTopLeftX, strip.SrcTopLeftY,
							strip.SrcBottomRightX-strip.SrcTopLeftX, strip.SrcBottomRightY-strip.SrcTopLeftY,
							srcfile, stripfile)
					*/
				}
				wf.Spec.Templates[0].Steps = append(wf.Spec.Templates[0].Steps, ps)
			} else {
				lastCommands = append(lastCommands, slaveCommands...)
			}

			vrtname := fmt.Sprintf("gs://%s/%s/vrt-z%d.vrt", workBucket, jobid, z)
			command := []string{"tiler", "vrt", vrtname}
			for _, strip := range strips {
				if rpc {
					strip += ".vrt"
				}
				command = append(command, strip)
			}
			if shell {
				fmt.Println(printCommand(command))
			}
			if len(slaveCommands) > 1 {
				step := wfv1.WorkflowStep{
					Name: fmt.Sprintf("VRT-Z%d", z),
					Inline: &wfv1.Template{
						Container: &k8sv1.Container{
							Name:    "vrt",
							Image:   dockerImage,
							Command: command,
						},
						RetryStrategy: &wfv1.RetryStrategy{
							Limit: intOrStringPtr(5),
						},
					},
				}
				wf.Spec.Templates[0].Steps = append(wf.Spec.Templates[0].Steps,
					wfv1.ParallelSteps{
						Steps: []wfv1.WorkflowStep{step},
					})
			} else {
				lastCommands = append(lastCommands, command)
			}
			srcfile = vrtname
		}
		if len(lastCommands) > 0 {
			source := "set -e\n"
			for _, lc := range lastCommands {
				source += fmt.Sprintf("%s\n", printCommand(lc))
			}
			step := wfv1.WorkflowStep{
				Name: "lastCommands",
				Inline: &wfv1.Template{
					Metadata: wfv1.Metadata{
						Annotations: map[string]string{
							"cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
						},
					},
					Script: &wfv1.ScriptTemplate{
						Container: k8sv1.Container{
							Name:    "lastCommands",
							Image:   dockerImage,
							Command: []string{"sh"},
						},
						Source: source,
					},
					RetryStrategy: &wfv1.RetryStrategy{
						Limit: intOrStringPtr(5),
					},
				},
			}
			wf.Spec.Templates[0].Steps = append(wf.Spec.Templates[0].Steps,
				wfv1.ParallelSteps{
					Steps: []wfv1.WorkflowStep{step},
				})
		}
		step := wfv1.WorkflowStep{
			Name: "cogify",
			Inline: &wfv1.Template{
				RetryStrategy: &wfv1.RetryStrategy{
					Limit: intOrStringPtr(5),
				},
				Metadata: wfv1.Metadata{
					Annotations: map[string]string{
						"cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
					},
				},
				Container: &k8sv1.Container{
					Name:  "cogify",
					Image: dockerImage,
					Command: []string{"tiler", "cogify",
						"--w", fmt.Sprintf("%d", srcStruct.SizeX),
						"--h", fmt.Sprintf("%d", srcStruct.SizeY),
						"--pixelCount", fmt.Sprintf("%d", pixelCount),
						dstDatasetName},
					Resources: k8sv1.ResourceRequirements{
						Requests: k8sv1.ResourceList{
							k8sv1.ResourceCPU:    resource.MustParse("1"),
							k8sv1.ResourceMemory: resource.MustParse("4G"),
						},
					},
				},
			},
		}
		for _, zs := range zstrips {
			step.Inline.Container.Command = append(step.Inline.Container.Command, strings.Join(zs, ","))
		}
		if shell {
			fmt.Println(printCommand(step.Inline.Container.Command))
		}
		wf.Spec.Templates[0].Steps = append(wf.Spec.Templates[0].Steps,
			wfv1.ParallelSteps{
				Steps: []wfv1.WorkflowStep{step},
			})
		if !shell {
			yb, err := yaml.Marshal(wf)
			if err != nil {
				panic(err)
			}
			fmt.Println(string(yb))
		}

		return nil
	},
}

//splits string into switches and returns an error if invalid switches have been passed
func getSwitches(sw string, isOvr bool) ([]string, error) {
	switches := []string{}
	resamplingProvided := false
	for _, s := range strings.Split(sw, " ") {
		switch s {
		case "-te", "-outsize", "-tr", "-srcwin", "-projwin", "-a_ullr":
			return nil, fmt.Errorf("%s switch not allowed", s)
		case "-b":
			if isOvr {
				return nil, fmt.Errorf("%s switch not allowed for overviews", s)
			}
		case "-r":
			resamplingProvided = true
		default:
			switches = append(switches, s)
		}
	}
	if !resamplingProvided {
		switches = append(switches, "-r", "bilinear")
	}
	return switches, nil
}

var slaveCmd = &cobra.Command{
	Use:   "slave srcfile dstfile",
	Short: "extract strip from srcfile and save to dstfile",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		dstDatasetName := args[0]
		srcDatasetName := args[1]

		creationOptions := map[string]string{
			"TILED":       "YES",
			"COMPRESS":    "LZW",
			"BLOCKXSIZE":  "256",
			"BLOCKYSIZE":  "256",
			"NUM_THREADS": "4",
		}
		for _, co := range copts {
			idx := strings.Index(co, "=")
			if idx > 0 {
				creationOptions[co[0:idx]] = co[idx+1:]
			}
		}
		coptstring := []string{}
		for k, v := range creationOptions {
			coptstring = append(coptstring, k+"="+v)
		}

		srcDataset, err := godal.Open(srcDatasetName, godal.RasterOnly())
		if err != nil {
			return fmt.Errorf("open %s: %w", srcDatasetName, err)
		}
		defer srcDataset.Close()

		tmpDataset, _ := ioutil.TempFile(".", "strip-*.tif")
		tmpDataset.Close()
		tmpDatasetName := tmpDataset.Name()
		defer os.Remove(tmpDatasetName)

		switches, err := getSwitches(slaveSwitches, (srcWidth != float64(width)))
		if err != nil {
			return err
		}
		switches = append(switches,
			"-outsize", fmt.Sprintf("%d", width), fmt.Sprintf("%d", height),
			"-srcwin",
			fmt.Sprintf("%g", ulx),
			fmt.Sprintf("%g", uly),
			fmt.Sprintf("%g", srcWidth),
			fmt.Sprintf("%g", srcHeight))
		dstDS, err := srcDataset.Translate(tmpDatasetName, switches,
			godal.CreationOption(coptstring...),
			godal.ConfigOption(configOpts...))
		if err != nil {
			return fmt.Errorf("open %s: %w", srcDatasetName, err)
		}

		if err = dstDS.Close(); err != nil {
			return fmt.Errorf("close temp tif: %w", err)
		}
		stripReader, err := os.Open(tmpDatasetName)
		if err != nil {
			return fmt.Errorf("failed to reopen %s: %w", tmpDatasetName, err)
		}
		var cogw io.WriteCloser
		if strings.HasPrefix(dstDatasetName, "gs://") {
			b, o, err := adst.Parse(dstDatasetName)
			if err != nil {
				return fmt.Errorf("invalid dst %s: %w", dstDatasetName, err)
			}
			cogw = stcl.Bucket(b).Object(o).NewWriter(ctx)
		} else {
			if cogw, err = os.Create(dstDatasetName); err != nil {
				return fmt.Errorf("create %s: %w", dstDatasetName, err)
			}
		}
		if err = cogger.DefaultConfig().Rewrite(cogw, stripReader); err != nil {
			return fmt.Errorf("cogify strip: %w", err)
		}
		if err = cogw.Close(); err != nil {
			return fmt.Errorf("close %s: %w", dstDatasetName, err)
		}
		if rpc {
			tmpDatasetName += ".vrt"
			defer os.Remove(tmpDatasetName)
			ds, err := godal.Open(dstDatasetName)
			if err != nil {
				return fmt.Errorf("failed to reopen %s: %w", dstDatasetName, err)
			}
			defer ds.Close()
			vrtds, err := ds.Translate(tmpDatasetName, nil, godal.VRT)
			if err != nil {
				return fmt.Errorf("trn to vrt: %w", err)
			}
			vrtds.ClearMetadata(godal.Domain("RPC"))
			vrtds.SetGeoTransform([6]float64{ulx, srcWidth / float64(width), 0, -uly, 0, -srcHeight / float64(height)})
			if err = vrtds.Close(); err != nil {
				return fmt.Errorf("close vrt: %w", err)
			}
			if strings.HasPrefix(dstDatasetName, "gs://") {
				if err = adstcl.UploadFromFile(ctx, dstDatasetName+".vrt", tmpDatasetName); err != nil {
					return fmt.Errorf("upload %s: %w", dstDatasetName+".vrt", err)
				}
			} else {
				if err = os.Rename(tmpDatasetName, dstDatasetName+".vrt"); err != nil {
					return fmt.Errorf("rename vrt to %s: %w", dstDatasetName, err)
				}
			}
		}
		return nil
	},
}

func gdalPreload(datasets []string) error {
	pool := gobs.NewPool(25)
	batch := pool.Batch()
	for _, dsn := range datasets {
		dsn := dsn
		batch.Submit(func() error {
			ds, err := godal.Open(dsn)
			if err != nil {
				return err
			}
			ds.Close()
			return nil
		})
	}
	if err := batch.Wait(); err != nil {
		return err
	}
	return nil
}

var vrtCmd = &cobra.Command{
	Use:   "vrt gs://bucket/vrtfile.vrt gs://bucket/srcstrips.tif...",
	Short: "create vrtfile from srcfiles",
	Args:  cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		dstDatasetName := args[0]
		tmpDataset, _ := ioutil.TempFile(".", "strips-*.vrt")
		tmpDataset.Close()
		tmpDatasetName := tmpDataset.Name()
		defer os.Remove(tmpDatasetName)

		if err := gdalPreload(args[1:]); err != nil {
			return err
		}

		dstDS, err := godal.BuildVRT(tmpDatasetName, args[1:], nil)
		if err != nil {
			return fmt.Errorf("create vrt: %w", err)
		}
		if err = dstDS.Close(); err != nil {
			return fmt.Errorf("close temp vrt: %w", err)
		}
		if strings.HasPrefix(args[0], "gs://") {
			if err = adstcl.UploadFromFile(ctx, dstDatasetName, tmpDatasetName); err != nil {
				return fmt.Errorf("upload: %w", err)
			}
		} else {
			if err = os.Rename(tmpDatasetName, dstDatasetName); err != nil {
				return fmt.Errorf("rename %s->%s: %w", tmpDatasetName, dstDatasetName, err)
			}
		}
		return nil
	},
}
var coggerCmd = &cobra.Command{
	Use:   "cogify cogfile.tif strip-z0-0.tif,strip-z0-1.tif,... strip-z1-0.tif,strip-z1-1.tif,... ...",
	Short: "create cog from strips",
	Args:  cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		dstDatasetName := args[0]
		tiler, err := cogger.NewTiler(width, height, cogger.TargetPixelCount(pixelCount),
			cogger.PreloadTiles(300))
		if err != nil {
			return fmt.Errorf("newtiler: %w", err)
		}
		pyramid := tiler.Tiling()
		readers := make([][]tiff.ReadAtReadSeeker, len(pyramid))
		if len(pyramid) != len(args)-1 {
			return fmt.Errorf("supplied strip pyramid %d does not match expected %d", len(args)-1, len(pyramid))
		}
		for c := range args[1:] {
			stripnames := strings.Split(args[c+1], ",")
			if len(pyramid[c].Strips) != len(stripnames) {
				return fmt.Errorf("level %d has %d strip sources, expecting %d", c,
					len(stripnames), len(pyramid[c].Strips))
			}
			if err := gdalPreload(stripnames); err != nil {
				return err
			}
			stripreaders := make([]tiff.ReadAtReadSeeker, len(stripnames))
			for sr, stripname := range stripnames {
				if stripreaders[sr], err = gcsa.Reader(stripname); err != nil {
					return fmt.Errorf("open %s: %w", stripname, err)
				}
			}
			readers[c] = stripreaders
		}

		var cogw io.WriteCloser
		if strings.HasPrefix(dstDatasetName, "gs://") {
			cogbucket, cogobject, err := adst.Parse(dstDatasetName)
			if err != nil {
				return fmt.Errorf("invalid dst %s: %w", dstDatasetName, err)
			}
			cogw = stcl.Bucket(cogbucket).Object(cogobject).NewWriter(ctx)
		} else {
			cogw, err = os.Create(dstDatasetName)
			if err != nil {
				return fmt.Errorf("create %s: %w", dstDatasetName, err)
			}
		}

		if err := tiler.AssembleStrips(cogw, readers); err != nil {
			return fmt.Errorf("tiler.assemble: %w", err)
		}
		if err := cogw.Close(); err != nil {
			return fmt.Errorf("close %s: %w", dstDatasetName, err)
		}
		return nil
	},
}
