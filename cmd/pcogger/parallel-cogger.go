package main

import (
	"fmt"
	"os"

	"github.com/airbusgeo/cogger"
	"github.com/airbusgeo/godal"
	"github.com/google/tiff"
	"github.com/google/uuid"
	"github.com/sourcegraph/conc/pool"
)

// This program is an example implementation using cogger's stripping functionalities
// to produce a COG using parallel/multi-threaded conversions

func main() {
	input := os.Args[1]
	output := os.Args[2]
	numworkers := 16
	creationOptions := []string{"TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256", "COMPRESS=JXL", "JXL_EFFORT=3", "NUM_THREADS=4"}
	godal.RegisterAll()

	err := process(input, output, numworkers, creationOptions)
	if err != nil {
		err = fmt.Errorf("process %s: %w", input, err)
		fmt.Println(err)
		os.Exit(1)
	}
}

func process(input string, output string, numworkers int, creationOptions []string) error {

	inds, err := godal.Open(input)
	if err != nil {
		return err
	}
	str := inds.Structure()

	opts := []cogger.StripperOption{
		cogger.InternalTileSize(256, 256), //this must match BLOCKXSIZE/BLOCKYSIZE from above !
	}
	if str.BlockSizeY > 256 && str.BlockSizeY%256 > 0 { //align to the blocksize of the source dataset, if possible
		opts = append(opts, cogger.FullresStripHeightMultiple(str.BlockSizeY))
	}

	stripper, err := cogger.NewStripper(str.SizeX, str.SizeY, opts...)
	if err != nil {
		return fmt.Errorf("new stripper: %w", err)
	}

	pyramid := stripper.Pyramid()
	vrt_accum := []string{}
	srcStrips := [][]string{} //used to accumulate the file names of intermediate strips. the ordering must be identical to Pyramid/Pyramid.Strips
	prefix := uuid.Must(uuid.NewRandom()).String()

	for l := range pyramid {
		// if l==0 : read for input dataset
		infile := input
		if l > 0 {
			// we are creating an overview, i.e. we are now referencing strips that have been created at the previous iteration
			// we optionally build a vrt file making a single gdal image from the all the strips created at the previous level
			if len(vrt_accum) > 1 {
				infile = fmt.Sprintf("l%s_%d.vrt", prefix, l-1)
				vds, err := godal.BuildVRT(infile, vrt_accum, nil)
				if err != nil {
					return fmt.Errorf("create vrt %s: %w", infile, err)
				}
				if err := vds.Close(); err != nil {
					return fmt.Errorf("close vrt %s: %w", infile, err)
				}
				defer os.Remove(infile) //nolint:errcheck
			} else {
				// previous iteration created a single strip, no use to create a vrt
				infile = vrt_accum[0]
			}
			vrt_accum = []string{}
		}

		p := pool.New().WithMaxGoroutines(numworkers).WithErrors().WithFirstError()
		lStrips := []string{}
		for s, strip := range pyramid[l].Strips {
			stripname := fmt.Sprintf("s%s_%d_%d.tif", prefix, l, s)
			defer os.Remove(stripname) //nolint:errcheck
			lStrips = append(lStrips, stripname)
			vrt_accum = append(vrt_accum, stripname)
			trnopts := []string{
				"-srcwin", "0", fmt.Sprintf("%g", strip.SrcTopLeftY), fmt.Sprintf("%g", strip.SrcWidth), fmt.Sprintf("%g", strip.SrcHeight),
			}
			if l > 0 {
				//we are in an overview level, so we must specify output size (which will be input size divided by 2) and resampling method
				trnopts = append(trnopts, "-outsize", fmt.Sprintf("%d", strip.Width), fmt.Sprintf("%d", strip.Height), "-r", "average")
			}
			p.Go(func() error {
				ds, err := godal.Open(infile)
				if err != nil {
					return fmt.Errorf("open %s: %w", infile, err)
				}
				defer ds.Close() //nolint:errcheck
				outds, err := ds.Translate(stripname, trnopts, godal.CreationOption(creationOptions...))
				if err != nil {
					return fmt.Errorf("translate %s->%s: %w", infile, stripname, err)
				}
				if err := outds.Close(); err != nil {
					return fmt.Errorf("close %s: %w", stripname, err)
				}
				//fmt.Println("created", stripname)
				return nil
			})
		}
		if err := p.Wait(); err != nil {
			return err
		}
		srcStrips = append(srcStrips, lStrips)
	}

	// get readers on all strips
	readers := [][]tiff.ReadAtReadSeeker{}
	for l := range srcStrips {
		readers = append(readers, []tiff.ReadAtReadSeeker{})
		for s := range srcStrips[l] {
			r, err := os.Open(srcStrips[l][s])
			if err != nil {
				return fmt.Errorf("re-open %s: %w", srcStrips[l][s], err)
			}
			readers[l] = append(readers[l], r)
			defer r.Close() //nolint:errcheck
		}
	}

	ifdtree, err := stripper.AssembleStrips(readers)
	if err != nil {
		return fmt.Errorf("assemble strips: %w", err)
	}

	outcog, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("create %s: %w", output, err)
	}

	if err := cogger.DefaultConfig().RewriteIFDTree(ifdtree, outcog); err != nil {
		return fmt.Errorf("rewrite: %w", err)
	}

	if err := outcog.Close(); err != nil {
		return fmt.Errorf("close %s: %w", output, err)
	}

	return nil

}
