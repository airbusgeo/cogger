package cogger

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/google/tiff"
	"github.com/stretchr/testify/assert"
)

func TestStripperOverviews(t *testing.T) {
	testfunc := func(w, h int, expectedlen int) {
		t.Helper()
		stripper, _ := NewStripper(w, h, InternalTileSize(300, 300), MinOverviewSize(3))
		pyramid := stripper.Pyramid()
		assert.Len(t, pyramid, expectedlen)
	}
	cases := [][]int{
		{300, 300, 1},
		{299, 299, 1},
		{301, 301, 2},
		{300, 301, 2},
		{301, 300, 2},
		{301, 4, 2},
		{301, 3, 1},
		{301, 2, 1},
		{4, 301, 2},
		{3, 301, 1},
		{2, 301, 1},
	}

	for _, c := range cases {
		testfunc(c[0], c[1], c[2])
	}
	//tiler,_=NewTiler(InternalTileSize(10,10),MinOverviewSize(3),OverviewCount()
}

func TestFullresStripHeightMultipleCount(t *testing.T) {
	testfunc := func(w, h int, tilesize int, targetCount int,
		heightMultiple int, stripheights []float64) {
		t.Helper()
		tiler, _ := NewStripper(w, h, InternalTileSize(tilesize, tilesize),
			TargetPixelCount(targetCount),
			FullresStripHeightMultiple(heightMultiple))
		pyramid := tiler.Pyramid()
		assert.Len(t, pyramid[0].Strips, len(stripheights))
		for s, strip := range pyramid[0].Strips {
			assert.Equal(t, stripheights[s], strip.SrcHeight)
		}
	}
	type tc struct {
		w, h, tilesize, targetCount int
		heightMultiple              int
		stripheights                []float64
	}
	cases := []tc{
		{256, 256, 256, 1024 * 1024, 256, []float64{256}},
		{256, 256, 256, 1024 * 1024, 512, []float64{256}},
		{256, 1024, 256, 1024 * 1024, 512, []float64{1024}},
		{256, 1024, 256, 256 * 256, 512, []float64{512, 512}},
		{256, 1025, 256, 256 * 256, 512, []float64{512, 513}},
		{256, 1023, 256, 256 * 256, 512, []float64{512, 511}},
		{256, 1024, 256, 256 * 256, 768, []float64{768, 256}},
		{256, 1025, 256, 256 * 256, 768, []float64{768, 257}},
		{256, 1023, 256, 256 * 256, 768, []float64{1023}},
	}
	for _, c := range cases {
		testfunc(c.w, c.h, c.tilesize, c.targetCount, c.heightMultiple, c.stripheights)
	}
}

func ExampleStripper() {
	// let's imagine we have an input.jp2 file of size 12345x23456 pixels, that we want to convert to a cog
	//
	// we will do this in parallel using gdal and cogger, using the stripper class to subdivide the task

	inputFile := os.Getenv("STRIPPER_INPUT")
	if inputFile == "" {
		inputFile = "input.jp2"
	}
	width, _ := strconv.Atoi(os.Getenv("STRIPPER_WIDTH"))
	if width == 0 {
		width = 12345
	}
	height, _ := strconv.Atoi(os.Getenv("STRIPPER_HEIGHT"))
	if height == 0 {
		height = 23456
	}

	stripper, _ := NewStripper(width, height)

	pyr := stripper.Pyramid()

	tx, ty := stripper.InternalTileSize()
	copts := fmt.Sprintf("-co TILED=YES -co COMPRESS=LZW -co BLOCKXSIZE=%d -co BLOCKYSIZE=%d", tx, ty)

	srcStrips := [][]tiff.ReadAtReadSeeker{} //used to accumulate the readers of intermediate strips. the ordering must be identical to Pyramid/Pyramid.Strips

	vrt_accum := []string{}
	for l := range pyr {
		fmt.Printf("\nbatch %d\n\n", l+1)
		infile := inputFile
		if l > 0 {
			// if we are in an overview level, optionally build a vrt file making a single gdal image from the all the strips created at the previous level
			if len(vrt_accum) > 1 {
				infile = fmt.Sprintf("l_%d.vrt", l-1)
				fmt.Printf("gdalbuildvrt %s %s\n", infile, strings.Join(vrt_accum, " "))
			} else {
				infile = vrt_accum[0]
			}
			vrt_accum = []string{}
		}

		lStrips := []tiff.ReadAtReadSeeker{}
		for s, strip := range pyr[l].Strips {
			stripname := fmt.Sprintf("tmp_%d_%d.tif", l, s)
			vrt_accum = append(vrt_accum, stripname)
			resizeOpts := ""
			if l > 0 {
				resizeOpts = fmt.Sprintf("-outsize %d %d -r average", strip.Width, strip.Height)
			}
			fmt.Printf("gdal_translate %s %s %s %s -srcwin 0 %g %g %g\n", infile, stripname, copts, resizeOpts,
				strip.SrcTopLeftY, strip.SrcWidth, strip.SrcHeight)
			rs, _ := os.Open(stripname)
			lStrips = append(lStrips, rs)
		}
		srcStrips = append(srcStrips, lStrips)
	}

	outcog, _ := os.Create("stripper-example-output.tif")
	//defer os.Remove(outcog.Name())

	ifdtree, _ := stripper.AssembleStrips(srcStrips)
	_ = ifdtree

	DefaultConfig().RewriteIFDTree(ifdtree, outcog)
	outcog.Close()

	////output: foo
}
