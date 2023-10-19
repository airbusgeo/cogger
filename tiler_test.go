package cogger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTilerOverviews(t *testing.T) {
	testfunc := func(w, h int, expectedlen int) {
		t.Helper()
		tiler, _ := NewStripper(w, h, InternalTileSize(300, 300), MinOverviewSize(3))
		pyramid := tiler.Pyramid()
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
			FullresStripHeightMuliple(heightMultiple))
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
