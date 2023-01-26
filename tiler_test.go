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
