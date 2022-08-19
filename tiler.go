package cogger

import (
	"fmt"
	"io"
	"math"

	"github.com/airbusgeo/osio"
	"github.com/google/tiff"
)

type Tiler struct {
	targetTilePixelCount                      int
	minOverviewSize                           int
	internalTilingWidth, internalTilingHeight int
	overviewCount                             int
	width, height                             int
	pyr                                       Pyramid
	preloadTiles                              int
}

type ErrInvalidOption struct {
	msg string
}

func (err ErrInvalidOption) Error() string {
	return err.msg
}

type TilerOption func(t *Tiler) error

func InternalTileSize(width, height int) TilerOption {
	return func(t *Tiler) error {
		if width <= 0 || height <= 0 {
			return ErrInvalidOption{"internal tile width and height must be >=1"}
		}
		t.internalTilingWidth, t.internalTilingHeight = width, height
		return nil
	}
}

func MinOverviewSize(size int) TilerOption {
	return func(t *Tiler) error {
		if size <= 0 {
			return ErrInvalidOption{"minimal overview size must be >=1"}
		}
		t.minOverviewSize = size
		return nil
	}
}

func OverviewCount(count int) TilerOption {
	return func(t *Tiler) error {
		if count < 0 {
			return ErrInvalidOption{"overview count must be >=0"}
		}
		t.overviewCount = count
		return nil
	}
}
func TargetPixelCount(count int) TilerOption {
	return func(t *Tiler) error {
		if count < 0 {
			return ErrInvalidOption{"target pixel count must be >=0"}
		}
		t.targetTilePixelCount = count
		return nil
	}
}
func PreloadTiles(count int) TilerOption {
	return func(t *Tiler) error {
		if count < 0 {
			return ErrInvalidOption{"preloaded tiles must be >=0"}
		}
		t.preloadTiles = count
		return nil
	}
}

func NewTiler(width, height int, options ...TilerOption) (Tiler, error) {
	var err error
	t := Tiler{
		width:                width,
		height:               height,
		targetTilePixelCount: 8192 * 8192,
		internalTilingWidth:  256,
		internalTilingHeight: 256,
		overviewCount:        -1,
		minOverviewSize:      2,
		preloadTiles:         0,
	}
	for _, o := range options {
		if err := o(&t); err != nil {
			return t, err
		}
	}
	if t.pyr, err = t.pyramid(width, height); err != nil {
		return t, err
	}
	return t, nil
}

type Strip struct {
	SrcTopLeftX, SrcTopLeftY         float64
	SrcBottomRightX, SrcBottomRightY float64
	SrcWidth, SrcHeight              float64
	TargetWidth, TargetHeight        int
}

type Image struct {
	internalTilingWidth, internalTilingHeight int
	Width, Height                             int
	Strips                                    []Strip
}

type Pyramid []Image

func (t Tiler) Tiling() Pyramid {
	return t.pyr
}

type DagStrip struct {
	Level, Strip int        //index inside pyramid
	Parents      []DagStrip //max 3
}

func (t Tiler) DAG(pyr Pyramid) (DagStrip, error) {
	if len(pyr[len(pyr)-1].Strips) != 1 {
		return DagStrip{}, fmt.Errorf("BUG: lowest resolution has more than 1 strip")
	}
	last := DagStrip{
		Level: len(pyr) - 1,
		Strip: 0,
	}

	var parents func(level, strip int) []DagStrip
	parents = func(level, strip int) []DagStrip {
		if level == 0 {
			panic("bug")
		}
		curstrip := pyr[level].Strips[strip]
		top := math.Floor(curstrip.SrcTopLeftX)
		bottom := math.Ceil(curstrip.SrcBottomRightX)

		h := 0.0
		var ret []DagStrip
		for psi, ps := range pyr[level-1].Strips {
			if top > h+ps.SrcHeight || bottom < h {
				continue
			}
			parent := DagStrip{
				Level: level - 1,
				Strip: psi,
			}
			if parent.Level > 0 {
				parent.Parents = parents(parent.Level, parent.Strip)
			}
			ret = append(ret, parent)

			h += curstrip.SrcHeight
		}
		return ret
	}

	if last.Level > 0 {
		last.Parents = parents(last.Level, last.Strip)
	}
	return last, nil
}

func (t Tiler) pyramid(width, height int) (Pyramid, error) {
	if width*height == 0 {
		return nil, ErrInvalidOption{"cannot tile 0-sized image"}
	}
	overviewCount := t.overviewCount
	if overviewCount == -1 {
		iw, ih := width, height
		overviewCount = 0
		for (iw > t.internalTilingWidth || ih > t.internalTilingHeight) &&
			(iw > t.minOverviewSize && ih > t.minOverviewSize) {
			overviewCount++
			iw /= 2
			ih /= 2
		}
	}
	pyramid := make([]Image, overviewCount+1)

	iw, ih := width, height
	pyramid[0] = t.tiling(width, height, width, height)
	for ovr := 1; ovr <= overviewCount; ovr++ {
		if (iw/2)*(ih/2) == 0 {
			return nil, ErrInvalidOption{"requested overview count results in 0-sized image"}
		}
		pyramid[ovr] = t.tiling(iw, ih, iw/2, ih/2)
		iw /= 2
		ih /= 2
	}
	return pyramid, nil
}

func (t Tiler) tiling(srcWidth, srcHeight, dstWidth, dstHeight int) Image {
	if dstWidth*dstHeight == 0 {
		panic("0 sized tiling")
	}
	numStrips := (dstWidth * dstHeight) / t.targetTilePixelCount
	if numStrips == 0 {
		numStrips = 1
	}
	stripHeight := dstHeight / numStrips
	if stripHeight <= t.internalTilingHeight {
		stripHeight = t.internalTilingHeight
	}
	if stripHeight%t.internalTilingHeight != 0 {
		stripHeight = (stripHeight/t.internalTilingHeight + 1) * t.internalTilingHeight
	}

	resY := float64(srcHeight) / float64(dstHeight)
	img := Image{
		internalTilingWidth:  t.internalTilingHeight,
		internalTilingHeight: t.internalTilingHeight,
		Width:                dstWidth,
		Height:               dstHeight,
	}
	dstRow := 0
	srcRow := float64(0)
	for dstRow < dstHeight {
		thisHeight := stripHeight
		if dstRow+stripHeight > dstHeight {
			thisHeight = dstHeight - dstRow
		}
		img.Strips = append(img.Strips, Strip{
			SrcTopLeftX:     0,
			SrcTopLeftY:     srcRow,
			SrcBottomRightX: float64(srcWidth),
			SrcBottomRightY: srcRow + float64(thisHeight)*resY,
			TargetWidth:     dstWidth,
			TargetHeight:    thisHeight,
		})
		dstRow += stripHeight
		srcRow += float64(stripHeight) * resY
	}
	return img
}

type pIFD struct {
	IFD
	readers   []tiff.ReadAtReadSeeker //TODO: close these
	origIFDS  []*IFD
	origMasks []*IFD
	ntx, nty  int //total number of (256x256) tiles
	np        int //number of image planes
}

func unmarshalIFD(ifd tiff.IFD) (IFD, error) {
	cifd := IFD{}
	err := tiff.UnmarshalIFD(ifd, &cifd)
	if err != nil {
		return IFD{}, err
	}
	return cifd, nil
}

//given a tile inside the main cog, return the strip and the index of the tile inside that strip
func (img Image) tileStripIdx(x, y int) (strip int, stripx, stripy int) {
	/*
		ntx := (c.cellXSize + c.internalTileSize - 1) / c.internalTileSize
		nty := (c.cellYSize + c.internalTileSize - 1) / c.internalTileSize
		fx := x / ntx
		fy := y / nty
		cell = fy*c.nCellsX + fx
		cellx = x % ntx
		celly = y % nty
		return
	*/

	ntx := (img.Strips[0].TargetWidth + img.internalTilingWidth - 1) / img.internalTilingWidth
	nty := (img.Strips[0].TargetHeight + img.internalTilingHeight - 1) / img.internalTilingHeight

	strip = y / nty
	stripx = x % ntx
	stripy = y % nty
	return
}

func (t Tiler) AssembleStrips(dstCog io.Writer, srcStrips [][]tiff.ReadAtReadSeeker) error {
	pyr := t.Tiling()
	mainIFD, err := pyr[0].assembleLevelStrips(srcStrips[0])
	if err != nil {
		panic(err)
	}
	for z, ovrStrips := range srcStrips[1:] {
		ovrIFD, err := pyr[z+1].assembleLevelStrips(ovrStrips)
		if err != nil {
			panic(err)
		}
		mainIFD.AddOverview(&ovrIFD.IFD)
	}

	cfg := DefaultConfig()
	cfg.PreloadTiles = t.preloadTiles
	if err = cfg.RewriteIFDTree(&mainIFD.IFD, dstCog); err != nil {
		return fmt.Errorf("rewrite: %w", err)
	}
	return nil
}

func (img Image) assembleLevelStrips(srcStrips []tiff.ReadAtReadSeeker) (*pIFD, error) {
	//prepare the main (synthetic, i.e. not tied to an actual file) IFD
	pifd := &pIFD{}
	pifd.readers = make([]tiff.ReadAtReadSeeker, len(srcStrips))
	pifd.origIFDS = make([]*IFD, len(srcStrips))

	maintifd, err := tiff.Parse(srcStrips[0], nil, nil)
	if err != nil {
		return nil, fmt.Errorf("tiff.parse first strip: %w", err)
	}
	maintifds := maintifd.IFDs()
	if len(maintifds) == 0 || len(maintifds) > 2 {
		return nil, fmt.Errorf("only one or 2 ifds supported, got %d", len(maintifds))
	}
	pifd.IFD, err = unmarshalIFD(maintifds[0])
	if err != nil {
		return nil, fmt.Errorf("unmarshal first strip: %w", err)
	}
	if pifd.SubfileType != 0 { //subfiletype none
		return nil, fmt.Errorf("main ifd subfiletype %d != 0", pifd.SubfileType)
	}
	pifd.TileByteCounts = nil
	pifd.TileOffsets = nil
	var mifdp *IFD
	if len(maintifds) == 2 {
		pifd.origMasks = make([]*IFD, len(srcStrips))
		mifd, err := unmarshalIFD(maintifds[1])
		if err != nil {
			return nil, fmt.Errorf("unmarshal first strip mask: %w", err)
		}
		if mifd.SubfileType != 4 { //subfiletype mask
			return nil, fmt.Errorf("mask subfiletype %d != 4", mifd.SubfileType)
		}
		if mifd.NPlanes() != 1 {
			return nil, fmt.Errorf("mask nplanes=%d must be exactly 1", mifd.NPlanes())
		}
		if mifd.ImageWidth != pifd.ImageWidth || mifd.ImageHeight != pifd.ImageHeight ||
			mifd.TileHeight != pifd.TileHeight || mifd.TileWidth != pifd.TileWidth {
			return nil, fmt.Errorf("mask size/tiling must match main size/tiling")
		}
		mifd.TileByteCounts = nil
		mifd.TileOffsets = nil
		mifdp = &mifd
	}
	pifd.ImageHeight = uint64(img.Height)
	pifd.ImageWidth = uint64(img.Width)
	pifd.ntx, pifd.nty = pifd.NTilesX(), pifd.NTilesY()
	pifd.np = pifd.NPlanes()
	nTifTiles := 0
	nTifMaskTiles := 0

	// plug in the actual cell IFDs
	for s, stripReader := range srcStrips {
		//avoid variable bug in function closures.
		s := s
		stripReader := stripReader

		stripReader.Seek(0, io.SeekStart)
		pifd.readers[s] = stripReader
		tifd, err := tiff.Parse(pifd.readers[s], nil, nil)
		if err != nil {
			return nil, fmt.Errorf("tiff.parse strip %d: %w", s, err)
		}
		tifds := tifd.IFDs()
		if len(tifds) != len(maintifds) {
			return nil, fmt.Errorf("inconsistent ifd/masks")
		}

		cifd, err := unmarshalIFD(tifds[0])
		if err != nil {
			return nil, fmt.Errorf("unmarshal strip %d: %w", s, err)
		}
		nTifTiles += len(cifd.TileByteCounts)
		pifd.origIFDS[s] = &cifd
		if cifd.SubfileType != 0 { //subfiletype none
			return nil, fmt.Errorf("BUG: subfiletype of ifd[0] != 0")
		}

		cifd.LoadTile = func(idx int, data []byte) error {
			if idx >= len(cifd.TileByteCounts) ||
				len(data) != int(cifd.TileByteCounts[idx]) {
				return fmt.Errorf("BUG: len(data)!=TileByteCounts[%d]", idx)
			}
			_, err = stripReader.ReadAt(data, int64(cifd.TileOffsets[idx]))
			if err != nil {
				rr := stripReader.(*osio.Reader)
				return fmt.Errorf("readat len=%d from %d in file of size %d: %w",
					len(data), cifd.TileOffsets[idx], rr.Size(),
					err)
			}
			return nil
		}
		if len(tifds) == 2 {
			mifd, err := unmarshalIFD(tifds[1])
			if err != nil {
				return nil, fmt.Errorf("unmarshal mask for strip %d: %w", s, err)
			}
			pifd.origMasks[s] = &mifd
			if mifd.SubfileType != 4 { //subfiletype mask
				return nil, fmt.Errorf("mask for strip %d subfiletype != 4", s)
			}
			nTifMaskTiles += len(mifd.TileByteCounts)
			mifd.LoadTile = func(idx int, data []byte) error {
				if idx >= len(mifd.TileByteCounts) ||
					len(data) != int(mifd.TileByteCounts[idx]) {
					return fmt.Errorf("BUG: mask len(data)!=TileByteCounts[%d]", idx)
				}
				_, err = stripReader.ReadAt(data, int64(mifd.TileOffsets[idx]))
				return err
			}
		}
	}
	if nTifMaskTiles != 0 && nTifTiles/pifd.np != nTifMaskTiles {
		return nil, fmt.Errorf("inconsistent mask tile count %d vs %d", nTifMaskTiles, nTifTiles)
	}
	if pifd.ntx*pifd.nty*pifd.np != nTifTiles {
		return nil, fmt.Errorf("inconsistent tile count %dx%dx%d vs %d", pifd.np, pifd.ntx, pifd.nty, nTifTiles)
	}
	pifd.TileByteCounts = make([]uint64, nTifTiles)
	oidx := 0
	for p := 0; p < pifd.np; p++ {
		for y := 0; y < pifd.nty; y++ {
			for x := 0; x < pifd.ntx; x++ {
				s, sx, sy := img.tileStripIdx(x, y)
				tidx := pifd.origIFDS[s].TileIdx(sx, sy, p)
				pifd.TileByteCounts[oidx] = pifd.origIFDS[s].TileByteCounts[tidx]
				oidx++
			}
		}
	}
	pifd.LoadTile = func(idx int, data []byte) error {
		x, y, p := pifd.TileFromIdx(idx)
		s, sx, sy := img.tileStripIdx(x, y)
		sidx := pifd.origIFDS[s].TileIdx(sx, sy, p)
		return pifd.origIFDS[s].LoadTile(sidx, data)
	}
	if mifdp != nil {
		mifdp.ImageHeight = pifd.ImageHeight
		mifdp.ImageWidth = pifd.ImageWidth
		if mifdp.NTilesX()*mifdp.NTilesY() != nTifMaskTiles {
			return nil, fmt.Errorf("inconsistent mask tile count %dx%d vs %d", mifdp.NTilesX(), mifdp.NTilesY(), nTifMaskTiles)
		}
		mifdp.TileByteCounts = make([]uint64, nTifMaskTiles)
		ntx, nty := mifdp.NTilesX(), mifdp.NTilesY()
		oidx := 0
		for y := 0; y < nty; y++ {
			for x := 0; x < ntx; x++ {
				s, sx, sy := img.tileStripIdx(x, y)
				tidx := pifd.origMasks[s].TileIdx(sx, sy, 0)
				mifdp.TileByteCounts[oidx] = pifd.origMasks[s].TileByteCounts[tidx]
				oidx++
			}
		}
		mifdp.LoadTile = func(idx int, data []byte) error {
			x, y, p := mifdp.TileFromIdx(idx)
			if p != 0 {
				return fmt.Errorf("BUG: planeidx %d != 0", p)
			}
			s, sx, sy := img.tileStripIdx(x, y)
			sidx := pifd.origMasks[s].TileIdx(sx, sy, 0)
			return pifd.origMasks[s].LoadTile(sidx, data)
		}
		pifd.AddMask(mifdp)
	}
	return pifd, nil
}
