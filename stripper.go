package cogger

import (
	"fmt"
	"io"
	"math"

	"github.com/google/tiff"
)

// A Stripper holds the information to inform how to split an image and its
// overviews into strips of roughly similar sizes, allowing these strips to be
// produced outside of the cogger library itself, and ensuring that the internal
// tiling of these individaul strips will be compatible with a cog
// reconstruction.
//
// The usual workflow is to create a Stripper matching the input image's
// dimensions, rough size of each strip to be processed, and output cog
// parameters (namely internal tiling size)
//
// For a full example, see the cmd/pcogger utility
type Stripper struct {
	targetStripPixelCount                     int
	minOverviewSize                           int
	fullresStripHeightMultiple                int
	internalTilingWidth, internalTilingHeight int
	overviewCount                             int
	width, height                             int
	pyr                                       Pyramid
}

type ErrInvalidOption struct {
	msg string
}

func (err ErrInvalidOption) Error() string {
	return err.msg
}

func (s Stripper) Size() (int, int) {
	return s.width, s.height
}

type StripperOption func(t *Stripper) error

// InternalTileSize sets the internal tiling size of the TIF file
func InternalTileSize(width, height int) StripperOption {
	return func(t *Stripper) error {
		if width <= 0 || height <= 0 {
			return ErrInvalidOption{"internal tile width and height must be >=1"}
		}
		t.internalTilingWidth, t.internalTilingHeight = width, height
		return nil
	}
}

func (s Stripper) InternalTileSize() (int, int) {
	return s.internalTilingWidth, s.internalTilingHeight
}

// MinOverviewSize sets the minimal overview size of the TIF file. i.e.
// overviews will stop being added once one of width or height has reached this
// value, even if the other dimension is still over the internal tile size
func MinOverviewSize(size int) StripperOption {
	return func(t *Stripper) error {
		if size <= 0 {
			return ErrInvalidOption{"minimal overview size must be >=1"}
		}
		t.minOverviewSize = size
		return nil
	}
}

func (s Stripper) MinOverviewSize() int {
	return s.minOverviewSize
}

// OverviewCount forces the number of overviews to create. By default we create
// as many as needed in order to reach a size that fits into a single internal
// TIF tile
func OverviewCount(count int) StripperOption {
	return func(t *Stripper) error {
		if count < 0 {
			return ErrInvalidOption{"overview count must be >=0"}
		}
		t.overviewCount = count
		return nil
	}
}

func (s Stripper) OverviewCount() int {
	return s.overviewCount
}

// Approximate number of pixels to use for a single strip. i.e. a single subjob
// will have to process approximately this number of pixels, whatever the size
// of the whole image. Will be adjusted to fit the internal tiling size
func TargetPixelCount(count int) StripperOption {
	return func(t *Stripper) error {
		if count < 0 {
			return ErrInvalidOption{"target pixel count must be >=0"}
		}
		t.targetStripPixelCount = count
		return nil
	}
}

func (s Stripper) TargetPixelCount() int {
	return s.targetStripPixelCount
}

// FullresStripHeightMultiple forces the strip height to be a multiple of the given heightBase
// value provided. This can be useful to ensure that the full resolution strip heights are a
// multiple of the internal tiling height of the source dataset to avoid duplicated source-tile
// decompressions.
func FullresStripHeightMultiple(heightBase int) StripperOption {
	return func(t *Stripper) error {
		if heightBase <= 0 {
			return ErrInvalidOption{"strip height multiple must be >=1"}
		}
		t.fullresStripHeightMultiple = heightBase
		return nil
	}
}

func (s Stripper) FullresStripHeightMultiple() int {
	return s.fullresStripHeightMultiple
}

// NewStripper create a stripper for an image of given width and height.
// Default options are:
// - 64 MPixel strips
// - 256x256 internal tiling
// - overviews down to just under 256 pixels
func NewStripper(width, height int, options ...StripperOption) (Stripper, error) {
	var err error
	t := Stripper{
		width:                      width,
		height:                     height,
		fullresStripHeightMultiple: 0,
		targetStripPixelCount:      8192 * 8192,
		internalTilingWidth:        256,
		internalTilingHeight:       256,
		overviewCount:              -1,
		minOverviewSize:            2,
	}
	for _, o := range options {
		if err := o(&t); err != nil {
			return t, err
		}
	}
	if t.fullresStripHeightMultiple%t.internalTilingHeight != 0 {
		return t, fmt.Errorf("StripHeightMultiple %d must be a multiple of internal tiling height %d", t.fullresStripHeightMultiple, t.internalTilingHeight)
	}
	if t.pyr, err = t.pyramid(width, height); err != nil {
		return t, err
	}
	return t, nil
}

// A Strip is the basic building block of an Image, and corresponds to a rectangle
// of Width*Height pixels who's upper left corner is TopLeftX,TopLeftY. The TopLeftX
// and TopLeftY properties are informative only and are not needed in order to
// process an image with this API.
//
// In order to populate the pixels of a given strip, data must be copied or
// downsampled from a window of a source Image. Depending on the context, the
// source can either be the original image when creating the full-resolution
// IFD, or the full-resolution IFD when creating the first overview, or the
// previous overview when creating another overview.  The source window is
// defined by it's upper left corner (SrcTopLeftX and SrcTopLeftY) and size
// (SrcWidth and SrcHeight)
//
// In gdal lingo, this strip can be calculated by running
//
//	gdal_translate source_dataset.tif strip.tif -srcwin <SrcTopLeftX> <SrcTopLeftY> <SrcWidth> <SrcHeight> -outsize <Width> <Height> -r average
type Strip struct {
	Width, Height            int
	TopLeftX, TopLeftY       int
	SrcTopLeftX, SrcTopLeftY float64
	SrcWidth, SrcHeight      float64
}

// An Image is a Width*Height rectangle of pixels (i.e. there are no overviews
// at this level) and its decompostion into Strips that can be processed
// concurrently
type Image struct {
	internalTilingWidth, internalTilingHeight int
	Width, Height                             int
	Strips                                    []Strip
}

// A Pyramid represents an Image and its overviews.

// The Image at index 0 is the full
// resolution image, and Pyramid[0]'s Strip's Source properties reference the pixel
// frame of the input image.
//
// The Images at index >0 are the overviews, who's Strip's sources reference the image at
// the previous index
type Pyramid []Image

func (t Stripper) Pyramid() Pyramid {
	return t.pyr
}

// A DAG can be used further optimize parrallel strip computing, by explicitely
// referencing which parent strips are needed to be used to downsample the given
// strip of an overview.
// For advanced usage; it is simpler to process all images of a pyramid level in
// parallel before moving down to the next level
type Dag [][]Node

// A Node represents a single Strip in the Dag
type Node struct {
	// Parents are the indexes of the parent strips that will be used to compose the current strip
	Parents []int
	//ParentOffset is the Y position in the parent image of the top-most parent strip
	ParentOffset int
}

// Compute the DAG for a Pyramid.
func (pyr Pyramid) DAG() Dag {
	ret := make(Dag, len(pyr))

	var parents = func(level, strip int) ([]int, int) {
		if level == 0 {
			return nil, 0
		}
		curstrip := pyr[level].Strips[strip]
		top := int(math.Floor(curstrip.SrcTopLeftY))
		bottom := int(math.Ceil(curstrip.SrcTopLeftY+curstrip.SrcHeight)) - 1

		h := 0
		var parentstrips []int
		off := math.MaxInt
		for psi, ps := range pyr[level-1].Strips {
			if top >= h+ps.Height || bottom < h {
				h += ps.Height
				continue
			}
			if h < off {
				off = h
			}
			parentstrips = append(parentstrips, psi)
			h += ps.Height
		}
		return parentstrips, off
	}

	for z, img := range pyr {
		ret[z] = make([]Node, len(img.Strips))
		for s := range img.Strips {
			entries, off := parents(z, s)
			ret[z][s] = Node{Parents: entries, ParentOffset: off}
		}
	}
	return ret
}

func (t Stripper) pyramid(width, height int) (Pyramid, error) {
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
			iw = (int)(math.Ceil(float64(iw) / 2))
			ih = (int)(math.Ceil(float64(ih) / 2))
		}
	}
	pyramid := make([]Image, overviewCount+1)

	iw, ih := width, height
	pyramid[0] = t.stripping(width, height, width, height)
	for ovr := 1; ovr <= overviewCount; ovr++ {
		if iw <= 1 || ih <= 1 {
			return nil, ErrInvalidOption{"requested overview count results in 0-sized image"}
		}
		niw := (int)(math.Ceil(float64(iw) / 2))
		nih := (int)(math.Ceil(float64(ih) / 2))
		pyramid[ovr] = t.stripping(iw, ih, niw, nih)
		iw = niw
		ih = nih
	}
	return pyramid, nil
}

func (t Stripper) stripping(srcWidth, srcHeight, dstWidth, dstHeight int) Image {
	if dstWidth*dstHeight == 0 || srcWidth*srcHeight == 0 {
		panic("0 sized image")
	}
	numStrips := (srcWidth * srcHeight) / t.targetStripPixelCount
	if numStrips == 0 {
		numStrips = 1
	}
	stripHeight := dstHeight / numStrips
	if stripHeight <= t.internalTilingHeight {
		stripHeight = t.internalTilingHeight
	}

	// compute a strip size aligned to the strip height multiple if requested
	// and we are not downsampling, otherwise align to the internal tiling height.
	// NB: the stripHeightMultiple is required to be a multiple of the internalTilingHeight
	if srcWidth == dstWidth && srcHeight == dstHeight && t.fullresStripHeightMultiple != 0 {
		stripHeight = (stripHeight/t.fullresStripHeightMultiple + 1) * t.fullresStripHeightMultiple
	} else if stripHeight%t.internalTilingHeight != 0 {
		stripHeight = (stripHeight/t.internalTilingHeight + 1) * t.internalTilingHeight
	}
	numStrips = int(math.Ceil(float64(dstHeight) / float64(stripHeight)))

	resY := float64(srcHeight) / float64(dstHeight)
	img := Image{
		internalTilingWidth:  t.internalTilingHeight,
		internalTilingHeight: t.internalTilingHeight,
		Width:                dstWidth,
		Height:               dstHeight,
	}
	dstRow := 0
	srcRow := float64(0)
	for s := 0; s < numStrips; s++ {
		thisHeight := stripHeight
		if dstRow+stripHeight > dstHeight {
			thisHeight = dstHeight - dstRow
		}
		if s > 0 && thisHeight < t.internalTilingHeight {
			lastStrip := len(img.Strips) - 1
			img.Strips[lastStrip].SrcHeight += float64(thisHeight) * resY
			img.Strips[lastStrip].Height += thisHeight
		} else {
			img.Strips = append(img.Strips, Strip{
				SrcTopLeftX: 0,
				SrcTopLeftY: srcRow,
				SrcWidth:    float64(srcWidth),
				SrcHeight:   float64(thisHeight) * resY,
				Width:       dstWidth,
				Height:      thisHeight,
				TopLeftX:    0,
				TopLeftY:    dstRow,
			})
		}
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

// given a tile x,y inside the output cog,
// return the corresponding strip and the index stripx,stripy of the tile inside that strip
func (img Image) tileStripIdx(x, y int) (strip int, stripx, stripy int) {
	/* first find the correct strip */
	strip = 0
	accumy := 0
	for {
		stripnty := (img.Strips[strip].Height + img.internalTilingHeight - 1) / img.internalTilingHeight
		if accumy+stripnty <= y {
			accumy += stripnty
			strip++
			continue
		}
		stripy = y - accumy
		break
	}

	ntx := (img.Strips[strip].Width + img.internalTilingWidth - 1) / img.internalTilingWidth
	stripx = x % ntx
	return
}

// AssembleStrips takes the strips that have been created by following the corresponding
// Pyramid structure. The ordering of the strip readers in the srcStrips double array
// must be the same as that of the pyramid structure.
//
// This create a "virtual" IFD tree that has all the caracteristics of final COG
// file, with the tile loading functions that will reference which tile from
// which strip to use.
func (t Stripper) AssembleStrips(srcStrips [][]tiff.ReadAtReadSeeker) (*IFD, error) {
	pyr := t.Pyramid()
	mainIFD, err := pyr[0].assembleLevelStrips(srcStrips[0])
	if err != nil {
		return nil, fmt.Errorf("assemble main ifd: %w", err)
	}
	for z, ovrStrips := range srcStrips[1:] {
		ovrIFD, err := pyr[z+1].assembleLevelStrips(ovrStrips)
		if err != nil {
			return nil, fmt.Errorf("assemble overview %d: %w", z+1, err)
		}
		if err := mainIFD.AddOverview(&ovrIFD.IFD); err != nil {
			return nil, fmt.Errorf("add overview %d: %w", z+1, err)
		}
	}
	return &mainIFD.IFD, nil
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

		if _, err := stripReader.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind strip %d: %w", s, err)
		}
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
				type sizer interface {
					Size() int64
				}
				sz := ""
				if ss, ok := stripReader.(sizer); ok {
					sz = fmt.Sprintf(" in source of size %d", ss.Size())
				}
				return fmt.Errorf("readat len=%d from %d%s: %w",
					len(data), cifd.TileOffsets[idx], sz,
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
		if err := pifd.AddMask(mifdp); err != nil {
			return nil, fmt.Errorf("add mask: %w", err)
		}
	}
	return pifd, nil
}
