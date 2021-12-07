package cogger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	_ "github.com/google/tiff/bigtiff"
)

const (
	subfileTypeNone         = 0
	subfileTypeReducedImage = 1
	subfileTypePage         = 2
	subfileTypeMask         = 4
)

// PlanarInterleaving describes how the band data should be interleaved for tiffs
// with more than 1 plane and with PlanarConfiguration=2
//
// This is an advanced usage option that does not modify the actual image data
// but tweaks the order in which each plane's (i.e. band's) tile is written
// in the resulting file.
//
// Examples for a 3-band rgb image:
//
//  - [[0,1,2]] will result in tiles written in the order r1,g1,b1,r2,g2,b2...rn,gn,bn. This
//    is the default.
//  - [0],[1],[2]] => r1,r2...rn,g1,g2....gn,b1,b2...bn
//  - [0],[2],[1]] => r1,r2...rn,b1,b2....bn,g1,g2...gn
//  - [0,1],[2]] => r1,g1,r2,g2...rn,gn,b1,b2....bn
//
// Examples for a 3-band rgb image with mask:
//
//  - [[0,1,2,3]] will result in tiles written in the order r1,g1,b1,m1,r2,g2,b2,m2...rn,gn,bn,mn. This
//    is the default.
//  - [0],[1],[2],[3]] => r1,r2...rn,g1,g2...gn,b1,b2...bn,m1,m2...mn
//  - [0],[3],[2],[1]] => r1,r2...rn,m1,m2...m3,b1,b2...bn,g1,g2...gn
//  - [0,1],[2],[3]] => r1,g1,r2,g2...rn,gn,b1,b2....bn,m1m2...mn
//
// For a n-band image, each band index from 0 to n-1 must appear exactly once
// in the array. If the image also has a mask, the index n must also appear exactly
// once and represents the mask position.
type PlanarInterleaving [][]int

type IFD struct {
	//Any field added here should also be accounted for in computeStructure and writeIFD
	SubfileType               uint32   `tiff:"field,tag=254"`
	ImageWidth                uint64   `tiff:"field,tag=256"`
	ImageHeight               uint64   `tiff:"field,tag=257"`
	BitsPerSample             []uint16 `tiff:"field,tag=258"`
	Compression               uint16   `tiff:"field,tag=259"`
	PhotometricInterpretation uint16   `tiff:"field,tag=262"`
	DocumentName              string   `tiff:"field,tag=269"`
	SamplesPerPixel           uint16   `tiff:"field,tag=277"`
	PlanarConfiguration       uint16   `tiff:"field,tag=284"`
	DateTime                  string   `tiff:"field,tag=306"`
	Predictor                 uint16   `tiff:"field,tag=317"`
	Colormap                  []uint16 `tiff:"field,tag=320"`
	TileWidth                 uint16   `tiff:"field,tag=322"`
	TileHeight                uint16   `tiff:"field,tag=323"`
	TileOffsets               []uint64 `tiff:"field,tag=324"`
	TileByteCounts            []uint64 `tiff:"field,tag=325"`
	ExtraSamples              []uint16 `tiff:"field,tag=338"`
	SampleFormat              []uint16 `tiff:"field,tag=339"`
	JPEGTables                []byte   `tiff:"field,tag=347"`

	ModelPixelScaleTag     []float64 `tiff:"field,tag=33550"`
	ModelTiePointTag       []float64 `tiff:"field,tag=33922"`
	ModelTransformationTag []float64 `tiff:"field,tag=34264"`
	GeoKeyDirectoryTag     []uint16  `tiff:"field,tag=34735"`
	GeoDoubleParamsTag     []float64 `tiff:"field,tag=34736"`
	GeoAsciiParamsTag      string    `tiff:"field,tag=34737"`
	GDALMetaData           string    `tiff:"field,tag=42112"`
	NoData                 string    `tiff:"field,tag=42113"`
	LERCParams             []uint32  `tiff:"field,tag=50674"`
	RPCs                   []float64 `tiff:"field,tag=50844"`
	LoadTile               func(idx int, data []byte) error

	mask               *IFD   //Optional single-plane mask. Mask.Mask and Mask.Overviews must be nil
	overviews          []*IFD //Optional overviews, sorted from largest to smallest. Overviews.Overviews must be nil.
	newTileOffsets     []uint64
	ntags              int
	tagSize            int
	strileSize         int
	planarInterleaving PlanarInterleaving
}

func (ifd *IFD) nTilesX() int {
	return (int(ifd.ImageWidth) + int(ifd.TileWidth) - 1) / int(ifd.TileWidth)
}
func (ifd *IFD) nTilesY() int {
	return (int(ifd.ImageHeight) + int(ifd.TileHeight) - 1) / int(ifd.TileHeight)
}
func (ifd *IFD) nPlanes() int {
	planeCount := 1
	if ifd.PlanarConfiguration == 2 {
		planeCount = int(ifd.SamplesPerPixel)
	}
	return planeCount
}
func (ifd *IFD) tileIdx(x, y, plane int) int {
	nx, ny := ifd.nTilesX(), ifd.nTilesY()
	return int(nx*ny*plane + y*nx + x)
}
func (ifd *IFD) tileLen(idx int) int {
	return int(ifd.TileByteCounts[idx])
}

// SetPlanarInterleaving configures a non-default planar interleaving
// for this ifd. Must be called after AddMask.
func (ifd *IFD) SetPlanarInterleaving(pi PlanarInterleaving) error {
	if ifd.PlanarConfiguration != 2 {
		return fmt.Errorf("ifd is not PLANARCONFIG_SEPARATE")
	}
	n := int(ifd.SamplesPerPixel)
	if ifd.mask != nil {
		n++
	}
	check := make([]bool, n)
	for _, l1 := range pi {
		for _, l2 := range l1 {
			if l2 < 0 || l2 >= n || check[l2] {
				return fmt.Errorf("invalid/duplicate entry %d", l2)
			}
			check[l2] = true
		}
	}
	for i, l2 := range check {
		if !l2 {
			return fmt.Errorf("missing entry %d", i)
		}
	}
	//deep copy
	for _, l1 := range pi {
		l2 := append([]int{}, l1...)
		ifd.planarInterleaving = append(ifd.planarInterleaving, l2)
	}
	return nil
}

func (ifd *IFD) setDefaultPlanarInterleaving() {
	if ifd.planarInterleaving != nil {
		return
	}
	if ifd.nPlanes() == 1 {
		if ifd.mask != nil {
			ifd.planarInterleaving = [][]int{{0, 1}}
		} else {
			ifd.planarInterleaving = [][]int{{0}}
		}
		return
	}
	n := int(ifd.SamplesPerPixel)
	if ifd.mask != nil {
		n++
	}
	ns := make([]int, n)
	for i := range ns {
		ns[i] = i
	}
	err := ifd.SetPlanarInterleaving(append([][]int{}, ns))
	if err != nil {
		panic(err) //bug
	}
}

func (ifd *IFD) AddOverview(ovr *IFD) error {
	if len(ovr.overviews) > 0 {
		return fmt.Errorf("cannot add overview with embedded overview")
	}
	ovr.SubfileType = subfileTypeReducedImage
	ovr.ModelPixelScaleTag = nil
	ovr.ModelTiePointTag = nil
	ovr.ModelTransformationTag = nil
	ovr.GeoAsciiParamsTag = ""
	ovr.GeoDoubleParamsTag = nil
	ovr.GeoKeyDirectoryTag = nil
	idx := 0
	for idx = range ifd.overviews {
		if ifd.overviews[idx].ImageWidth > ovr.ImageWidth &&
			ifd.overviews[idx].ImageHeight > ovr.ImageHeight {
			idx++
			continue
		}
		break
	}
	prev := ifd
	if len(ifd.overviews) > 0 {
		prev = ifd.overviews[len(ifd.overviews)-1]
	}
	if prev.ImageWidth <= ovr.ImageWidth ||
		prev.ImageHeight <= ovr.ImageHeight {
		return fmt.Errorf("invalid overview size")
	}
	if prev.SamplesPerPixel != ovr.SamplesPerPixel ||
		len(prev.BitsPerSample) != len(ovr.BitsPerSample) {
		return fmt.Errorf("invalid band count")
	}
	ifd.overviews = append(ifd.overviews, nil)
	copy(ifd.overviews[idx+1:], ifd.overviews[idx:])
	ifd.overviews[idx] = ovr
	return nil
}

// AddMask sets msk as a mask for ifd. Must not be called after SetPlanarInterleaving
func (ifd *IFD) AddMask(msk *IFD) error {
	if msk.mask != nil || len(msk.overviews) > 0 {
		return fmt.Errorf("cannot add mask containing overviews or mask")
	}
	if len(ifd.planarInterleaving) > 0 {
		return fmt.Errorf("AddMask must be called before calling SetPlanarInterleaving")
	}
	if msk.ImageWidth != ifd.ImageWidth || msk.ImageHeight != ifd.ImageHeight ||
		msk.TileWidth != ifd.TileWidth || msk.TileHeight != ifd.TileHeight ||
		msk.SamplesPerPixel != 1 || len(msk.BitsPerSample) != 1 ||
		len(msk.TileByteCounts) != len(ifd.TileByteCounts)/ifd.nPlanes() {
		return fmt.Errorf("incompatible mask structure")
	}
	switch ifd.SubfileType {
	case subfileTypeNone:
		msk.SubfileType = subfileTypeMask
	case subfileTypeReducedImage:
		msk.SubfileType = subfileTypeMask | subfileTypeReducedImage
	default:
		return fmt.Errorf("invalid parent subfiletype")
	}
	msk.ModelPixelScaleTag = nil
	msk.ModelTiePointTag = nil
	msk.ModelTransformationTag = nil
	msk.GeoAsciiParamsTag = ""
	msk.GeoDoubleParamsTag = nil
	msk.GeoKeyDirectoryTag = nil
	ifd.mask = msk
	return nil
}

func (cog *cog) computeStructure(ifd *IFD) {
	ifd.ntags = 0
	ifd.tagSize = 16 //8 for field count + 8 for next ifd offset
	ifd.strileSize = 0
	tagSize := 20
	if !cog.bigtiff {
		ifd.tagSize = 6 // 2 for field count + 4 for next ifd offset
		tagSize = 12
	}
	if ifd.SubfileType > 0 {
		ifd.ntags++
		ifd.tagSize += tagSize
	}
	if ifd.ImageWidth > 0 {
		ifd.ntags++
		ifd.tagSize += tagSize
	}
	if ifd.ImageHeight > 0 {
		ifd.ntags++
		ifd.tagSize += tagSize
	}
	if len(ifd.BitsPerSample) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.BitsPerSample, cog.bigtiff)
	}
	if ifd.Compression > 0 {
		ifd.ntags++
		ifd.tagSize += tagSize
	}

	ifd.ntags++ /*PhotometricInterpretation*/
	ifd.tagSize += tagSize

	if len(ifd.DocumentName) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.DocumentName, cog.bigtiff)
	}
	if ifd.SamplesPerPixel > 0 {
		ifd.ntags++
		ifd.tagSize += tagSize
	}
	if ifd.PlanarConfiguration > 0 {
		ifd.ntags++
		ifd.tagSize += tagSize
	}
	if len(ifd.DateTime) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.DateTime, cog.bigtiff)
	}
	if ifd.Predictor > 0 {
		ifd.ntags++
		ifd.tagSize += tagSize
	}
	if len(ifd.Colormap) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.Colormap, cog.bigtiff)
	}
	if ifd.TileWidth > 0 {
		ifd.ntags++
		ifd.tagSize += tagSize
	}
	if ifd.TileHeight > 0 {
		ifd.ntags++
		ifd.tagSize += tagSize
	}
	//ignore original offsets, get the number of them from the TileByteCounts
	if len(ifd.TileByteCounts) > 0 {
		ifd.ntags++
		ifd.tagSize += tagSize
		if cog.bigtiff {
			ifd.strileSize += arrayFieldSize(ifd.TileByteCounts, cog.bigtiff) - tagSize
		} else {
			ifd.strileSize += arrayFieldSize32(ifd.TileByteCounts, cog.bigtiff) - tagSize
		}
	}
	if len(ifd.TileByteCounts) > 0 {
		ifd.ntags++
		ifd.tagSize += tagSize
		ifd.strileSize += arrayFieldSize32(ifd.TileByteCounts, cog.bigtiff) - tagSize
	}
	if len(ifd.ExtraSamples) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.ExtraSamples, cog.bigtiff)
	}
	if len(ifd.SampleFormat) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.SampleFormat, cog.bigtiff)
	}
	if len(ifd.JPEGTables) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.JPEGTables, cog.bigtiff)
	}
	if len(ifd.ModelPixelScaleTag) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.ModelPixelScaleTag, cog.bigtiff)
	}
	if len(ifd.ModelTiePointTag) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.ModelTiePointTag, cog.bigtiff)
	}
	if len(ifd.ModelTransformationTag) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.ModelTransformationTag, cog.bigtiff)
	}
	if len(ifd.GeoKeyDirectoryTag) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.GeoKeyDirectoryTag, cog.bigtiff)
	}
	if len(ifd.GeoDoubleParamsTag) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.GeoDoubleParamsTag, cog.bigtiff)
	}
	if ifd.GeoAsciiParamsTag != "" {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.GeoAsciiParamsTag, cog.bigtiff)
	}
	if ifd.GDALMetaData != "" {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.GDALMetaData, cog.bigtiff)
	}
	if ifd.NoData != "" {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.NoData, cog.bigtiff)
	}
	if len(ifd.LERCParams) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.LERCParams, cog.bigtiff)
	}
	if len(ifd.RPCs) > 0 {
		ifd.ntags++
		ifd.tagSize += arrayFieldSize(ifd.RPCs, cog.bigtiff)
	}
}

type tagData struct {
	bytes.Buffer
	Offset int
}

func (t *tagData) NextOffset() int {
	return t.Offset + t.Buffer.Len()
}

type Config struct {
	//Encoding selects big or little endian tiff encoding. Default: little
	Encoding binary.ByteOrder

	//BigTIFF forces bigtiff creation. Default: false, i.e. only if needed
	BigTIFF bool

	// PlanarInterleaving for separate-plane files.
	// Default: nil resulting in {{0,1,...n}} i.e. interleaved planes
	PlanarInterleaving PlanarInterleaving

	//WithGDALGhostArea inserts gdal specific read optimizations
	WithGDALGhostArea bool
}

func DefaultConfig() Config {
	return Config{
		Encoding:          binary.LittleEndian,
		BigTIFF:           false,
		WithGDALGhostArea: true,
	}
}

type cog struct {
	enc           binary.ByteOrder
	ifd           *IFD
	bigtiff       bool
	withGDALGhost bool
}

func (cog *cog) writeHeader(w io.Writer) error {
	glen := uint64(0)
	if cog.withGDALGhost {
		glen = uint64(len(ghost))
		if cog.ifd.mask != nil {
			glen = uint64(len(ghostmask))
		}
	}
	var err error
	if cog.bigtiff {
		buf := [16]byte{}
		if cog.enc == binary.LittleEndian {
			copy(buf[0:], []byte("II"))
		} else {
			copy(buf[0:], []byte("MM"))
		}
		cog.enc.PutUint16(buf[2:], 43)
		cog.enc.PutUint16(buf[4:], 8)
		cog.enc.PutUint16(buf[6:], 0)
		cog.enc.PutUint64(buf[8:], 16+glen)
		_, err = w.Write(buf[:])
	} else {
		buf := [8]byte{}
		if cog.enc == binary.LittleEndian {
			copy(buf[0:], []byte("II"))
		} else {
			copy(buf[0:], []byte("MM"))
		}
		cog.enc.PutUint16(buf[2:], 42)
		cog.enc.PutUint32(buf[4:], 8+uint32(glen))
		_, err = w.Write(buf[:])
	}
	if err != nil {
		return err
	}
	if cog.withGDALGhost {
		if cog.ifd.mask != nil {
			_, err = w.Write([]byte(ghostmask))
		} else {
			_, err = w.Write([]byte(ghost))
		}
	}
	return err
}

const ghost = `GDAL_STRUCTURAL_METADATA_SIZE=000140 bytes
LAYOUT=IFDS_BEFORE_DATA
BLOCK_ORDER=ROW_MAJOR
BLOCK_LEADER=SIZE_AS_UINT4
BLOCK_TRAILER=LAST_4_BYTES_REPEATED
KNOWN_INCOMPATIBLE_EDITION=NO
  ` //2 spaces: 1 for the gdal spec, and one to ensure the actual start offset is on a word boundary

const ghostmask = `GDAL_STRUCTURAL_METADATA_SIZE=000174 bytes
LAYOUT=IFDS_BEFORE_DATA
BLOCK_ORDER=ROW_MAJOR
BLOCK_LEADER=SIZE_AS_UINT4
BLOCK_TRAILER=LAST_4_BYTES_REPEATED
KNOWN_INCOMPATIBLE_EDITION=NO
 MASK_INTERLEAVED_WITH_IMAGERY=YES
` //the space at the start of the last line is required to make room for changing NO to YES

func (cog *cog) computeImageryOffsets() error {
	nplanes := cog.ifd.nPlanes()
	haveMask := false
	cog.computeStructure(cog.ifd)
	if cog.ifd.mask != nil {
		cog.computeStructure(cog.ifd.mask)
		haveMask = true
	}
	for _, oifd := range cog.ifd.overviews {
		if oifd.nPlanes() != nplanes {
			return fmt.Errorf("inconsistent band count")
		}
		iHaveMask := oifd.mask != nil
		if iHaveMask != haveMask {
			return fmt.Errorf("inconsistent mask count")
		}
		cog.computeStructure(oifd)
		if oifd.mask != nil {
			cog.computeStructure(oifd.mask)
		}
	}

	//offset to start of image data
	dataOffset := uint64(16)
	if !cog.bigtiff {
		dataOffset = 8
	}
	if cog.withGDALGhost {
		if cog.ifd.mask != nil {
			dataOffset += uint64(len(ghostmask) + 4)
		} else {
			dataOffset += uint64(len(ghost) + 4)
		}
	}

	dataOffset += uint64(cog.ifd.strileSize + cog.ifd.tagSize)
	if cog.ifd.mask != nil {
		dataOffset += uint64(cog.ifd.mask.strileSize + cog.ifd.mask.tagSize)
	}
	for _, ifd := range cog.ifd.overviews {
		dataOffset += uint64(ifd.strileSize + ifd.tagSize)
		if ifd.mask != nil {
			dataOffset += uint64(ifd.mask.strileSize + ifd.mask.tagSize)
		}
	}

	datas := cog.ifdInterlacing()
	tiles := cog.tiles(datas)
	for tile := range tiles {
		tileidx := tile.ifd.tileIdx(tile.x, tile.y, tile.plane)
		if tile.ifd.tileLen(tileidx) > 0 {
			if cog.bigtiff {
				tile.ifd.newTileOffsets[tileidx] = dataOffset
			} else {
				if dataOffset > uint64(^uint32(0)) { //^uint32(0) is max uint32
					//rerun with bigtiff support

					//first empty out the tiles channel to avoid a goroutine leak
					for range tiles {
						//skip
					}
					cog.bigtiff = true
					return cog.computeImageryOffsets()
				}
				tile.ifd.newTileOffsets[tileidx] = dataOffset
			}
			dataOffset += tile.ifd.TileByteCounts[tileidx]
			if cog.withGDALGhost {
				dataOffset += 8
			}
		} else {
			tile.ifd.newTileOffsets[tileidx] = 0
		}
	}
	return nil
}

func (cfg Config) RewriteIFDTree(ifd *IFD, out io.Writer) error {
	cog := &cog{
		enc:           cfg.Encoding,
		bigtiff:       cfg.BigTIFF,
		withGDALGhost: cfg.WithGDALGhostArea,
		ifd:           ifd,
	}
	havePlanar := ifd.nPlanes() > 1
	for _, oifd := range ifd.overviews {
		if oifd.nPlanes() > 1 {
			havePlanar = true
		}
	}
	if havePlanar {
		cog.withGDALGhost = false
	}
	if len(cfg.PlanarInterleaving) == 0 {
		//set all unset to default
		ifd.setDefaultPlanarInterleaving()
		for _, ovr := range ifd.overviews {
			ovr.setDefaultPlanarInterleaving()
		}
	} else {
		//set all unset to configured value
		if len(ifd.planarInterleaving) == 0 { //don't override existing
			if err := ifd.SetPlanarInterleaving(cfg.PlanarInterleaving); err != nil {
				return fmt.Errorf("invalid planar interleaving: %w", err)
			}
		}
		for o, ovr := range ifd.overviews {
			if len(ovr.planarInterleaving) == 0 { //don't override existing
				if err := ovr.SetPlanarInterleaving(cfg.PlanarInterleaving); err != nil {
					return fmt.Errorf("invalid planar interleaving for overview %d: %w", o, err)
				}
			}
		}
	}

	ifd.newTileOffsets = make([]uint64, len(ifd.TileByteCounts))
	if ifd.mask != nil {
		ifd.mask.newTileOffsets = make([]uint64, len(ifd.mask.TileByteCounts))
	}
	for _, oifd := range ifd.overviews {
		oifd.newTileOffsets = make([]uint64, len(oifd.TileByteCounts))
		if oifd.mask != nil {
			oifd.mask.newTileOffsets = make([]uint64, len(oifd.mask.TileByteCounts))
		}
	}
	err := cog.computeImageryOffsets()
	if err != nil {
		return err
	}

	//compute start of strile data, and offsets to subIFDs
	//striles are placed after all ifds
	strileData := &tagData{Offset: 16}
	if !cog.bigtiff {
		strileData.Offset = 8
	}
	if cog.withGDALGhost {
		if ifd.mask != nil {
			strileData.Offset += len(ghostmask)
		} else {
			strileData.Offset += len(ghost)
		}
	}

	strileData.Offset += ifd.tagSize
	if ifd.mask != nil {
		strileData.Offset += ifd.mask.tagSize
	}
	for _, oifd := range ifd.overviews {
		strileData.Offset += oifd.tagSize
		if oifd.mask != nil {
			strileData.Offset += oifd.mask.tagSize
		}
	}

	cog.writeHeader(out)

	off := 16
	if !cog.bigtiff {
		off = 8
	}
	if cog.withGDALGhost {
		if cog.ifd.mask != nil {
			off += len(ghostmask)
		} else {
			off += len(ghost)
		}
	}

	err = cog.writeIFD(out, ifd, off, strileData, ifd.mask != nil || len(ifd.overviews) > 0)
	if err != nil {
		return fmt.Errorf("write main ifd: %w", err)
	}
	off += ifd.tagSize
	if ifd.mask != nil {
		err = cog.writeIFD(out, ifd.mask, off, strileData, len(ifd.overviews) > 0)
		if err != nil {
			return fmt.Errorf("write mask: %w", err)
		}
		off += ifd.mask.tagSize
	}

	for i, oifd := range ifd.overviews {
		err = cog.writeIFD(out, oifd, off, strileData,
			oifd.mask != nil || i != len(ifd.overviews)-1)
		if err != nil {
			return fmt.Errorf("write overview ifd %d: %w", i, err)
		}
		off += ifd.tagSize
		if oifd.mask != nil {
			err := cog.writeIFD(out, oifd.mask, off, strileData,
				i != len(ifd.overviews)-1)
			if err != nil {
				return fmt.Errorf("write ifd: %w", err)
			}
			off += oifd.mask.tagSize
		}
	}

	_, err = out.Write(strileData.Bytes())
	if err != nil {
		return fmt.Errorf("write strile pointers: %w", err)
	}

	datas := cog.ifdInterlacing()
	tiles := cog.tiles(datas)
	data := []byte{}
	for tile := range tiles {
		idx := tile.ifd.tileIdx(tile.x, tile.y, tile.plane)
		bc := tile.ifd.tileLen(idx)
		if bc > 0 {
			if len(data) < bc+8 {
				data = make([]byte, (bc+8)*2)
			}
			binary.LittleEndian.PutUint32(data, uint32(bc)) //header ghost: tile size
			err = tile.Data(data[4 : 4+bc])
			if err != nil {
				return fmt.Errorf("tile.data: %w", err)
			}
			copy(data[4+bc:8+bc], data[bc:4+bc]) //trailer ghost: repeat last 4 bytes
			if cog.withGDALGhost {
				_, err = out.Write(data[0 : bc+8])
			} else {
				_, err = out.Write(data[4 : bc+4])
			}
			if err != nil {
				return fmt.Errorf("write %d: %w", bc, err)
			}
		}
	}

	return err
}

func (cog *cog) writeIFD(w io.Writer, ifd *IFD, offset int, striledata *tagData, next bool) error {

	nextOff := 0
	if next {
		nextOff = offset + ifd.tagSize
	}
	var err error
	// Make space for "pointer area" containing IFD entry data
	// longer than 4 bytes.
	overflow := &tagData{
		Offset: offset + 8 + 20*ifd.ntags + 8,
	}
	if !cog.bigtiff {
		overflow.Offset = offset + 2 + 12*ifd.ntags + 4
	}

	if cog.bigtiff {
		err = binary.Write(w, cog.enc, uint64(ifd.ntags))
	} else {
		err = binary.Write(w, cog.enc, uint16(ifd.ntags))
	}
	if err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	if ifd.SubfileType > 0 {
		err := cog.writeField(w, 254, ifd.SubfileType)
		if err != nil {
			panic(err)
		}
	}
	if ifd.ImageWidth > 0 {
		err := cog.writeField(w, 256, uint32(ifd.ImageWidth))
		if err != nil {
			panic(err)
		}
	}
	if ifd.ImageHeight > 0 {
		err := cog.writeField(w, 257, uint32(ifd.ImageHeight))
		if err != nil {
			panic(err)
		}
	}

	if len(ifd.BitsPerSample) > 0 {
		err := cog.writeArray(w, 258, ifd.BitsPerSample, overflow)
		if err != nil {
			panic(err)
		}
	}

	if ifd.Compression > 0 {
		err := cog.writeField(w, 259, ifd.Compression)
		if err != nil {
			panic(err)
		}
	}

	err = cog.writeField(w, 262, ifd.PhotometricInterpretation)
	if err != nil {
		panic(err)
	}

	//DocumentName              string   `tiff:"field,tag=269"`
	if len(ifd.DocumentName) > 0 {
		err := cog.writeArray(w, 269, ifd.DocumentName, overflow)
		if err != nil {
			panic(err)
		}
	}

	//SamplesPerPixel           uint16   `tiff:"field,tag=277"`
	if ifd.SamplesPerPixel > 0 {
		err := cog.writeField(w, 277, ifd.SamplesPerPixel)
		if err != nil {
			panic(err)
		}
	}

	//PlanarConfiguration       uint16   `tiff:"field,tag=284"`
	if ifd.PlanarConfiguration > 0 {
		err := cog.writeField(w, 284, ifd.PlanarConfiguration)
		if err != nil {
			panic(err)
		}
	}

	//DateTime                  string   `tiff:"field,tag=306"`
	if len(ifd.DateTime) > 0 {
		err := cog.writeArray(w, 306, ifd.DateTime, overflow)
		if err != nil {
			panic(err)
		}
	}

	//Predictor                 uint16   `tiff:"field,tag=317"`
	if ifd.Predictor > 0 {
		err := cog.writeField(w, 317, ifd.Predictor)
		if err != nil {
			panic(err)
		}
	}

	//Colormap                  []uint16 `tiff:"field,tag=320"`
	if len(ifd.Colormap) > 0 {
		err := cog.writeArray(w, 320, ifd.Colormap, overflow)
		if err != nil {
			panic(err)
		}
	}

	//TileWidth                 uint16   `tiff:"field,tag=322"`
	if ifd.TileWidth > 0 {
		err := cog.writeField(w, 322, ifd.TileWidth)
		if err != nil {
			panic(err)
		}
	}

	//TileHeight                uint16   `tiff:"field,tag=323"`
	if ifd.TileHeight > 0 {
		err := cog.writeField(w, 323, ifd.TileHeight)
		if err != nil {
			panic(err)
		}
	}

	//TileOffsets               []uint64 `tiff:"field,tag=324"`
	if len(ifd.newTileOffsets) > 0 {
		var err error
		if cog.bigtiff {
			err = cog.writeArray(w, 324, ifd.newTileOffsets, striledata)
		} else {
			err = cog.writeArray32(w, 324, ifd.newTileOffsets, striledata)
		}
		if err != nil {
			panic(err)
		}
	}

	//TileByteCounts            []uint64 `tiff:"field,tag=325"`
	if len(ifd.TileByteCounts) > 0 {
		err := cog.writeArray32(w, 325, ifd.TileByteCounts, striledata)
		if err != nil {
			panic(err)
		}
	}

	//ExtraSamples              []uint16 `tiff:"field,tag=338"`
	if len(ifd.ExtraSamples) > 0 {
		err := cog.writeArray(w, 338, ifd.ExtraSamples, overflow)
		if err != nil {
			panic(err)
		}
	}

	//SampleFormat              []uint16 `tiff:"field,tag=339"`
	if len(ifd.SampleFormat) > 0 {
		err := cog.writeArray(w, 339, ifd.SampleFormat, overflow)
		if err != nil {
			panic(err)
		}
	}

	//JPEGTables                []byte   `tiff:"field,tag=347"`
	if len(ifd.JPEGTables) > 0 {
		err := cog.writeArray(w, 347, ifd.JPEGTables, overflow)
		if err != nil {
			panic(err)
		}
	}

	//ModelPixelScaleTag     []float64 `tiff:"field,tag=33550"`
	if len(ifd.ModelPixelScaleTag) > 0 {
		err := cog.writeArray(w, 33550, ifd.ModelPixelScaleTag, overflow)
		if err != nil {
			panic(err)
		}
	}

	//ModelTiePointTag       []float64 `tiff:"field,tag=33922"`
	if len(ifd.ModelTiePointTag) > 0 {
		err := cog.writeArray(w, 33922, ifd.ModelTiePointTag, overflow)
		if err != nil {
			panic(err)
		}
	}

	//ModelTransformationTag []float64 `tiff:"field,tag=34264"`
	if len(ifd.ModelTransformationTag) > 0 {
		err := cog.writeArray(w, 34264, ifd.ModelTransformationTag, overflow)
		if err != nil {
			panic(err)
		}
	}

	//GeoKeyDirectoryTag     []uint16  `tiff:"field,tag=34735"`
	if len(ifd.GeoKeyDirectoryTag) > 0 {
		err := cog.writeArray(w, 34735, ifd.GeoKeyDirectoryTag, overflow)
		if err != nil {
			panic(err)
		}
	}

	//GeoDoubleParamsTag     []float64 `tiff:"field,tag=34736"`
	if len(ifd.GeoDoubleParamsTag) > 0 {
		err := cog.writeArray(w, 34736, ifd.GeoDoubleParamsTag, overflow)
		if err != nil {
			panic(err)
		}
	}

	//GeoAsciiParamsTag      string    `tiff:"field,tag=34737"`
	if len(ifd.GeoAsciiParamsTag) > 0 {
		err := cog.writeArray(w, 34737, ifd.GeoAsciiParamsTag, overflow)
		if err != nil {
			panic(err)
		}
	}

	if ifd.GDALMetaData != "" {
		err := cog.writeArray(w, 42112, ifd.GDALMetaData, overflow)
		if err != nil {
			panic(err)
		}
	}
	//NoData string `tiff:"field,tag=42113"`
	if len(ifd.NoData) > 0 {
		err := cog.writeArray(w, 42113, ifd.NoData, overflow)
		if err != nil {
			panic(err)
		}
	}
	if len(ifd.LERCParams) > 0 {
		err := cog.writeArray(w, 50674, ifd.LERCParams, overflow)
		if err != nil {
			panic(err)
		}
	}
	if len(ifd.RPCs) > 0 {
		err := cog.writeArray(w, 50844, ifd.RPCs, overflow)
		if err != nil {
			panic(err)
		}
	}

	if cog.bigtiff {
		err = binary.Write(w, cog.enc, uint64(nextOff))
	} else {
		err = binary.Write(w, cog.enc, uint32(nextOff))
	}
	if err != nil {
		return fmt.Errorf("write next: %w", err)
	}
	_, err = w.Write(overflow.Bytes())
	if err != nil {
		return fmt.Errorf("write parea: %w", err)
	}
	return nil
}

type tile struct {
	ifd   *IFD
	x, y  int
	plane int
}

func (t tile) Data(data []byte) error {
	idx := t.ifd.tileIdx(t.x, t.y, t.plane)
	{ //safety net
		tl := t.ifd.tileLen(idx)
		if len(data) != int(tl) {
			panic("wrong buffer size")
		}
	}
	if len(data) > 0 {
		return t.ifd.LoadTile(idx, data)
	}
	return nil
}

type entry struct { //todo: rename this
	ifd  *IFD
	mask *IFD
}

type entries []entry //todo: rename this

func (cog *cog) ifdInterlacing() entries {
	//count overviews
	ret := make([]entry, 1+len(cog.ifd.overviews))
	havemask := cog.ifd.mask != nil
	if havemask {
		ret[len(cog.ifd.overviews)] = entry{cog.ifd, cog.ifd.mask}
	} else {
		ret[len(cog.ifd.overviews)] = entry{cog.ifd, nil}
	}
	for idx := 0; idx < len(cog.ifd.overviews); idx++ {
		oifd := cog.ifd.overviews[len(cog.ifd.overviews)-1-idx]
		if havemask {
			ret[idx] = entry{oifd, oifd.mask}
		} else {
			ret[idx] = entry{oifd, nil}
		}
	}
	return ret
}

func (cog *cog) tiles(entries entries) chan tile {
	ch := make(chan tile)
	go func() {
		defer close(ch)
		for _, entry := range entries {
			maskIdx := -1
			if entry.mask != nil {
				if entry.ifd.PlanarConfiguration == 2 {
					maskIdx = int(entry.ifd.SamplesPerPixel)
				} else {
					maskIdx = 1
				}
			}
			ntx, nty := entry.ifd.nTilesX(), entry.ifd.nTilesY()
			for _, l1 := range entry.ifd.planarInterleaving {
				for y := 0; y < nty; y++ {
					for x := 0; x < ntx; x++ {
						for _, p := range l1 {
							if p != maskIdx {
								ch <- tile{
									ifd:   entry.ifd,
									plane: p,
									x:     x,
									y:     y,
								}
							} else {
								ch <- tile{
									ifd:   entry.mask,
									plane: 0,
									x:     x,
									y:     y,
								}
							}
						}
					}
				}
			}
		}

	}()
	return ch
}
