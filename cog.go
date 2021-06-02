package cogger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/google/tiff"
	_ "github.com/google/tiff/bigtiff"
)

type SubfileType uint32

const (
	SubfileTypeNone         = 0
	SubfileTypeReducedImage = 1
	SubfileTypePage         = 2
	SubfileTypeMask         = 4
)

type PlanarConfiguration uint16

const (
	PlanarConfigurationContig   = 1
	PlanarConfigurationSeparate = 2
)

type Predictor uint16

const (
	PredictorNone          = 1
	PredictorHorizontal    = 2
	PredictorFloatingPoint = 3
)

type SampleFormat uint16

const (
	SampleFormatUInt          = 1
	SampleFormatInt           = 2
	SampleFormatIEEEFP        = 3
	SampleFormatVoid          = 4
	SampleFormatComplexInt    = 5
	SampleFormatComplexIEEEFP = 6
)

type ExtraSamples uint16

const (
	ExtraSamplesUnspecified = 0
	ExtraSamplesAssocAlpha  = 1
	ExtraSamplesUnassAlpha  = 2
)

type PhotometricInterpretation uint16

const (
	PhotometricInterpretationMinIsWhite = 0
	PhotometricInterpretationMinIsBlack = 1
	PhotometricInterpretationRGB        = 2
	PhotometricInterpretationPalette    = 3
	PhotometricInterpretationMask       = 4
	PhotometricInterpretationSeparated  = 5
	PhotometricInterpretationYCbCr      = 6
	PhotometricInterpretationCIELab     = 8
	PhotometricInterpretationICCLab     = 9
	PhotometricInterpretationITULab     = 10
	PhotometricInterpretationLOGL       = 32844
	PhotometricInterpretationLOGLUV     = 32845
)

type IFD struct {
	//Any field added here should also be accounted for in WriteIFD and ifd.Fieldcount
	SubfileType               uint32   `tiff:"field,tag=254"`
	ImageWidth                uint64   `tiff:"field,tag=256"`
	ImageLength               uint64   `tiff:"field,tag=257"`
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
	TileLength                uint16   `tiff:"field,tag=323"`
	OriginalTileOffsets       []uint64 `tiff:"field,tag=324"`
	NewTileOffsets64          []uint64
	NewTileOffsets32          []uint32
	TempTileByteCounts        []uint64 `tiff:"field,tag=325"`
	TileByteCounts            []uint32
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

	overview *IFD
	masks    []*IFD

	ntags            uint64
	ntilesx, ntilesy uint64
	tagsSize         uint64
	strileSize       uint64
	r                tiff.BReader
}

/*
func (ifd *IFD) TagCount() uint64 {
	s, _, _ := ifd.Structure()
	return s
}
func (ifd *IFD) TagsSize() uint64 {
	_, s, _ := ifd.Structure()
	return s
}
func (ifd *IFD) StrileSize() uint64 {
	_, _, s := ifd.Structure()
	return s
}
*/

func (ifd *IFD) AddOverview(ovr *IFD) {
	ovr.SubfileType = SubfileTypeReducedImage
	ovr.ModelPixelScaleTag = nil
	ovr.ModelTiePointTag = nil
	ovr.ModelTransformationTag = nil
	ovr.GeoAsciiParamsTag = ""
	ovr.GeoDoubleParamsTag = nil
	ovr.GeoKeyDirectoryTag = nil
	ifd.overview = ovr
}
func (ifd *IFD) AddMask(msk *IFD) error {
	if len(msk.masks) > 0 || msk.overview != nil {
		return fmt.Errorf("cannot add mask with overviews or masks")
	}
	switch ifd.SubfileType {
	case SubfileTypeNone:
		msk.SubfileType = SubfileTypeMask
	case SubfileTypeReducedImage:
		msk.SubfileType = SubfileTypeMask | SubfileTypeReducedImage
	default:
		return fmt.Errorf("invalid subfiledtype")
	}
	msk.ModelPixelScaleTag = nil
	msk.ModelTiePointTag = nil
	msk.ModelTransformationTag = nil
	msk.GeoAsciiParamsTag = ""
	msk.GeoDoubleParamsTag = nil
	msk.GeoKeyDirectoryTag = nil
	ifd.masks = append(ifd.masks, msk)
	return nil
}

func (ifd *IFD) structure(bigtiff bool) (tagCount, ifdSize, strileSize uint64) {
	cnt := uint64(0)
	size := uint64(16) //8 for field count + 8 for next ifd offset
	tagSize := uint64(20)
	if !bigtiff {
		size = 6 // 2 for field count + 4 for next ifd offset
		tagSize = 12
	}
	strileSize = uint64(0)

	if ifd.SubfileType > 0 {
		cnt++
		size += tagSize
	}
	if ifd.ImageWidth > 0 {
		cnt++
		size += tagSize
	}
	if ifd.ImageLength > 0 {
		cnt++
		size += tagSize
	}
	if len(ifd.BitsPerSample) > 0 {
		cnt++
		size += arrayFieldSize(ifd.BitsPerSample, bigtiff)
	}
	if ifd.Compression > 0 {
		cnt++
		size += tagSize
	}

	cnt++ /*PhotometricInterpretation*/
	size += tagSize

	if len(ifd.DocumentName) > 0 {
		cnt++
		size += arrayFieldSize(ifd.DocumentName, bigtiff)
	}
	if ifd.SamplesPerPixel > 0 {
		cnt++
		size += tagSize
	}
	if ifd.PlanarConfiguration > 0 {
		cnt++
		size += tagSize
	}
	if len(ifd.DateTime) > 0 {
		cnt++
		size += arrayFieldSize(ifd.DateTime, bigtiff)
	}
	if ifd.Predictor > 0 {
		cnt++
		size += tagSize
	}
	if len(ifd.Colormap) > 0 {
		cnt++
		size += arrayFieldSize(ifd.BitsPerSample, bigtiff)
	}
	if ifd.TileWidth > 0 {
		cnt++
		size += tagSize
	}
	if ifd.TileLength > 0 {
		cnt++
		size += tagSize
	}
	if len(ifd.NewTileOffsets32) > 0 {
		cnt++
		size += tagSize
		strileSize += arrayFieldSize(ifd.NewTileOffsets32, bigtiff) - tagSize
	} else if len(ifd.NewTileOffsets64) > 0 {
		cnt++
		size += tagSize
		strileSize += arrayFieldSize(ifd.NewTileOffsets64, bigtiff) - tagSize
	}
	if len(ifd.TileByteCounts) > 0 {
		cnt++
		size += tagSize
		strileSize += arrayFieldSize(ifd.TileByteCounts, bigtiff) - tagSize
	}
	if len(ifd.ExtraSamples) > 0 {
		cnt++
		size += arrayFieldSize(ifd.ExtraSamples, bigtiff)
	}
	if len(ifd.SampleFormat) > 0 {
		cnt++
		size += arrayFieldSize(ifd.SampleFormat, bigtiff)
	}
	if len(ifd.JPEGTables) > 0 {
		cnt++
		size += arrayFieldSize(ifd.JPEGTables, bigtiff)
	}
	if len(ifd.ModelPixelScaleTag) > 0 {
		cnt++
		size += arrayFieldSize(ifd.ModelPixelScaleTag, bigtiff)
	}
	if len(ifd.ModelTiePointTag) > 0 {
		cnt++
		size += arrayFieldSize(ifd.ModelTiePointTag, bigtiff)
	}
	if len(ifd.ModelTransformationTag) > 0 {
		cnt++
		size += arrayFieldSize(ifd.ModelTransformationTag, bigtiff)
	}
	if len(ifd.GeoKeyDirectoryTag) > 0 {
		cnt++
		size += arrayFieldSize(ifd.GeoKeyDirectoryTag, bigtiff)
	}
	if len(ifd.GeoDoubleParamsTag) > 0 {
		cnt++
		size += arrayFieldSize(ifd.GeoDoubleParamsTag, bigtiff)
	}
	if ifd.GeoAsciiParamsTag != "" {
		cnt++
		size += arrayFieldSize(ifd.GeoAsciiParamsTag, bigtiff)
	}
	if ifd.GDALMetaData != "" {
		cnt++
		size += arrayFieldSize(ifd.GDALMetaData, bigtiff)
	}
	if ifd.NoData != "" {
		cnt++
		size += arrayFieldSize(ifd.NoData, bigtiff)
	}
	if len(ifd.LERCParams) > 0 {
		cnt++
		size += arrayFieldSize(ifd.LERCParams, bigtiff)
	}
	if len(ifd.RPCs) > 0 {
		cnt++
		size += arrayFieldSize(ifd.RPCs, bigtiff)
	}
	return cnt, size, strileSize
}

type TagData struct {
	bytes.Buffer
	Offset uint64
}

func (t *TagData) NextOffset() uint64 {
	return t.Offset + uint64(t.Buffer.Len())
}

type COG struct {
	enc     binary.ByteOrder
	ifd     *IFD
	bigtiff bool
}

func New() *COG {
	return &COG{enc: binary.LittleEndian}
}

func (cog *COG) writeHeader(w io.Writer) error {
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
		cog.enc.PutUint64(buf[8:], 16)
		_, err := w.Write(buf[:])
		return err
	} else {
		buf := [8]byte{}
		if cog.enc == binary.LittleEndian {
			copy(buf[0:], []byte("II"))
		} else {
			copy(buf[0:], []byte("MM"))
		}
		cog.enc.PutUint16(buf[2:], 42)
		cog.enc.PutUint32(buf[4:], 8)
		_, err := w.Write(buf[:])
		return err
	}
}

const (
	TByte      = 1
	TAscii     = 2
	TShort     = 3
	TLong      = 4
	TRational  = 5
	TSByte     = 6
	TUndefined = 7
	TSShort    = 8
	TSLong     = 9
	TSRational = 10
	TFloat     = 11
	TDouble    = 12
	TLong8     = 16
	TSLong8    = 17
	TIFD8      = 18
)

func (cog *COG) computeStructure() {
	ifd := cog.ifd
	for ifd != nil {
		ifd.ntags, ifd.tagsSize, ifd.strileSize = ifd.structure(cog.bigtiff)
		//ifd.ntilesx = uint64(math.Ceil(float64(ifd.ImageWidth) / float64(ifd.TileWidth)))
		//ifd.ntilesy = uint64(math.Ceil(float64(ifd.ImageLength) / float64(ifd.TileLength)))
		ifd.ntilesx = (ifd.ImageWidth + uint64(ifd.TileWidth) - 1) / uint64(ifd.TileWidth)
		ifd.ntilesy = (ifd.ImageLength + uint64(ifd.TileLength) - 1) / uint64(ifd.TileLength)
		for _, mifd := range ifd.masks {
			mifd.ntags, mifd.tagsSize, mifd.strileSize = mifd.structure(cog.bigtiff)
			//	mifd.ntilesx = uint64(math.Ceil(float64(mifd.ImageWidth) / float64(mifd.TileWidth)))
			//	mifd.ntilesy = uint64(math.Ceil(float64(mifd.ImageLength) / float64(mifd.TileLength)))
			mifd.ntilesx = (mifd.ImageWidth + uint64(mifd.TileWidth) - 1) / uint64(mifd.TileWidth)
			mifd.ntilesy = (mifd.ImageLength + uint64(mifd.TileLength) - 1) / uint64(mifd.TileLength)
		}
		ifd = ifd.overview
	}
}

func (cog *COG) computeImageryOffsets() error {
	ifd := cog.ifd
	for ifd != nil {
		if cog.bigtiff {
			ifd.NewTileOffsets64 = make([]uint64, len(ifd.OriginalTileOffsets))
			ifd.NewTileOffsets32 = nil
		} else {
			ifd.NewTileOffsets32 = make([]uint32, len(ifd.OriginalTileOffsets))
			ifd.NewTileOffsets64 = nil
		}
		//mifd.NewTileOffsets = mifd.OriginalTileOffsets
		for _, sc := range ifd.masks {
			if cog.bigtiff {
				sc.NewTileOffsets64 = make([]uint64, len(sc.OriginalTileOffsets))
				sc.NewTileOffsets32 = nil
			} else {
				sc.NewTileOffsets32 = make([]uint32, len(sc.OriginalTileOffsets))
				sc.NewTileOffsets64 = nil
			}
			//sc.NewTileOffsets = sc.OriginalTileOffsets
		}
		ifd = ifd.overview
	}
	cog.computeStructure()

	//offset to start of image data
	dataOffset := uint64(16)
	if !cog.bigtiff {
		dataOffset = 8
	}

	ifd = cog.ifd
	for ifd != nil {
		dataOffset += ifd.strileSize + ifd.tagsSize
		for _, sc := range ifd.masks {
			dataOffset += sc.strileSize + sc.tagsSize
		}
		ifd = ifd.overview
	}

	datas := cog.dataInterlacing()
	tiles := datas.Tiles()
	for tile := range tiles {
		tileidx := tile.x + tile.y*tile.ifd.ntilesx
		cnt := uint64(tile.ifd.TileByteCounts[tileidx])
		if cnt > 0 {
			if cog.bigtiff {
				tile.ifd.NewTileOffsets64[tileidx] = dataOffset
			} else {
				if dataOffset > uint64(^uint32(0)) { //^uint32(0) is max uint32
					//rerun with bigtiff support
					cog.bigtiff = true
					return cog.computeImageryOffsets()
				}
				tile.ifd.NewTileOffsets32[tileidx] = uint32(dataOffset)
			}
			dataOffset += uint64(tile.ifd.TileByteCounts[tileidx])
		} else {
			if cog.bigtiff {
				tile.ifd.NewTileOffsets64[tileidx] = 0
			} else {
				tile.ifd.NewTileOffsets32[tileidx] = 0
			}
		}
	}

	return nil
}

func (cog *COG) Write(out io.Writer) error {

	err := cog.computeImageryOffsets()
	if err != nil {
		return err
	}

	//compute start of strile data, and offsets to subIFDs
	//striles are placed after all ifds
	strileData := &TagData{Offset: 16}
	if !cog.bigtiff {
		strileData.Offset = 8
	}

	ifd := cog.ifd
	for ifd != nil {
		strileData.Offset += ifd.tagsSize
		for _, sc := range ifd.masks {
			strileData.Offset += sc.tagsSize
		}
		ifd = ifd.overview
	}

	cog.writeHeader(out)

	ifd = cog.ifd
	off := uint64(16)
	if !cog.bigtiff {
		off = 8
	}
	for ifd != nil {
		nmasks := len(ifd.masks)
		err := cog.writeIFD(out, ifd, off, strileData, nmasks > 0 || ifd.overview != nil)
		if err != nil {
			return fmt.Errorf("write ifd: %w", err)
		}
		off += ifd.tagsSize
		for i, si := range ifd.masks {
			err := cog.writeIFD(out, si, off, strileData, i != nmasks-1 || ifd.overview != nil)
			if err != nil {
				return fmt.Errorf("write ifd: %w", err)
			}
			off += si.tagsSize
		}
		ifd = ifd.overview
	}

	_, err = out.Write(strileData.Bytes())
	if err != nil {
		return fmt.Errorf("write strile pointers: %w", err)
	}

	datas := cog.dataInterlacing()
	tiles := datas.Tiles()
	buf := &bytes.Buffer{}
	for tile := range tiles {
		buf.Reset()
		idx := tile.x + tile.y*tile.ifd.ntilesx
		if tile.ifd.TileByteCounts[idx] > 0 {
			_, err := tile.ifd.r.Seek(int64(tile.ifd.OriginalTileOffsets[idx]), io.SeekStart)
			if err != nil {
				return fmt.Errorf("seek to %d: %w", tile.ifd.OriginalTileOffsets[idx], err)
			}
			_, err = io.CopyN(out, tile.ifd.r, int64(tile.ifd.TileByteCounts[idx]))
			if err != nil {
				return fmt.Errorf("copy %d from %d: %w",
					tile.ifd.TileByteCounts[idx], tile.ifd.OriginalTileOffsets[idx], err)
			}
		}
	}

	return err
}

func (cog *COG) writeIFD(w io.Writer, ifd *IFD, offset uint64, striledata *TagData, next bool) error {

	nextOff := uint64(0)
	if next {
		nextOff = offset + ifd.tagsSize
	}
	var err error
	// Make space for "pointer area" containing IFD entry data
	// longer than 4 bytes.
	overflow := &TagData{
		Offset: offset + 8 + 20*ifd.ntags + 8,
	}
	if !cog.bigtiff {
		overflow.Offset = offset + 2 + 12*ifd.ntags + 4
	}

	if cog.bigtiff {
		err = binary.Write(w, cog.enc, ifd.ntags)
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
	if ifd.ImageLength > 0 {
		err := cog.writeField(w, 257, uint32(ifd.ImageLength))
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
	if ifd.TileLength > 0 {
		err := cog.writeField(w, 323, ifd.TileLength)
		if err != nil {
			panic(err)
		}
	}

	//TileOffsets               []uint64 `tiff:"field,tag=324"`
	if len(ifd.NewTileOffsets32) > 0 {
		err := cog.writeArray(w, 324, ifd.NewTileOffsets32, striledata)
		if err != nil {
			panic(err)
		}
	} else {
		err := cog.writeArray(w, 324, ifd.NewTileOffsets64, striledata)
		if err != nil {
			panic(err)
		}
	}

	//TileByteCounts            []uint32 `tiff:"field,tag=325"`
	if len(ifd.TileByteCounts) > 0 {
		err := cog.writeArray(w, 325, ifd.TileByteCounts, striledata)
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
		err = binary.Write(w, cog.enc, nextOff)
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
	ifd  *IFD
	x, y uint64
}

type datas [][]*IFD

func (cog *COG) dataInterlacing() datas {
	//count overviews
	ifd := cog.ifd
	count := 0
	for ifd != nil {
		count++
		ifd = ifd.overview
	}
	ret := make([][]*IFD, count)
	ifd = cog.ifd
	for idx := count - 1; idx >= 0; idx-- {
		ret[idx] = append(ret[idx], ifd)
		for _, mi := range ifd.masks {
			ret[idx] = append(ret[idx], mi)
		}
		ifd = ifd.overview
	}
	return ret
}

func (d datas) Tiles() chan tile {
	ch := make(chan tile)
	go func() {
		defer close(ch)

		for _, ovr := range d {
			for y := uint64(0); y < ovr[0].ntilesy; y++ {
				for x := uint64(0); x < ovr[0].ntilesx; x++ {
					for _, ifd := range ovr {
						ch <- tile{
							ifd: ifd,
							x:   x,
							y:   y,
						}
					}
				}
			}
		}

	}()
	return ch
}
