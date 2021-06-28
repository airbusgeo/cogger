package cogger

import (
	"fmt"
	"io"
	"sort"

	"github.com/google/tiff"
)

func loadMultipleTIFFs(tifs []tiff.TIFF) ([]*ifd, error) {
	ifds := make([]*ifd, 0)
	for it, tif := range tifs {
		tifds := tif.IFDs()
		for i := range tifds {
			ifd, err := loadIFD(tif.R(), tifds[i])
			if err != nil {
				return nil, err
			}
			if it != 0 {
				//check that the additional files are smaller than the first, i.e. that they represent an overview
				if ifd.ImageLength >= ifds[0].ImageLength || ifd.ImageWidth >= ifds[0].ImageWidth {
					return nil, fmt.Errorf("provided tiff %d size %dx%d is larger than first tiff size %dx%d. when using multiple files, the subsequent ones must be overviews of the first one",
						it, ifd.ImageWidth, ifd.ImageLength, ifds[0].ImageWidth, ifds[0].ImageLength)
				}
				//force to overview
				ifd.SubfileType |= subfileTypeReducedImage
			}
			ifds = append(ifds, ifd)
		}
	}
	return ifds, nil
}

func loadSingleTIFF(tif tiff.TIFF) ([]*ifd, error) {
	tifds := tif.IFDs()
	ifds := make([]*ifd, len(tifds))
	var err error
	for i := range tifds {
		ifds[i], err = loadIFD(tif.R(), tifds[i])
		if err != nil {
			return nil, err
		}
	}
	return ifds, nil
}

func loadIFD(r tiff.BReader, tifd tiff.IFD) (*ifd, error) {
	ifd := &ifd{r: r}
	err := tiff.UnmarshalIFD(tifd, ifd)
	if err != nil {
		return nil, err
	}
	if len(ifd.TempTileByteCounts) > 0 {
		ifd.TileByteCounts = make([]uint32, len(ifd.TempTileByteCounts))
		for i := range ifd.TempTileByteCounts {
			ifd.TileByteCounts[i] = uint32(ifd.TempTileByteCounts[i])
		}
		ifd.TempTileByteCounts = nil //reclaim mem
	}
	return ifd, nil
}

// Rewrite reshuffles the tiff bytes provided as readers into a COG output
// to out
func Rewrite(out io.Writer, readers ...tiff.ReadAtReadSeeker) error {
	tiffs := []tiff.TIFF{}
	if len(readers) == 0 {
		return fmt.Errorf("missing readers")
	}
	for i, r := range readers {
		tif, err := tiff.Parse(r, nil, nil)
		if err != nil {
			return fmt.Errorf("parse tiff %d: %w", i, err)
		}
		tiffs = append(tiffs, tif)
	}
	err := sanityCheck(tiffs)
	if err != nil {
		return fmt.Errorf("consistency check: %w", err)
	}
	var ifds []*ifd
	if len(tiffs) > 1 {
		ifds, err = loadMultipleTIFFs(tiffs)
		if err != nil {
			return fmt.Errorf("load: %w", err)
		}
	} else {
		ifds, err = loadSingleTIFF(tiffs[0])
		if err != nil {
			return fmt.Errorf("load: %w", err)
		}
	}
	sort.Slice(ifds, func(i, j int) bool {
		//return in order: fullres, fullresmasks, ovr1, ovr1masks, ovr2, ....
		if ifds[i].ImageLength != ifds[j].ImageLength {
			return ifds[i].ImageLength > ifds[j].ImageLength
		}
		return ifds[i].SubfileType < ifds[j].SubfileType
	})
	if ifds[0].SubfileType != 0 {
		return fmt.Errorf("failed sort: first px=%d type=%d", ifds[0].ImageLength, ifds[0].SubfileType)
	}
	cog := new()
	cog.ifd = ifds[0]
	curOvr := cog.ifd
	l := curOvr.ImageLength
	for _, ci := range ifds[1:] {
		if ci.ImageLength == l {
			curOvr.AddMask(ci)
		} else {
			curOvr.AddOverview(ci)
			curOvr = ci
			l = curOvr.ImageLength
		}
	}

	err = cog.write(out)
	if err != nil {
		return fmt.Errorf("mucog write: %w", err)
	}
	return nil
}

func sanityCheck(tiffs []tiff.TIFF) error {
	if len(tiffs) == 0 {
		return fmt.Errorf("no tiffs")
	}
	order := tiffs[0].Order()
	if order != "MM" && order != "II" {
		return fmt.Errorf("unknown byte order")
	}
	for it, tif := range tiffs {
		if tif.Order() != order {
			return fmt.Errorf("inconsistent byte order")
		}
		for ii, ifd := range tif.IFDs() {
			err := sanityCheckIFD(ifd)
			if err != nil {
				return fmt.Errorf("tif %d ifd %d: %w", it, ii, err)
			}
		}
	}
	return nil
}

func sanityCheckIFD(ifd tiff.IFD) error {
	to := ifd.GetField(324)
	tl := ifd.GetField(325)
	if to == nil || tl == nil {
		return fmt.Errorf("no tiles")
	}
	if to.Count() != tl.Count() {
		return fmt.Errorf("inconsistent tile off/len count")
	}
	so := ifd.GetField(272)
	sl := ifd.GetField(279)
	if so != nil || sl != nil {
		return fmt.Errorf("tif has strips")
	}
	return nil
}
