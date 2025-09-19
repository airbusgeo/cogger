package cogger

import (
	"fmt"
	"io"
	"sort"

	"github.com/google/tiff"
)

func loadTIFFs(tifRs []tiff.ReadAtReadSeeker) ([]*IFD, error) {
	var order string
	ifds := make([]*IFD, 0)
	for ii, tifr := range tifRs {
		tif, err := tiff.Parse(tifr, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("parse tiff %d: %w", ii, err)
		}
		if ii == 0 {
			order = tif.Order()
		} else if tif.Order() != order {
			return nil, fmt.Errorf("inconsistent tif byte ordering")
		}
		tifds := tif.IFDs()
		for i := range tifds {
			ifd, err := loadIFD(tif.R(), tifds[i])
			if err != nil {
				return nil, fmt.Errorf("load ifd %d of tif %d: %w", i, ii, err)
			}
			ifds = append(ifds, ifd)
		}
	}
	return ifds, nil
}

func loadIFD(r tiff.ReadAtReadSeeker, tifd tiff.IFD) (*IFD, error) {
	ifd := &IFD{}
	err := tiff.UnmarshalIFD(tifd, ifd)
	if err != nil {
		return nil, err
	}
	if len(ifd.TileByteCounts) == 0 || len(ifd.TileByteCounts) != len(ifd.TileOffsets) {
		return nil, fmt.Errorf("ifd is not tiled")
	}
	ifd.LoadTile = func(idx int, data []byte) error {
		if idx >= len(ifd.TileByteCounts) || len(data) != int(ifd.TileByteCounts[idx]) {
			panic("bug")
		}
		_, err = r.ReadAt(data, int64(ifd.TileOffsets[idx]))
		return err
	}
	return ifd, nil
}

// Rewrite reshuffles the tiff bytes provided as readers into a COG output
// to out
//
// Use Config.Rewrite in order to use non-default options
func Rewrite(out io.Writer, readers ...tiff.ReadAtReadSeeker) error {
	return DefaultConfig().Rewrite(out, readers...)
}

func (cfg Config) Rewrite(out io.Writer, readers ...tiff.ReadAtReadSeeker) error {
	return cfg.RewriteSplitted(out, out, readers...)
}

func (cfg Config) RewriteSplitted(headerOut, dataOut io.Writer, readers ...tiff.ReadAtReadSeeker) error {
	if len(readers) == 0 {
		return fmt.Errorf("missing readers")
	}
	ifds, err := loadTIFFs(readers)
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	sort.Slice(ifds, func(i, j int) bool {
		//return in order: fullres, fullresmasks, ovr1, ovr1masks, ovr2, ....
		if ifds[i].ImageHeight*ifds[i].ImageWidth != ifds[j].ImageHeight*ifds[j].ImageWidth {
			return ifds[i].ImageHeight*ifds[i].ImageWidth > ifds[j].ImageHeight*ifds[j].ImageWidth
		}
		return ifds[i].SubfileType < ifds[j].SubfileType
	})
	if ifds[0].SubfileType != 0 {
		return fmt.Errorf("failed sort: first px=%dx%d type=%d", ifds[0].ImageWidth, ifds[0].ImageHeight, ifds[0].SubfileType)
	}
	curOvr := ifds[0]
	w, h := curOvr.ImageWidth, curOvr.ImageHeight
	for _, ci := range ifds[1:] {
		if ci.ImageHeight == h && ci.ImageWidth == w {
			err = curOvr.AddMask(ci)
		} else {
			err = ifds[0].AddOverview(ci)
			curOvr = ci
			w, h = curOvr.ImageWidth, curOvr.ImageHeight
		}
		if err != nil {
			return fmt.Errorf("failed to add overview/mask %dx%dx%d: %w",
				ci.ImageWidth, ci.ImageHeight, ci.SamplesPerPixel, err)
		}
	}

	err = cfg.RewriteIFDTreeSplitted(ifds[0], headerOut, dataOut)
	if err != nil {
		return fmt.Errorf("mucog write: %w", err)
	}
	return nil
}
