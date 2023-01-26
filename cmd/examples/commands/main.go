package main

import (
	"fmt"

	"github.com/airbusgeo/cogger"
)

func main() {
	stripper, _ := cogger.NewStripper(16353, 16353, cogger.InternalTileSize(256, 256))
	pyramid := stripper.Pyramid()
	dag := pyramid.DAG()
	stripname := func(z, s int) string {
		return fmt.Sprintf("strip-%d-%d.tif", z, s)
	}

	for z, znodes := range dag {
		for s, node := range znodes {
			curstrip := pyramid[z].Strips[s]
			srcfile := "input.tif"
			dstfile := stripname(z, s)
			if z != 0 {
				if len(node.Parents) > 1 {
					vrtfile := fmt.Sprintf("parentof-%d-%d.vrt", z, s)
					cmd := "gdalbuildvrt " + vrtfile
					for _, p := range node.Parents {
						cmd += " " + stripname(z-1, p)
					}
					fmt.Println(cmd)
					srcfile = vrtfile
				} else {
					srcfile = stripname(z-1, node.Parents[0])
				}
			}
			cmd := fmt.Sprintf("gdal_translate -co TILED=YES -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 -co COMPRESS=LZW %s %s",
				srcfile, dstfile)
			if z != 0 {
				cmd += " -r bilinear"
			}
			cmd += fmt.Sprintf(" -outsize %d %d -srcwin %f %f %f %f",
				curstrip.Width, curstrip.Height,
				curstrip.SrcTopLeftX,
				curstrip.SrcTopLeftY-float64(node.ParentOffet),
				curstrip.SrcWidth, curstrip.SrcHeight,
			)
			fmt.Println(cmd)
		}
		fmt.Println("###")
	}
	fmt.Println("plus command to assemble all strip*.tif files")
}
