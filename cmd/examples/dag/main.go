package main

import (
	"fmt"

	"github.com/airbusgeo/cogger"
)

func main() {
	stripper, _ := cogger.NewStripper(16353, 16353, cogger.InternalTileSize(256, 256))
	pyramid := stripper.Pyramid()
	dag := pyramid.DAG()

	for z, znodes := range dag {
		fmt.Printf("level %d size %dx%d\n", z, pyramid[z].Width, pyramid[z].Height)
		for s, node := range znodes {
			if z == 0 {
				if len(node.Parents) != 0 {
					panic("level 0 strips cannnot have any parents")
				}
				curstrip := pyramid[z].Strips[s]
				fmt.Printf("strip %d (height=%d) of full res image correspond to the pixels from line %d to %d of the source image\n",
					s, curstrip.Height,
					int(curstrip.SrcTopLeftY), int(curstrip.SrcTopLeftY+curstrip.SrcHeight))
			}
			if z == 1 {
				if len(node.Parents) == 0 {
					panic("overview strips must have at least one parents")
				}
				curstrip := pyramid[z].Strips[s]
				fmt.Printf("strip %d (height=%d) of overview 1 correspond to the pixels from line %f to %f of the source image\n",
					s, curstrip.Height, curstrip.SrcTopLeftY, curstrip.SrcTopLeftY+curstrip.SrcHeight)
				pids := fmt.Sprintf("%d", node.Parents[0])
				for i := 1; i < len(node.Parents); i++ {
					pids += fmt.Sprintf(",%d", node.Parents[i])
				}
				fmt.Printf("it can be extracted from lines %f to %f of strips %s of the fullres\n",
					curstrip.SrcTopLeftY-float64(node.ParentOffet),
					curstrip.SrcTopLeftY+curstrip.SrcHeight-float64(node.ParentOffet),
					pids)
			}
			if z > 1 {
				if len(node.Parents) == 0 {
					panic("overview strips must have at least one parents")
				}
				curstrip := pyramid[z].Strips[s]
				fmt.Printf("strip %d (height=%d) of overview %d correspond to the pixels from line %f to %f of the previous overview\n",
					s, curstrip.Height, z, curstrip.SrcTopLeftY, curstrip.SrcTopLeftY+curstrip.SrcHeight)
				pids := fmt.Sprintf("%d", node.Parents[0])
				for i := 1; i < len(node.Parents); i++ {
					pids += fmt.Sprintf(",%d", node.Parents[i])
				}
				fmt.Printf("it can be extracted from lines %f to %f of strips %s of the previous overview\n",
					curstrip.SrcTopLeftY-float64(node.ParentOffet),
					curstrip.SrcTopLeftY+curstrip.SrcHeight-float64(node.ParentOffet),
					pids)
			}
			/*
				src:="source image"
				dst:="full-res destination"
				if strip.Level==1{
					src="full-res destination"
					dst="overview 1"
				} else if strip.Level>1 {
					src=fmt.Sprintf("overview %d",strip.Level-1)
					dst=fmt.Sprintf("overview %d",strip.Level)
				}
				fmt.Println("strip %d of %s)

			*/
			fmt.Println("---")
		}
		fmt.Println("###")
	}
}
