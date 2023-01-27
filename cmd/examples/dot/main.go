package main

import (
	"flag"
	"fmt"

	"github.com/airbusgeo/cogger"
)

func main() {
	w := 16353
	h := 16353
	ss := 8192 * 8192
	flag.IntVar(&w, "w", 16353, "input width")
	flag.IntVar(&h, "h", 16353, "input height")
	flag.IntVar(&ss, "ss", 8192*8192, "strip size")
	flag.Parse()
	stripper, _ := cogger.NewStripper(w, h, cogger.InternalTileSize(256, 256),
		cogger.TargetPixelCount(ss))
	pyramid := stripper.Pyramid()
	dag := pyramid.DAG()
	stripname := func(z, s int) string {
		return fmt.Sprintf("%d-%d.tif", z, s)
	}

	fmt.Println("digraph cogger {")

	for z, znodes := range dag {
		for s, node := range znodes {
			fmt.Println("\"" + stripname(z, s) + "\"")
			for _, p := range node.Parents {
				fmt.Println("\"" + stripname(z-1, p) + "\" -> \"" + stripname(z, s) + "\";")
			}
		}
	}
	fmt.Println("}")
}
