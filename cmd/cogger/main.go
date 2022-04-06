package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/airbusgeo/cogger"

	"github.com/google/tiff"
	_ "github.com/google/tiff/bigtiff"
)

func main() {
	ctx := context.Background()
	err := run(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	outfile := flag.String("output", "out.tif", "destination file")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options] file.tif [overview.tif...]\nOptions:\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		return fmt.Errorf("")
	}

	totalSize := int64(0)
	readers := make([]tiff.ReadAtReadSeeker, len(args))
	for i, input := range args {
		topFile, err := os.Open(input)
		if err != nil {
			return fmt.Errorf("open %s: %w", args[0], err)
		}
		defer topFile.Close()
		st, err := topFile.Stat()
		if err != nil {
			return fmt.Errorf("stat %s: %w", args[0], err)
		}
		totalSize += st.Size()
		readers[i] = topFile
	}
	out, err := os.Create(*outfile)
	if err != nil {
		return fmt.Errorf("create %s: %w", *outfile, err)
	}
	err = cogger.DefaultConfig().Rewrite(out, readers...)
	if err != nil {
		return fmt.Errorf("mucog write: %w", err)
	}
	err = out.Close()
	if err != nil {
		return fmt.Errorf("close %s: %w", *outfile, err)
	}
	return nil
}
