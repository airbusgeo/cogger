# Cogger
[![Go Reference](https://pkg.go.dev/badge/github.com/airbusgeo/cogger.svg)](https://pkg.go.dev/github.com/airbusgeo/cogger)
[![License](https://img.shields.io/github/license/airbusgeo/cogger.svg)](https://github.com/airbusgeo/cogger/blob/main/LICENSE)

Cogger is a standalone binary and a golang library that reads an internally tiled geotiff (optionally with overviews and masks)
and rewrites it as a [Cloud Optimized Geotiff (COG)](https://www.cogeo.org). This process being a reshuffling of the original
geotiff's bytes, it should run as fast as the underlying disk or network i/o.

Cogger does not do any pixel manipulation on the provided image, it is up to you to provide an input geotiff which can be suitably
transformed to a COG, namely:

* it must be internally tiled
* it should be compressed with one of the standard supported tiff compression mechanisms
* it should contain overviews

## Installation

### Binaries

We publish the `cogger` binaries for the major platforms/cpus, which you can grab from our [releases](https://github.com/airbusgeo/cogger/releases)

### From source

The library version of `cogger` can be used in go code with:
```go
import "github.com/airbusgeo/cogger"
```

The cogger binary can be installed directly to your `$GOPATH/bin` with:
```bash
go install github.com/airbusgeo/cogger/cmd/cogger@latest
```

## Usage

### Binary

```bash
gdal_translate -of GTIFF -co BIGTIFF=YES -co TILED=YES -co COMPRESS=ZSTD -co NUM_THREADS=4 input.file geotif.tif
gdaladdo --config GDAL_NUM_THREADS 4 geotif.tif 2 4 8 16 32
cogger -output mycog.tif geotif.tif
```

### Library

The cogger API consists of a single function:
```go
func Rewrite(out io.Writer, readers ...tiff.ReadAtReadSeeker) error
```

with the reader allowing random read access to the input file, i.e. implementing
```go
Read(buf []byte) (int,error)
ReatAt(buf []byte, offset int64) (int,error)
Seek(off int64, whence int) (int64,error)
```

The writer is a plain `io.Writer` which means that the output cog can be directly
streamed to http/cloud storage without having to be stored in an intermediate file.

For an full example of library usage, see the `main.go` file in `cmd/cogger`.

### Advanced

Cogger is able to assemble a single COG from a main tif file and overviews that have been computed
in distinct files. This may be useful as `gdaladdo` is missing some features to fine tune the options
of each individual overview.

```bash
gdal_translate -of GTIFF -co BIGTIFF=YES -co TILED=YES -co COMPRESS=ZSTD -co NUM_THREADS=4 input.file geotif.tif
# compute first overview
gdal_translate -of GTIFF -outsize 50% 50% -co BLOCKXSIZE=128 -co TILED=YES -co COMPRESS=ZSTD -co NUM_THREADS=4  geotif.tif ovr.tif.1
# compute second overview
gdal_translate -of GTIFF -outsize 50% 50% -co BLOCKXSIZE=256 -co TILED=YES -co COMPRESS=ZSTD -co NUM_THREADS=4  ovr.tif.1 ovr.tif.2
# compute third overview
gdal_translate -of GTIFF -outsize 50% 50% -co BLOCKXSIZE=512 -co TILED=YES -co COMPRESS=ZSTD -co NUM_THREADS=4  ovr.tif.2 ovr.tif.3
# compute COG from geotif.tif and ovr.tif.* overviews
cogger -output mycog.tif geotif.tif ovr.tif.1 ovr.tif.2 ovr.tif.3
```

## Contributing

Contributions are welcome. Please read the [contribution guidelines](CONTRIBUTING.md)
before submitting fixes or enhancements.

## Licensing
Cogger is licensed under the Apache License, Version 2.0. See
[LICENSE](https://github.com/airbusgeo/cogger/blob/main/LICENSE) for the full
license text.


