# cogger

Cogger rewrites a tiled geotiff to a COG Geotiff. 
It's just a byte shuffler so it's super fast!
 
## Install

```
git clone git@github.com:airbusgeo/cogger.git
cd cogger
go run cmd/cogger/main.go
```

## Usage

```
Usage: main [options] file.tif [overview.tif...]
Options:
  -output string
        destination file (default "out.tif")
```
