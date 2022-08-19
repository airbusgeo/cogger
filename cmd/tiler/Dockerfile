FROM golang:bullseye AS builder
ARG COGGER_IMAGE

RUN apt update && apt -y install \
    build-essential \
    cmake \
    curl \
    libbrotli-dev \
    libgflags-dev \
    libjpeg-dev \
    libopenjp2-7-dev \
    libpng-dev \
    libsqlite3-dev \
    libwebp-dev \
    libzstd-dev \
    libproj-dev \
    pkgconf \
    sqlite3 \
    unzip

WORKDIR /build/jxl
#main from 10/05/2022
ARG JXLVERSION=ff65c387ac98c7f597ed7e954c28fb32065bf553
RUN curl -sL https://github.com/libjxl/libjxl/archive/$JXLVERSION.tar.gz -o jxl.tgz &&\
        tar xzf jxl.tgz &&\
        cd libjxl-$JXLVERSION &&\
        ./deps.sh &&\
        mkdir build && cd build &&\
        cmake .. -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release &&\
        make -j8 &&\
        make install &&\
    cd /build && rm -rf jxl
    

WORKDIR /build/gdal
ARG GDALVERSION=release/3.5
RUN curl -sL https://github.com/OSGeo/gdal/archive/$GDALVERSION.tar.gz -o gdal.tar.gz &&\
	tar  xzf gdal.tar.gz --strip-components 1 &&\
    mkdir build && cd build &&\
    cmake .. \
    -DOGR_BUILD_OPTIONAL_DRIVERS=OFF \
    -DGDAL_ENABLE_DRIVER_DIMAP=ON \
    -DGDAL_ENABLE_DRIVER_JP2OPENJPEG=ON  \
    -DGDAL_ENABLE_DRIVER_WEBP=ON \
    -DGDAL_ENABLE_DRIVER_JPEG=ON \
    -DGDAL_ENABLE_DRIVER_PNG=ON \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=OFF \
    -DGDAL_USE_CURL=OFF \
    -DGDAL_USE_SQLITE3=OFF \
    -DGDAL_ENABLE_DRIVER_ISO8211=OFF \
    -DGDAL_BUILD_OPTIONAL_DRIVERS=OFF \
    -DGDAL_USE_JXL=ON \
    -DGDAL_USE_KDU=OFF \
    -DGDAL_USE_TIFF_INTERNAL=ON \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_PYTHON_BINDINGS=OFF \
    -DENABLE_GNM=OFF\
    -DGDAL_USE_XERCESC=OFF &&\
	make -j8 && \
	make install &&\
	ldconfig /usr/local/lib &&\
    cd /build && rm -rf gdal

WORKDIR /cogger
COPY go.* /cogger/
COPY vendor.docker vendor
COPY go.* /cogger/
RUN cd vendor && go build ./... && cd ..
COPY *.go /cogger/
COPY cmd/tiler/*.go /cogger/cmd/tiler/
WORKDIR /cogger/cmd/tiler
RUN go build -ldflags "-X main.defaultImage=$COGGER_IMAGE" -o tiler .

FROM debian:bullseye
RUN apt update && apt install -y \
    libbrotli1 \
    libjpeg62-turbo \
    libopenjp2-7 \
    libpng16-16 \
    libproj19 \
    libsqlite3-0 \
    libwebp6 \
    libzstd1 &&\
    apt clean &&\
    rm -rf /var/lib/apt/lists/*


COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /usr/local/lib/*.so /usr/local/lib/
COPY --from=builder /usr/local/share/* /usr/local/share/
RUN ldconfig
COPY --from=builder /cogger/cmd/tiler/tiler /usr/local/bin/tiler

