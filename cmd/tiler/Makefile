IMAGE:=eu.gcr.io/d-dzm-deepzoom/cogger/cogger:20220817

build:
	@go mod vendor && mv ../../vendor ../../vendor.docker
	-docker build -t $(IMAGE) --build-arg COGGER_IMAGE=$(IMAGE) -f Dockerfile ../..
	@rm -rf ../../vendor.docker

push:
	docker push $(IMAGE)

build-local:
	go build -ldflags "-X main.defaultImage=$(IMAGE)" .
