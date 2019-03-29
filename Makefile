PROJECT_NAME = druid

PROJECT_ROOT = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_BIN = $(PROJECT_NAME)

PROJECT_REV = $(shell git rev-parse HEAD)
PROJECT_IMAGE = registry.build.lqm.io/$(PROJECT_NAME):$(PROJECT_REV)

PROJECT_ARTIFACT = /tmp/$(PROJECT_NAME)_$(PROJECT_REV)

.PHONY: all build image artifact publish-image publish-artifact

default: build

all: build

build:
	mvn clean install -DskipTests -Pdist,bundle-contrib-exts --quiet

image:
	docker build -t $(PROJECT_IMAGE) .

artifact: image
	$(eval CID := $(shell docker create $(PROJECT_IMAGE)))
	docker cp $(CID):/opt/druid/distribution $(PROJECT_ARTIFACT)
	docker rm $(CID)
	tar -czvf $(PROJECT_ARTIFACT).tar.gz -C $(PROJECT_ARTIFACT) .

publish-image: image
	docker push $(PROJECT_IMAGE)

publish-artifact: artifact
	gsutil cp $(PROJECT_ARTIFACT).tar.gz gs://lqm-artifact-storage/$(PROJECT_NAME)/$(PROJECT_REV)
