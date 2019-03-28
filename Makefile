PROJECT_NAME = druid

PROJECT_ROOT = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_BIN = $(PROJECT_NAME)

PROJECT_REV = $(shell git rev-parse HEAD)
PROJECT_IMAGE = liquidm/$(PROJECT_NAME):$(PROJECT_REV)

.PHONY: all build setup publish-artifact

default: build

all: build

build:
	mvn clean install -DskipTests -Pdist,bundle-contrib-exts --quiet

image:
	docker build -t $(PROJECT_IMAGE) .
	docker tag $(PROJECT_IMAGE) registry.build.lqm.io/$(PROJECT_NAME):$(PROJECT_REV)

artifact: image
	$(eval CID := $(shell docker create $(PROJECT_IMAGE)))
	docker cp $(CID):/opt/druid/distribution/*.tar.gz /tmp/$(PROJECT_NAME)/$(PROJECT_NAME)_$(PROJECT_REV).tar.gz
	docker rm $(CID)

publish-image: image
	docker push registry.build.lqm.io/$(PROJECT_NAME):$(PROJECT_REV)

publish-artifact: artifact
	gsutil cp /tmp/$(PROJECT_NAME)_$(PROJECT_REV).tar.gz gs://lqm-artifact-storage/$(PROJECT_NAME)/$(PROJECT_REV)