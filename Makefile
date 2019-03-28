PROJECT_NAME = druid

PROJECT_ROOT = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_FULL_NAME = $(PROJECT_NAMESPACE)/$(PROJECT_NAME)
PROJECT_BIN = $(PROJECT_NAME)

PROJECT_REV = $(shell git rev-parse HEAD)

.PHONY: all build setup publish-artifact

default: build

all: build

setup:
	rm -f ~/.m2/settings.xml

build: setup
	mvn clean install -DskipTests -Pdist,bundle-contrib-exts --quiet

publish-artifact: build
	gsutil cp distribution/target/*.tar.gz gs://lqm-artifact-storage/$(PROJECT_NAME)/$(PROJECT_REV)