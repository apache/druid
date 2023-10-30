#!/bin/sh
TAG=${1?"tag must be specified"}
mvn clean package -T 12 -DskipTests -Pdist,bundle-contrib-exts && DOCKER_BUILDKIT=1 docker build -t nakama888/druid:${TAG} -f distribution/docker/Dockerfile --build-arg BUILD_FROM_SOURCE=false --platform linux/amd64 .  && docker push nakama888/druid:${TAG}
