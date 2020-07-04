.PHONY: build

PWD = $(shell pwd)

SRC_DIR = $(PWD)/..
BINDIR = $(SRC_DIR)/../builds/druid/bin
DATADIR = $(SRC_DIR)/../builds/druid/var/
DEBUG_DIR = $(PWD)

build:
	$(DEBUG_DIR)/build_druid.sh
build_pi:
	$(DEBUG_DIR)/build_druid.sh -pi
run:
	$(DEBUG_DIR)/run.sh &
push:
	/usr/local/bin/python3 $(DEBUG_DIR)/send_to_pubsub.py
cluster:
	$(DEBUG_DIR)/run_cluster.sh &
clean:
	rm -r $(DATADIR)
subspec:
	$(SRC_DIR)/submit_spec.sh
stop:
	$(BINDIR)/service --down -d $(DATADIR)

restart: stop clean timeout 5 run
rebuild: stop clean build


