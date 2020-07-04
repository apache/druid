.PHONY: build

PWD = $(shell pwd)

SRC_DIR = $(PWD)/..
BINDIR = $(SRC_DIR)/../builds/druid/bin
DATADIR = $(SRC_DIR)/../builds/druid/var/

build:
	$(SRC_DIR)/debugging/build_druid.sh
build_pi:
	${SRC_DIR}/debugging/build_druid.sh -pi
run:
	$(SRC_DIR)/debugging/run.sh &
cluster:
	$(SRC_DIR)/debugging/run_cluster.sh &
clean:
	rm -r $(DATADIR)
subspec:
	$(SRC_DIR)/submit_spec.sh

stop:
	$(BINDIR)/service --down -d $(DATADIR)

restart: stop clean timeout 5 run
rebuild: stop clean build


