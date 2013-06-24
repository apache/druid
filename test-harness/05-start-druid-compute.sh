#!/bin/bash
(
cd druid 
bin/start-compute.sh >> ../logs/druid-compute.log 2>&1 &
)
