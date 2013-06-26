#!/bin/bash
(
cd druid 
bin/start-broker.sh >> ../logs/druid-broker.log 2>&1 &
)
