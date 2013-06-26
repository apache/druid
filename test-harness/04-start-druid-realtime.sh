#!/bin/bash
(
cd druid 
bin/start-realtime.sh >> ../logs/druid-realtime.log 2>&1 &
)
