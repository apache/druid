#!/bin/bash
ps -eaf | grep kafka\.Kafka | grep -v grep | awk '{print $2}' | xargs kill
