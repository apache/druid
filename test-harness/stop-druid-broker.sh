#!/bin/bash
ps -eaf | grep BrokerMain | grep -v grep | awk '{print $2}' | xargs kill
