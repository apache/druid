#!/bin/bash
ps -eaf | grep QuorumPeerMain | grep -v grep | awk '{print $2}' | xargs kill
