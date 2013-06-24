#!/bin/bash
ps -eaf | grep RealtimeMain | grep -v grep | awk '{print $2}' | xargs kill
