#!/bin/bash
ps -eaf | grep ComputeMain | grep -v grep | awk '{print $2}' | xargs kill
