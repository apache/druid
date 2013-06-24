#!/bin/bash
ps -eaf | grep MasterMain | grep -v grep | awk '{print $2}' | xargs kill
