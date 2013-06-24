#!/bin/bash
ps -eaf | grep conjure | grep -v grep | awk '{print $2}' | xargs kill
