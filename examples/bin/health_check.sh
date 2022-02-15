#! /bin/bash
set -e

curl -f -s http://localhost:9090/selfDiscovered | grep -q "true"
