#!/usr/bin/env python

import argparse
import json
import random
import sys
from datetime import datetime

def main():
  parser = argparse.ArgumentParser(description='Generate example page request latency metrics.')
  parser.add_argument('--count', '-c', type=int, default=25, help='Number of events to generate (negative for unlimited)')
  args = parser.parse_args()

  count = 0
  while args.count < 0 or count < args.count:
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    r = random.randint(1, 4)
    if r == 1 or r == 2:
      page = '/'
    elif r == 3:
      page = '/list'
    else:
      page = '/get/' + str(random.randint(1, 99))

    server = 'www' + str(random.randint(1, 5)) + '.example.com'

    latency = max(1, random.gauss(80, 40))

    print(json.dumps({
      'timestamp': timestamp,
      'metricType': 'request/latency',
      'value': int(latency),

      # Additional dimensions
      'page': page,
      'server': server,
      'http_method': 'GET',
      'http_code': '200',
      'unit': 'milliseconds'
    }))

    count += 1

try:
  main()
except KeyboardInterrupt:
  sys.exit(1)
