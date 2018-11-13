#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import json

from kafka import KafkaProducer
from metrics_pb2 import Metrics


producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'metrics_pb'

for row in iter(sys.stdin):
    d = json.loads(row)
    metrics = Metrics()
    for k, v in d.items():
        setattr(metrics, k, v)
    pb = metrics.SerializeToString()
    producer.send(topic, pb)
