#!/usr/bin/env python

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
