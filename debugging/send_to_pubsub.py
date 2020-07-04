#!/usr/bin/env python3
import os
import time
import random
import json
import sys
from google.cloud import pubsub_v1

def publish(publisher, topic, message):
    data = message.encode('utf-8')
    return publisher.publish(topic_path, data=data)

def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic_name, message_future.exception()))
    else:
        print(message_future.result())

if __name__ == '__main__':
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        print("WARNING: You need to have 'GOOGLE_APPLICATION_CREDENTIALS' set in your environment")
        sys.exit(-1)
    if not os.getenv("DRUID_GCP_PROJECT_ID"):
        print("WARNING: You need to have 'DRUID_GCP_PROJECT_ID' set in your environment")
        sys.exit(-1)
    if not os.getenv("DRUID_PUBSUB_TOPIC"):
        print("WARNING: You need to have 'DRUID_PUBSUB_TOPIC' set in your environment")
        sys.exit(-1)

    PROJECT_ID, TOPIC = os.getenv("DRUID_GCP_PROJECT_ID"), os.getenv("DRUID_PUBSUB_TOPIC")
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC)

    for _ in range(10):
        msg = json.dumps({"d":"d1", "v":10, "u": "uuid1", "ts": 1579022621715})
        print('pushing {}'.format(msg))
        message_future = publish(publisher, topic_path, msg)
        message_future.add_done_callback(callback)
        sleep_time = random.choice(range(1, 3, 1))
        time.sleep(sleep_time)
