/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.kafka;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaConsumerMonitor extends AbstractMonitor
{
  private volatile boolean stopAfterNext = false;

  private static final String CLIENT_ID_TAG = "client-id";

  // Kafka metric name -> Kafka metric descriptor
  private static final Map<String, KafkaConsumerMetric> METRICS =
      Stream.of(
          new KafkaConsumerMetric(
              "bytes-consumed-total",
              "kafka/consumer/bytesConsumed",
              Set.of(CLIENT_ID_TAG, "topic"),
              KafkaConsumerMetric.MetricType.COUNTER
          ),
          new KafkaConsumerMetric(
              "records-consumed-total",
              "kafka/consumer/recordsConsumed",
              Set.of(CLIENT_ID_TAG, "topic"),
              KafkaConsumerMetric.MetricType.COUNTER
          ),
          new KafkaConsumerMetric(
              "fetch-total",
              "kafka/consumer/fetch",
              Set.of(CLIENT_ID_TAG),
              KafkaConsumerMetric.MetricType.COUNTER
          ),
          new KafkaConsumerMetric(
              "fetch-rate",
              "kafka/consumer/fetchRate",
              Set.of(CLIENT_ID_TAG),
              KafkaConsumerMetric.MetricType.GAUGE
          ),
          new KafkaConsumerMetric(
              "fetch-latency-avg",
              "kafka/consumer/fetchLatencyAvg",
              Set.of(CLIENT_ID_TAG),
              KafkaConsumerMetric.MetricType.GAUGE
          ),
          new KafkaConsumerMetric(
              "fetch-latency-max",
              "kafka/consumer/fetchLatencyMax",
              Set.of(CLIENT_ID_TAG),
              KafkaConsumerMetric.MetricType.GAUGE
          ),
          new KafkaConsumerMetric(
              "fetch-size-avg",
              "kafka/consumer/fetchSizeAvg",
              Set.of(CLIENT_ID_TAG, "topic"),
              KafkaConsumerMetric.MetricType.GAUGE
          ),
          new KafkaConsumerMetric(
              "fetch-size-max",
              "kafka/consumer/fetchSizeMax",
              Set.of(CLIENT_ID_TAG, "topic"),
              KafkaConsumerMetric.MetricType.GAUGE
          ),
          new KafkaConsumerMetric(
              "records-lag",
              "kafka/consumer/recordsLag",
              Set.of(CLIENT_ID_TAG, "topic", "partition"),
              KafkaConsumerMetric.MetricType.GAUGE
          ),
          new KafkaConsumerMetric(
              "records-per-request-avg",
              "kafka/consumer/recordsPerRequestAvg",
              Set.of(CLIENT_ID_TAG, "topic"),
              KafkaConsumerMetric.MetricType.GAUGE
          ),
          new KafkaConsumerMetric(
              "outgoing-byte-total",
              "kafka/consumer/outgoingBytes",
              Set.of(CLIENT_ID_TAG, "node-id"),
              KafkaConsumerMetric.MetricType.COUNTER
          ),
          new KafkaConsumerMetric(
              "incoming-byte-total",
              "kafka/consumer/incomingBytes",
              Set.of(CLIENT_ID_TAG, "node-id"),
              KafkaConsumerMetric.MetricType.COUNTER
          )
      ).collect(Collectors.toMap(KafkaConsumerMetric::getKafkaMetricName, Function.identity()));

  private final KafkaConsumer<?, ?> consumer;
  private final Map<MetricName, AtomicLong> counters = new HashMap<>();

  public KafkaConsumerMonitor(final KafkaConsumer<?, ?> consumer)
  {
    this.consumer = consumer;
  }

  @Override
  public boolean doMonitor(final ServiceEmitter emitter)
  {
    for (final Map.Entry<MetricName, ? extends Metric> entry : consumer.metrics().entrySet()) {
      final MetricName metricName = entry.getKey();
      final KafkaConsumerMetric kafkaConsumerMetric = METRICS.get(metricName.name());

      if (kafkaConsumerMetric != null &&
          kafkaConsumerMetric.getDimensions().equals(metricName.tags().keySet())) {
        final Number newValue = (Number) entry.getValue().metricValue();
        final Number emitValue;

        if (kafkaConsumerMetric.getMetricType() == KafkaConsumerMetric.MetricType.GAUGE || newValue == null) {
          emitValue = newValue;
        } else if (kafkaConsumerMetric.getMetricType() == KafkaConsumerMetric.MetricType.COUNTER) {
          final long newValueAsLong = newValue.longValue();
          final long priorValue =
              counters.computeIfAbsent(metricName, ignored -> new AtomicLong())
                      .getAndSet(newValueAsLong);
          emitValue = newValueAsLong - priorValue;
        } else {
          throw DruidException.defensive("Unexpected metric type[%s]", kafkaConsumerMetric.getMetricType());
        }

        if (emitValue != null && !Double.isNaN(emitValue.doubleValue())) {
          final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
          for (final String dimension : kafkaConsumerMetric.getDimensions()) {
            if (!dimension.equals(CLIENT_ID_TAG)) {
              builder.setDimension(dimension, metricName.tags().get(dimension));
            }
          }
          emitter.emit(builder.setMetric(kafkaConsumerMetric.getDruidMetricName(), emitValue));
        }
      }
    }

    return !stopAfterNext;
  }

  public void stopAfterNextEmit()
  {
    stopAfterNext = true;
  }
}
