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

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.logger.Logger;
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
  private static final Logger log = new Logger(KafkaConsumerMonitor.class);

  private volatile boolean stopAfterNext = false;

  private static final String CLIENT_ID_TAG = "client-id";
  private static final String TOPIC_TAG = "topic";
  private static final String PARTITION_TAG = "partition";
  private static final String NODE_ID_TAG = "node-id";

  private static final String POLL_IDLE_RATIO_METRIC_NAME = "poll-idle-ratio-avg";

  /**
   * Kafka metric name -> Kafka metric descriptor. Taken from
   * <a href="https://kafka.apache.org/documentation/#consumer_fetch_monitoring">documentation</a>.
   */
  private static final Map<String, KafkaConsumerMetric> METRICS =
      Stream.of(
          new KafkaConsumerMetric(
              "bytes-consumed-total",
              "kafka/consumer/bytesConsumed",
              Set.of(CLIENT_ID_TAG, TOPIC_TAG),
              KafkaConsumerMetric.MetricType.COUNTER
          ),
          new KafkaConsumerMetric(
              "records-consumed-total",
              "kafka/consumer/recordsConsumed",
              Set.of(CLIENT_ID_TAG, TOPIC_TAG),
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
              Set.of(CLIENT_ID_TAG, TOPIC_TAG),
              KafkaConsumerMetric.MetricType.GAUGE
          ),
          new KafkaConsumerMetric(
              "fetch-size-max",
              "kafka/consumer/fetchSizeMax",
              Set.of(CLIENT_ID_TAG, TOPIC_TAG),
              KafkaConsumerMetric.MetricType.GAUGE
          ),
          new KafkaConsumerMetric(
              "records-lag",
              "kafka/consumer/recordsLag",
              Set.of(CLIENT_ID_TAG, TOPIC_TAG, PARTITION_TAG),
              KafkaConsumerMetric.MetricType.GAUGE
          ),
          new KafkaConsumerMetric(
              "records-per-request-avg",
              "kafka/consumer/recordsPerRequestAvg",
              Set.of(CLIENT_ID_TAG, TOPIC_TAG),
              KafkaConsumerMetric.MetricType.GAUGE
          ),
          new KafkaConsumerMetric(
              "outgoing-byte-total",
              "kafka/consumer/outgoingBytes",
              Set.of(CLIENT_ID_TAG, NODE_ID_TAG),
              KafkaConsumerMetric.MetricType.COUNTER
          ),
          new KafkaConsumerMetric(
              "incoming-byte-total",
              "kafka/consumer/incomingBytes",
              Set.of(CLIENT_ID_TAG, NODE_ID_TAG),
              KafkaConsumerMetric.MetricType.COUNTER
          )
      ).collect(Collectors.toMap(KafkaConsumerMetric::getKafkaMetricName, Function.identity()));

  private final KafkaConsumer<?, ?> consumer;
  private final Map<MetricName, AtomicLong> counters = new HashMap<>();
  private final AtomicDouble pollIdleRatioAvg = new AtomicDouble(1.0d);

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

      // Certain metrics are emitted both as grand totals and broken down by various tags; we want to ignore the
      // grand total and only emit the broken-down metrics. To do this, we check that metricName.tags() equals the
      // set of expected dimensions.
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
            if (!CLIENT_ID_TAG.equals(dimension)) {
              builder.setDimension(dimension, metricName.tags().get(dimension));
            }
          }
          emitter.emit(builder.setMetric(kafkaConsumerMetric.getDruidMetricName(), emitValue));
        }
      }

      // Capture `poll-idle-ratio-avg` metric for autoscaler purposes.
      if (POLL_IDLE_RATIO_METRIC_NAME.equals(metricName.name())) {
        if (entry.getValue().metricValue() != null) {
          pollIdleRatioAvg.set(((Number) entry.getValue().metricValue()).doubleValue());
        }
      }
    }

    return !stopAfterNext;
  }

  public void stopAfterNextEmit()
  {
    stopAfterNext = true;
  }

  /**
   * Average poll-to-idle ratio as reported by the Kafka consumer.
   * A value of 0 represents that the consumer is never idle, i.e. always consuming.
   * A value of 1 represents that the consumer is always idle, i.e. not receiving data.
   * @return
   */
  public double getPollIdleRatioAvg()
  {
    return pollIdleRatioAvg.get();
  }
}
