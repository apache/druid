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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

public class KafkaConsumerMonitor extends AbstractMonitor
{
  private volatile boolean stopAfterNext = false;

  // Kafka metric name -> Druid metric name
  private static final Map<String, String> METRICS =
      ImmutableMap.<String, String>builder()
                  .put("bytes-consumed-total", "kafka/consumer/bytesConsumed")
                  .put("records-consumed-total", "kafka/consumer/recordsConsumed")
                  .build();
  private static final String TOPIC_TAG = "topic";
  private static final Set<String> TOPIC_METRIC_TAGS = ImmutableSet.of("client-id", TOPIC_TAG);

  private final KafkaConsumer<?, ?> consumer;
  private final Map<String, AtomicLong> counters = new HashMap<>();

  public KafkaConsumerMonitor(final KafkaConsumer<?, ?> consumer)
  {
    this.consumer = consumer;
  }

  @Override
  public boolean doMonitor(final ServiceEmitter emitter)
  {
    for (final Map.Entry<MetricName, ? extends Metric> entry : consumer.metrics().entrySet()) {
      final MetricName metricName = entry.getKey();

      if (METRICS.containsKey(metricName.name()) && isTopicMetric(metricName)) {
        final String topic = metricName.tags().get(TOPIC_TAG);
        final long newValue = ((Number) entry.getValue().metricValue()).longValue();
        final long priorValue =
            counters.computeIfAbsent(metricName.name(), ignored -> new AtomicLong())
                    .getAndSet(newValue);

        if (newValue != priorValue) {
          final ServiceMetricEvent.Builder builder =
              new ServiceMetricEvent.Builder().setDimension(TOPIC_TAG, topic);
          emitter.emit(builder.setMetric(METRICS.get(metricName.name()), newValue - priorValue));
        }
      }
    }

    return !stopAfterNext;
  }

  public void stopAfterNextEmit()
  {
    stopAfterNext = true;
  }

  private static boolean isTopicMetric(final MetricName metricName)
  {
    // Certain metrics are emitted both as grand totals and broken down by topic; we want to ignore the grand total and
    // only look at the per-topic metrics. See https://kafka.apache.org/documentation/#consumer_fetch_monitoring.
    return TOPIC_METRIC_TAGS.equals(metricName.tags().keySet())
           && !Strings.isNullOrEmpty(metricName.tags().get(TOPIC_TAG));
  }
}
