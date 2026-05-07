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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Helpers for handling Kafka consumer properties supplied to the share-group
 * task. Single Responsibility: filter / validate properties before they reach
 * the {@link org.apache.kafka.clients.consumer.KafkaShareConsumer}.
 *
 * <p>Kafka 4.2.0 rejects a fixed set of properties when the consumer is a
 * share consumer (see {@code ShareConsumerConfig.SHARE_GROUP_UNSUPPORTED_CONFIGS}).
 * If users copy a regular consumer config wholesale, the broker raises
 * {@code ConfigException} at construction time. We strip those keys with a
 * warning so the task can still start; users get a clear log line telling them
 * exactly which keys were dropped.</p>
 *
 * <p>The list is kept in sync with Kafka 4.2.0; if the upstream list changes
 * in a future release, this code should be updated.</p>
 */
public final class ShareGroupConsumerProperties
{
  private static final Logger log = new Logger(ShareGroupConsumerProperties.class);

  /**
   * Mirror of {@code ShareConsumerConfig.SHARE_GROUP_UNSUPPORTED_CONFIGS} from
   * Kafka 4.2.0. Setting any of these on a {@code KafkaShareConsumer} causes
   * a {@code ConfigException} at construction. We strip them defensively.
   */
  static final Set<String> UNSUPPORTED_CONFIGS = ImmutableSet.of(
      "auto.offset.reset",
      "enable.auto.commit",
      "group.instance.id",
      "isolation.level",
      "partition.assignment.strategy",
      "interceptor.classes",
      "session.timeout.ms",
      "heartbeat.interval.ms",
      "group.protocol",
      "group.remote.assignor"
  );

  private ShareGroupConsumerProperties()
  {
  }

  /**
   * Returns a copy of {@code consumerProperties} with all share-group-unsupported
   * keys removed. Each removed key is logged at WARN level once. Insertion
   * order is preserved (uses {@link LinkedHashMap}).
   *
   * @param consumerProperties caller-supplied properties; never null
   * @return new map with unsupported keys removed
   */
  public static Map<String, Object> sanitize(Map<String, Object> consumerProperties)
  {
    final Map<String, Object> sanitized = new LinkedHashMap<>(consumerProperties.size());
    for (Map.Entry<String, Object> entry : consumerProperties.entrySet()) {
      if (UNSUPPORTED_CONFIGS.contains(entry.getKey())) {
        log.warn(
            "Stripping unsupported consumer property [%s] for share-group consumer "
            + "(see Kafka ShareConsumerConfig).",
            entry.getKey()
        );
      } else {
        sanitized.put(entry.getKey(), entry.getValue());
      }
    }
    return sanitized;
  }
}
