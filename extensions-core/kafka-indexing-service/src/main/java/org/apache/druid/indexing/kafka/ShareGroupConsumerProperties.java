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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.Set;

final class ShareGroupConsumerProperties
{
  private static final Logger log = new Logger(ShareGroupConsumerProperties.class);

  private static final Set<String> UNSUPPORTED_CONFIGS = Set.of(
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

  static Properties resolve(ShareGroupIndexTaskIOConfig ioConfig, ObjectMapper mapper)
  {
    final Properties properties = new Properties();
    KafkaRecordSupplier.addConsumerPropertiesFromConfig(properties, mapper, ioConfig.getConsumerProperties());
    for (final String key : UNSUPPORTED_CONFIGS) {
      if (properties.remove(key) != null) {
        log.warn("Ignoring consumer property [%s] for Kafka share group [%s].", key, ioConfig.getGroupId());
      }
    }
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ioConfig.getGroupId());
    properties.setProperty(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
    properties.setProperty(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit");
    return properties;
  }
}
