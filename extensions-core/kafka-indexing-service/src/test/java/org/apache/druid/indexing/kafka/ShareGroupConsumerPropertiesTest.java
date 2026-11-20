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

import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

public class ShareGroupConsumerPropertiesTest
{
  @Test
  public void testEnforcesExplicitAcknowledgementsAfterDynamicConfigExpansion()
  {
    final ShareGroupIndexTaskIOConfig config = new ShareGroupIndexTaskIOConfig(
        "topic",
        "share-group",
        Map.of(
            "bootstrap.servers", "localhost:9092",
            KafkaSupervisorIOConfig.DRUID_DYNAMIC_CONFIG_PROVIDER_KEY, Map.of(
                "type", "mapString",
                "config", Map.of(
                    ConsumerConfig.GROUP_ID_CONFIG, "ignored",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true",
                    ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "implicit",
                    ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "batch_optimized",
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "25"
                )
            )
        ),
        new JsonInputFormat(null, null, null, null, null),
        null
    );
    final Properties resolved = ShareGroupConsumerProperties.resolve(config, new DefaultObjectMapper());
    Assertions.assertEquals("share-group", resolved.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    Assertions.assertEquals("explicit", resolved.getProperty(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG));
    Assertions.assertEquals("record_limit", resolved.getProperty(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG));
    Assertions.assertNull(resolved.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    Assertions.assertEquals("25", resolved.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    Assertions.assertEquals("localhost:9092", resolved.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
  }
}
