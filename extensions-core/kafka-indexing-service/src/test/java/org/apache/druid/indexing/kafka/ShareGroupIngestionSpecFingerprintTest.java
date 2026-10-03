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

import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.storage.local.LocalFileStorageConnectorProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

public class ShareGroupIngestionSpecFingerprintTest
{
  @Test
  public void testFingerprintIsStableAndIgnoresConnectionProperties()
  {
    final LinkedHashMap<String, Object> firstProperties = new LinkedHashMap<>();
    firstProperties.put("bootstrap.servers", "first:9092");
    firstProperties.put("client.id", "first-client");
    final LinkedHashMap<String, Object> secondProperties = new LinkedHashMap<>();
    secondProperties.put("client.id", "second-client");
    secondProperties.put("bootstrap.servers", "second:9092");

    final String first = fingerprint(dataSchema("datasource"), ioConfig("topic", "group", firstProperties));
    final String second = fingerprint(dataSchema("datasource"), ioConfig("topic", "group", secondProperties));

    Assertions.assertEquals(first, second);
  }

  @Test
  public void testFingerprintChangesWithIngestionBehavior()
  {
    final String original = fingerprint(
        dataSchema("datasource"),
        ioConfig("topic", "group", Map.of("bootstrap.servers", "localhost:9092"))
    );

    Assertions.assertNotEquals(
        original,
        fingerprint(
            dataSchema("other-datasource"),
            ioConfig("topic", "group", Map.of("bootstrap.servers", "localhost:9092"))
        )
    );
    Assertions.assertNotEquals(
        original,
        fingerprint(
            dataSchema("datasource"),
            ioConfig("other-topic", "group", Map.of("bootstrap.servers", "localhost:9092"))
        )
    );
  }

  private static String fingerprint(DataSchema dataSchema, ShareGroupIndexTaskIOConfig ioConfig)
  {
    return ShareGroupIngestionSpecFingerprint.compute(
        new DefaultObjectMapper(),
        dataSchema,
        tuningConfig(),
        ioConfig
    );
  }

  private static DataSchema dataSchema(String dataSource)
  {
    return DataSchema.builder()
                     .withDataSource(dataSource)
                     .withTimestamp(new TimestampSpec("__time", null, null))
                     .withDimensions(DimensionsSpec.EMPTY)
                     .build();
  }

  private static ShareGroupIndexTaskIOConfig ioConfig(
      String topic,
      String groupId,
      Map<String, Object> consumerProperties
  )
  {
    return new ShareGroupIndexTaskIOConfig(
        topic,
        groupId,
        consumerProperties,
        null,
        null,
        "test-inbox",
        new LocalFileStorageConnectorProvider(new File("/tmp/share-inbox"))
    );
  }

  private static KafkaIndexTaskTuningConfig tuningConfig()
  {
    return new KafkaIndexTaskTuningConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }
}
