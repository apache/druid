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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.compaction.NewestSegmentFirstPolicy;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class DruidCompactionConfigTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerdeDefaultConfig() throws Exception
  {
    final DruidCompactionConfig defaultConfig = DruidCompactionConfig.empty();
    final String json = MAPPER.writeValueAsString(defaultConfig);

    DruidCompactionConfig deserialized = MAPPER.readValue(json, DruidCompactionConfig.class);
    Assertions.assertEquals(defaultConfig, deserialized);
  }

  @Test
  public void testSerdeWithLegacyConfig() throws Exception
  {
    final String json = "{\"compactionConfigs\":[],\"useSupervisors\":false,\"engine\":\"native\"}";
    Assertions.assertEquals(DruidCompactionConfig.legacy(), MAPPER.readValue(json, DruidCompactionConfig.class));
  }

  @Test
  public void testSerdeWithDatasourceConfigs() throws Exception
  {
    final DruidCompactionConfig config = new DruidCompactionConfig(
        Arrays.asList(
            InlineSchemaDataSourceCompactionConfig
                .builder()
                .forDataSource(TestDataSource.WIKI)
                .withSkipOffsetFromLatest(Period.hours(1))
                .build(),
            InlineSchemaDataSourceCompactionConfig
                .builder()
                .forDataSource(TestDataSource.KOALA)
                .withSkipOffsetFromLatest(Period.hours(2))
                .build()
        ),
        null,
        null,
        null,
        null,
        null,
        null
    );

    final String json = MAPPER.writeValueAsString(config);
    DruidCompactionConfig deserialized = MAPPER.readValue(json, DruidCompactionConfig.class);
    Assertions.assertEquals(config, deserialized);
  }

  @Test
  public void testCopyWithClusterConfig()
  {
    final DruidCompactionConfig config = DruidCompactionConfig.empty();

    final ClusterCompactionConfig clusterConfig = new ClusterCompactionConfig(
        0.5,
        10,
        new NewestSegmentFirstPolicy(null),
        true,
        CompactionEngine.MSQ,
        true
    );
    final DruidCompactionConfig copy = config.withClusterConfig(clusterConfig);

    Assertions.assertEquals(clusterConfig, copy.clusterConfig());
    Assertions.assertNotEquals(clusterConfig, config.clusterConfig());
  }

  @Test
  public void testCopyWithDatasourceConfigs()
  {
    final DruidCompactionConfig config = DruidCompactionConfig.empty();
    Assertions.assertTrue(config.getCompactionConfigs().isEmpty());

    final DataSourceCompactionConfig dataSourceConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withEngine(CompactionEngine.NATIVE)
        .build();
    final DruidCompactionConfig copy
        = config.withDatasourceConfigs(Collections.singletonList(dataSourceConfig));

    Assertions.assertEquals(1, copy.getCompactionConfigs().size());
    Assertions.assertEquals(dataSourceConfig, copy.findConfigForDatasource(TestDataSource.WIKI).orNull());
  }

  @Test
  public void testDefaultConfigValues()
  {
    final DruidCompactionConfig config = DruidCompactionConfig.empty();
    Assertions.assertTrue(config.getCompactionConfigs().isEmpty());
    Assertions.assertTrue(config.getCompactionPolicy() instanceof NewestSegmentFirstPolicy);
    Assertions.assertEquals(CompactionEngine.NATIVE, config.getEngine());
    Assertions.assertTrue(config.isUseSupervisors());
    Assertions.assertEquals(0.1, config.getCompactionTaskSlotRatio(), 1e-9);
    Assertions.assertEquals(Integer.MAX_VALUE, config.getMaxCompactionTaskSlots());
    Assertions.assertTrue(config.isStoreCompactionStatePerSegment());
  }
}
