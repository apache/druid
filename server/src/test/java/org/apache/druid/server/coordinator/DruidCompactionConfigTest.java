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
import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertEquals(defaultConfig, deserialized);
  }

  @Test
  public void testSerdeWithDatasourceConfigs() throws Exception
  {
    final DruidCompactionConfig config = new DruidCompactionConfig(
        Arrays.asList(
            DataSourceCompactionConfig
                .builder()
                .forDataSource(TestDataSource.WIKI)
                .withSkipOffsetFromLatest(Period.hours(1))
                .build(),
            DataSourceCompactionConfig
                .builder()
                .forDataSource(TestDataSource.KOALA)
                .withSkipOffsetFromLatest(Period.hours(2))
                .build()
        ),
        null,
        null,
        null,
        null
    );

    final String json = MAPPER.writeValueAsString(config);
    DruidCompactionConfig deserialized = MAPPER.readValue(json, DruidCompactionConfig.class);
    Assert.assertEquals(config, deserialized);
  }

  @Test
  public void testCopyWithClusterConfig()
  {
    final DruidCompactionConfig config = DruidCompactionConfig.empty();

    final ClusterCompactionConfig clusterConfig = new ClusterCompactionConfig(
        0.5,
        10,
        false,
        new NewestSegmentFirstPolicy(null)
    );
    final DruidCompactionConfig copy = config.withClusterConfig(clusterConfig);

    Assert.assertEquals(clusterConfig, copy.clusterConfig());
    Assert.assertNotEquals(clusterConfig, config.clusterConfig());
  }

  @Test
  public void testCopyWithDatasourceConfigs()
  {
    final DruidCompactionConfig config = DruidCompactionConfig.empty();
    Assert.assertTrue(config.getCompactionConfigs().isEmpty());

    final DataSourceCompactionConfig dataSourceConfig = DataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withEngine(CompactionEngine.NATIVE)
        .build();
    final DruidCompactionConfig copy
        = config.withDatasourceConfigs(Collections.singletonList(dataSourceConfig));

    Assert.assertEquals(1, copy.getCompactionConfigs().size());
    Assert.assertEquals(dataSourceConfig, copy.findConfigForDatasource(TestDataSource.WIKI).orNull());
  }
}
