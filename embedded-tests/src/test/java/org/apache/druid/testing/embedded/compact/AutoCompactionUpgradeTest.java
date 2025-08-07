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

package org.apache.druid.testing.embedded.compact;

import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class AutoCompactionUpgradeTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.manager.segments.useIncrementalCache", "always");

  protected CompactionResourceTestClient compactionResource;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addServer(coordinator);
  }

  @BeforeAll
  public void setupClient()
  {
    compactionResource = new CompactionResourceTestClient(cluster);
  }

  @Test
  public void test_configCanBeUpdated_afterVersionUpgrades() throws Exception
  {
    // Insert a minimal compaction config manually into the DB
    // Then start the Overlord to ensure that the Overlord has the latest config cached
    insertMinimalCompactionConfig((TestDerbyConnector) coordinator.bindings().sqlMetadataConnector());
    cluster.addServer(overlord);
    overlord.start();

    // Verify that compaction config already exist. This config was inserted manually into the database using SQL script.
    DruidCompactionConfig coordinatorCompactionConfig = DruidCompactionConfig.empty()
        .withDatasourceConfigs(compactionResource.getAllCompactionConfigs());
    DataSourceCompactionConfig foundDataSourceCompactionConfig
        = coordinatorCompactionConfig.findConfigForDatasource(dataSource).orNull();
    Assertions.assertNotNull(foundDataSourceCompactionConfig);

    // Now submit a new auto compaction configuration
    PartitionsSpec newPartitionsSpec = new DynamicPartitionsSpec(4000, null);
    Period newSkipOffset = Period.seconds(0);

    DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(dataSource)
        .withSkipOffsetFromLatest(newSkipOffset)
        .withTuningConfig(
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                new MaxSizeSplitHintSpec(null, 1),
                newPartitionsSpec,
                null,
                null,
                null,
                null,
                null,
                1,
                null,
                null,
                null,
                null,
                null,
                1,
                null
            )
        )
        .withGranularitySpec(
            new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null)
        )
        .withIoConfig(new UserCompactionTaskIOConfig(true))
        .build();
    compactionResource.submitCompactionConfig(compactionConfig);

    // Verify that compaction was successfully updated
    foundDataSourceCompactionConfig
        = compactionResource.getDataSourceCompactionConfig(dataSource);
    Assertions.assertNotNull(foundDataSourceCompactionConfig);
    Assertions.assertNotNull(foundDataSourceCompactionConfig.getTuningConfig());
    Assertions.assertEquals(foundDataSourceCompactionConfig.getTuningConfig().getPartitionsSpec(), newPartitionsSpec);
    Assertions.assertEquals(foundDataSourceCompactionConfig.getSkipOffsetFromLatest(), newSkipOffset);
  }

  /**
   * Inserts a bare-bones compaction config directly into the druid_config
   * metadata table.
   */
  private void insertMinimalCompactionConfig(TestDerbyConnector sqlConnector)
  {
    final String configJson = StringUtils.format(
        "{\"compactionConfigs\":[{\"dataSource\":\"%s\"}]}",
        dataSource
    );

    sqlConnector.retryWithHandle(
        handle -> handle.insert(
            StringUtils.format(
                "INSERT INTO %s (name, payload) VALUES ('coordinator.compaction.config',?)",
                sqlConnector.getMetadataTablesConfig().getConfigTable()
            ),
            configJson.getBytes(StandardCharsets.UTF_8)
        )
    );
  }
}
