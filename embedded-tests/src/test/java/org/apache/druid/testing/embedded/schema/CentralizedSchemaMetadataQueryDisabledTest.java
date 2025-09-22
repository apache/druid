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

package org.apache.druid.testing.embedded.schema;

import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.compact.CompactionSparseColumnTest;
import org.apache.druid.testing.embedded.compact.CompactionTaskTest;
import org.apache.druid.testing.embedded.indexing.KafkaDataFormatsTest;
import org.junit.jupiter.api.Nested;

/**
 * Re-runs various tests with {@code druid.coordinator.segmentMetadata.disableSegmentMetadataQueries}
 * set to true. This is a test-only config used to verify that schema is populated
 * correctly even when metadata queries fail to fetch schema from Historicals.
 */
public class CentralizedSchemaMetadataQueryDisabledTest
{
  private static EmbeddedDruidCluster configureCluster(EmbeddedDruidCluster cluster)
  {
    cluster.addCommonProperty("druid.centralizedDatasourceSchema.enabled", "true")
           .addCommonProperty("druid.centralizedDatasourceSchema.backFillEnabled", "true")
           .addCommonProperty("druid.centralizedDatasourceSchema.backFillPeriod", "500")
           .addCommonProperty("druid.coordinator.segmentMetadata.disableSegmentMetadataQueries", "true")
           .addCommonProperty("druid.sql.planner.metadataRefreshPeriod", "PT0.1s")
           .addCommonProperty("druid.coordinator.segmentMetadata.metadataRefreshPeriod", "PT0.1s")
           .addCommonProperty("druid.manager.segments.useIncrementalCache", "always");

    return cluster;
  }

  @Nested
  public class CompactionSparseColumn extends CompactionSparseColumnTest
  {
    @Override
    protected EmbeddedDruidCluster createCluster()
    {
      return configureCluster(super.createCluster());
    }
  }

  @Nested
  public class CompactionTask extends CompactionTaskTest
  {
    @Override
    protected EmbeddedDruidCluster createCluster()
    {
      return configureCluster(super.createCluster());
    }
  }

  @Nested
  public class KafkaDataFormats extends KafkaDataFormatsTest
  {
    @Override
    public EmbeddedDruidCluster createCluster()
    {
      return configureCluster(super.createCluster());
    }
  }
}
