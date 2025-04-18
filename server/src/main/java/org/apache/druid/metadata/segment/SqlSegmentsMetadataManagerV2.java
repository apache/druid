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

package org.apache.druid.metadata.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.server.coordinator.DruidCompactionConfig;

/**
 * Implementation V2 of {@link SegmentsMetadataManager}, that can use the
 * segments cached in {@link SegmentMetadataCache} to build a {@link DataSourcesSnapshot}.
 * <p>
 * This class acts as a wrapper over {@link SqlSegmentsMetadataManager} and the
 * {@link SegmentMetadataCache}. If the cache is enabled, an additional poll is
 * not done and the segments already present in the cache are used to build the
 * snapshot. If the {@link SegmentMetadataCache} is disabled, the polling is
 * delegated to the legacy implementation in {@link SqlSegmentsMetadataManager}.
 * <p>
 * The Coordinator always uses the snapshot to perform various segment management
 * duties such as loading, balancing, etc.
 * The Overlord uses the snapshot only when compaction supervisors are enabled.
 * Thus, when running the Overlord as a standalone service (i.e. not combined
 * with the Coordinator), {@link #startPollingDatabasePeriodically()} and
 * {@link #stopPollingDatabasePeriodically()} are called based on the current
 * state of {@link DruidCompactionConfig#isUseSupervisors()}.
 */
@ManageLifecycle
public class SqlSegmentsMetadataManagerV2 implements SegmentsMetadataManager
{
  private static final Logger log = new Logger(SqlSegmentsMetadataManagerV2.class);

  private final SegmentsMetadataManager delegate;
  private final SegmentMetadataCache segmentMetadataCache;
  private final CentralizedDatasourceSchemaConfig schemaConfig;

  public SqlSegmentsMetadataManagerV2(
      SegmentMetadataCache segmentMetadataCache,
      SegmentSchemaCache segmentSchemaCache,
      SQLMetadataConnector connector,
      Supplier<SegmentsMetadataManagerConfig> managerConfig,
      Supplier<MetadataStorageTablesConfig> tablesConfig,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig,
      ServiceEmitter serviceEmitter,
      ObjectMapper jsonMapper
  )
  {
    this.delegate = new SqlSegmentsMetadataManager(
        jsonMapper,
        managerConfig, tablesConfig, connector, segmentSchemaCache,
        centralizedDatasourceSchemaConfig, serviceEmitter
    );
    this.segmentMetadataCache = segmentMetadataCache;
    this.schemaConfig = centralizedDatasourceSchemaConfig;
  }

  /**
   * @return true if segment metadata cache is enabled and segment schema cache
   * is not enabled. Segment metadata cache currently does not handle segment
   * schema updates.
   */
  private boolean useCacheToBuildTimeline()
  {
    return segmentMetadataCache.isEnabled() && !schemaConfig.isEnabled();
  }

  @Override
  @LifecycleStart
  public void start()
  {
    delegate.start();
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    delegate.stop();
  }

  @Override
  public void startPollingDatabasePeriodically()
  {
    if (useCacheToBuildTimeline()) {
      log.info("Using segments in metadata cache to build timeline.");
    } else {
      log.info("Starting poll of segments from metadata store.");
      delegate.startPollingDatabasePeriodically();
    }
  }

  @Override
  public void stopPollingDatabasePeriodically()
  {
    if (useCacheToBuildTimeline()) {
      // Cache does not stop polling until service is stopped
    } else {
      log.info("Stopping poll of segments from metadata store.");
      delegate.stopPollingDatabasePeriodically();
    }
  }

  @Override
  public boolean isPollingDatabasePeriodically()
  {
    // When cache is being used, this will return true even after
    // stopPollingDatabasePeriodically has been called
    return useCacheToBuildTimeline() || delegate.isPollingDatabasePeriodically();
  }

  @Override
  public DataSourcesSnapshot getDataSourceSnapshot()
  {
    if (useCacheToBuildTimeline()) {
      return segmentMetadataCache.getDatasourcesSnapshot();
    } else {
      return delegate.getDataSourceSnapshot();
    }
  }

  @Override
  public DataSourcesSnapshot forceUpdateAndGetSnapshot()
  {
    if (useCacheToBuildTimeline()) {
      // TODO: we cannot force the cache to refresh, we can just wait
      return segmentMetadataCache.getDatasourcesSnapshot();
    } else {
      return delegate.forceUpdateAndGetSnapshot();
    }
  }

  // Methods delegated to SqlSegmentsMetadataManager V1 implementation

  @Override
  public void populateUsedFlagLastUpdatedAsync()
  {
    delegate.populateUsedFlagLastUpdatedAsync();
  }

  @Override
  public void stopAsyncUsedFlagLastUpdatedUpdate()
  {
    delegate.stopAsyncUsedFlagLastUpdatedUpdate();
  }
}
