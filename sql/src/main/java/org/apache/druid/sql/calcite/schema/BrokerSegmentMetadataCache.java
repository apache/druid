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

package org.apache.druid.sql.calcite.schema;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.ServerView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.AbstractSegmentMetadataCache;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Broker-side cache of segment metadata that combines segments to build
 * datasources which become "tables" in Calcite. This cache provides the "physical"
 * metadata about a dataSource which is blended with catalog "logical" metadata
 * to provide the final user-view of each dataSource.
 * <p>
 * This class extends {@link AbstractSegmentMetadataCache} and introduces following changes,
 * <ul>
 *   <li>The refresh mechanism includes polling the coordinator for datasource schema,
 *       and falling back to running {@link org.apache.druid.query.metadata.metadata.SegmentMetadataQuery}.</li>
 *   <li>It builds and caches {@link PhysicalDatasourceMetadata} object for the table schema</li>
 * </ul>
 */
@ManageLifecycle
public class BrokerSegmentMetadataCache extends AbstractSegmentMetadataCache<PhysicalDatasourceMetadata>
{
  private static final EmittingLogger log = new EmittingLogger(BrokerSegmentMetadataCache.class);

  private final PhysicalDatasourceMetadataFactory dataSourceMetadataFactory;
  private final CoordinatorClient coordinatorClient;

  private final BrokerSegmentMetadataCacheConfig config;

  @Inject
  public BrokerSegmentMetadataCache(
      final QueryLifecycleFactory queryLifecycleFactory,
      final TimelineServerView serverView,
      final BrokerSegmentMetadataCacheConfig config,
      final Escalator escalator,
      final InternalQueryConfig internalQueryConfig,
      final ServiceEmitter emitter,
      final PhysicalDatasourceMetadataFactory dataSourceMetadataFactory,
      final CoordinatorClient coordinatorClient
  )
  {
    super(
        queryLifecycleFactory,
        config,
        escalator,
        internalQueryConfig,
        emitter
    );
    this.dataSourceMetadataFactory = dataSourceMetadataFactory;
    this.coordinatorClient = coordinatorClient;
    this.config = config;
    initServerViewTimelineCallback(serverView);
  }

  private void initServerViewTimelineCallback(final TimelineServerView serverView)
  {
    serverView.registerTimelineCallback(
        callbackExec,
        new TimelineServerView.TimelineCallback()
        {
          @Override
          public ServerView.CallbackAction timelineInitialized()
          {
            synchronized (lock) {
              isServerViewInitialized = true;
              lock.notifyAll();
            }

            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentAdded(final DruidServerMetadata server, final DataSegment segment)
          {
            addSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DataSegment segment)
          {
            removeSegment(segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction serverSegmentRemoved(
              final DruidServerMetadata server,
              final DataSegment segment
          )
          {
            removeServerSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
          {
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  /**
   * Refreshes the set of segments in two steps:
   * <ul>
   *  <li>Polls the coordinator for the datasource schema.</li>
   *  <li>Refreshes the remaining set of segments by executing a SegmentMetadataQuery and
   *      builds datasource schema by combining segment schema.</li>
   * </ul>
   *
   * @param segmentsToRefresh    segments for which the schema might have changed
   * @param dataSourcesToRebuild datasources for which the schema might have changed
   * @throws IOException         when querying segment schema from data nodes and tasks
   */
  @Override
  public void refresh(final Set<SegmentId> segmentsToRefresh, final Set<String> dataSourcesToRebuild) throws IOException
  {
    // query schema for all datasources, which includes,
    // datasources explicitly marked for rebuilding
    // datasources for the segments to be refreshed
    // prebuilt datasources
    final Set<String> dataSourcesToQuery = new HashSet<>(dataSourcesToRebuild);

    segmentsToRefresh.forEach(segment -> dataSourcesToQuery.add(segment.getDataSource()));

    dataSourcesToQuery.addAll(tables.keySet());

    // Fetch datasource information from the Coordinator
    Map<String, PhysicalDatasourceMetadata> polledDataSourceMetadata = queryDataSourceInformation(dataSourcesToQuery);

    // update datasource metadata in the cache
    polledDataSourceMetadata.forEach(this::updateDSMetadata);

    // Remove segments of the datasource from refresh list for which we received schema from the Coordinator.
    segmentsToRefresh.removeIf(segmentId -> polledDataSourceMetadata.containsKey(segmentId.getDataSource()));

    Set<SegmentId> refreshed = new HashSet<>();

    // Refresh the remaining segments.
    if (!config.isDisableSegmentMetadataQueries()) {
      refreshed = refreshSegments(segmentsToRefresh);
    }

    synchronized (lock) {
      // Add missing segments back to the refresh list.
      segmentsNeedingRefresh.addAll(Sets.difference(segmentsToRefresh, refreshed));

      // Compute the list of datasources to rebuild tables for.
      dataSourcesToRebuild.addAll(dataSourcesNeedingRebuild);
      refreshed.forEach(segment -> dataSourcesToRebuild.add(segment.getDataSource()));

      // Remove those datasource for which we received schema from the Coordinator.
      dataSourcesToRebuild.removeAll(polledDataSourceMetadata.keySet());
      dataSourcesNeedingRebuild.clear();
    }

    // Rebuild the datasources.
    for (String dataSource : dataSourcesToRebuild) {
      final RowSignature rowSignature = buildDataSourceRowSignature(dataSource);
      if (rowSignature == null) {
        log.info("datasource [%s] no longer exists, all metadata removed.", dataSource);
        tables.remove(dataSource);
        continue;
      }

      final PhysicalDatasourceMetadata physicalDatasourceMetadata = dataSourceMetadataFactory.build(dataSource, rowSignature);
      updateDSMetadata(dataSource, physicalDatasourceMetadata);
    }
  }

  private Map<String, PhysicalDatasourceMetadata> queryDataSourceInformation(Set<String> dataSourcesToQuery)
  {
    Stopwatch stopwatch = Stopwatch.createStarted();

    List<DataSourceInformation> dataSourceInformations = null;

    emitter.emit(ServiceMetricEvent.builder().setMetric(
        "metadatacache/schemaPoll/count", 1));
    try {
      dataSourceInformations = FutureUtils.getUnchecked(coordinatorClient.fetchDataSourceInformation(dataSourcesToQuery), true);
    }
    catch (Exception e) {
      log.debug(e, "Failed to query datasource information from the Coordinator.");
      emitter.emit(ServiceMetricEvent.builder().setMetric(
          "metadatacache/schemaPoll/failed", 1));
    }

    emitter.emit(ServiceMetricEvent.builder().setMetric(
        "metadatacache/schemaPoll/time",
        stopwatch.elapsed(TimeUnit.MILLISECONDS)));

    final Map<String, PhysicalDatasourceMetadata> polledDataSourceMetadata = new HashMap<>();

    if (dataSourceInformations != null) {
      dataSourceInformations.forEach(dataSourceInformation -> polledDataSourceMetadata.put(
          dataSourceInformation.getDataSource(),
          dataSourceMetadataFactory.build(
              dataSourceInformation.getDataSource(),
              dataSourceInformation.getRowSignature()
          )
      ));
    }

    return polledDataSourceMetadata;
  }

  private void updateDSMetadata(String dataSource, PhysicalDatasourceMetadata physicalDatasourceMetadata)
  {
    final PhysicalDatasourceMetadata oldTable = tables.put(dataSource, physicalDatasourceMetadata);
    if (oldTable == null || !oldTable.getRowSignature().equals(physicalDatasourceMetadata.getRowSignature())) {
      log.info("[%s] has new signature: %s.", dataSource, physicalDatasourceMetadata.getRowSignature());
    } else {
      log.debug("[%s] signature is unchanged.", dataSource);
    }
  }
}
