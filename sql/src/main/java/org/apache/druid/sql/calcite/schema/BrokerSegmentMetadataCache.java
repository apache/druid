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

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.vavr.Predicates;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.AbstractSegmentMetadataCache;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;
import org.apache.druid.timeline.SegmentId;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Broker-side cache of segment metadata that combines segments to identify
 * dataSources which become "tables" in Calcite. This cache provides the "physical"
 * metadata about a dataSource which is blended with catalog "logical" metadata
 * to provide the final user-view of each dataSource.
 * <p>
 * This class extends {@link AbstractSegmentMetadataCache} and introduces following changes,
 * <ul>
 *   <li>The refresh mechanism now includes polling the coordinator for dataSource schema,
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
        serverView,
        config,
        escalator,
        internalQueryConfig,
        emitter
    );
    this.dataSourceMetadataFactory = dataSourceMetadataFactory;
    this.coordinatorClient = coordinatorClient;
  }

  /**
   * Refreshes the set of segments in two steps:
   * <ul>
   *  <li>Polls the coordinator for the dataSource schema to update the {@code tables}.</li>
   *  <li>Refreshes the remaining set of segments by executing a SegmentMetadataQuery.</li>
   * </ul>
   */
  @Override
  public void refresh(final Set<SegmentId> segmentsToRefresh, final Set<String> dataSourcesToRebuild) throws IOException
  {
    Set<String> dataSourcesToQuery = new HashSet<>();

    segmentsToRefresh.forEach(segment -> dataSourcesToQuery.add(segment.getDataSource()));

    Map<String, PhysicalDatasourceMetadata> polledDataSourceMetadata = new HashMap<>();

    // Fetch dataSource information from the Coordinator
    try {
      FutureUtils.getUnchecked(coordinatorClient.fetchDataSourceInformation(dataSourcesToQuery), true)
                 .forEach(dataSourceInformation -> polledDataSourceMetadata.put(
                     dataSourceInformation.getDataSource(),
                     dataSourceMetadataFactory.build(
                         dataSourceInformation.getDataSource(),
                         dataSourceInformation.getRowSignature()
                     )
                 ));
    }
    catch (Exception e) {
      log.warn("Failed to query dataSource information from the Coordinator.");
    }

    // remove any extra dataSources returned
    polledDataSourceMetadata.keySet().removeIf(Predicates.not(dataSourcesToQuery::contains));

    tables.putAll(polledDataSourceMetadata);

    // Remove segments of the dataSource from refresh list for which we received schema from the Coordinator.
    segmentsToRefresh.removeIf(segmentId -> polledDataSourceMetadata.containsKey(segmentId.getDataSource()));

    // Refresh the remaining segments.
    final Set<SegmentId> refreshed = refreshSegments(segmentsToRefresh);

    synchronized (lock) {
      // Add missing segments back to the refresh list.
      segmentsNeedingRefresh.addAll(Sets.difference(segmentsToRefresh, refreshed));

      // Compute the list of dataSources to rebuild tables for.
      dataSourcesToRebuild.addAll(dataSourcesNeedingRebuild);
      refreshed.forEach(segment -> dataSourcesToRebuild.add(segment.getDataSource()));

      // Remove those dataSource for which we received schema from the Coordinator.
      dataSourcesToRebuild.removeAll(polledDataSourceMetadata.keySet());
      dataSourcesNeedingRebuild.clear();
    }

    // Rebuild the dataSources.
    for (String dataSource : dataSourcesToRebuild) {
      final RowSignature rowSignature = buildDruidTable(dataSource);
      if (rowSignature == null) {
        log.info("dataSource [%s] no longer exists, all metadata removed.", dataSource);
        tables.remove(dataSource);
        return;
      }

      final PhysicalDatasourceMetadata physicalDatasourceMetadata = dataSourceMetadataFactory.build(dataSource, rowSignature);
      final PhysicalDatasourceMetadata oldTable = tables.put(dataSource, physicalDatasourceMetadata);
      if (oldTable == null || !oldTable.getRowSignature().equals(physicalDatasourceMetadata.getRowSignature())) {
        log.info("[%s] has new signature: %s.", dataSource, rowSignature);
      } else {
        log.debug("[%s] signature is unchanged.", dataSource);
      }
    }
  }
}
