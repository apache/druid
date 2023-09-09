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
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.segment.metadata.SegmentMetadataCache;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.timeline.SegmentId;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
@ManageLifecycle
public class BrokerSegmentMetadataCache extends SegmentMetadataCache
{
  private static final EmittingLogger log = new EmittingLogger(BrokerSegmentMetadataCache.class);
  private final PhysicalDatasourceMetadataBuilder physicalDatasourceMetadataBuilder;

  private final ConcurrentMap<String, DatasourceTable.PhysicalDatasourceMetadata> tables = new ConcurrentHashMap<>();

  private final CoordinatorClient coordinatorClient;

  @Inject
  public BrokerSegmentMetadataCache(
      final QueryLifecycleFactory queryLifecycleFactory,
      final TimelineServerView serverView,
      final BrokerSegmentMetadataCacheConfig config,
      final Escalator escalator,
      final InternalQueryConfig internalQueryConfig,
      final ServiceEmitter emitter,
      final PhysicalDatasourceMetadataBuilder physicalDatasourceMetadataBuilder,
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
    this.physicalDatasourceMetadataBuilder = physicalDatasourceMetadataBuilder;
    this.coordinatorClient = coordinatorClient;
  }

  @Override
  public void refresh(final Set<SegmentId> segmentsToRefresh, final Set<String> dataSourcesToRebuild) throws IOException
  {
    Set<String> dataSourcesToQuery = new HashSet<>();

    segmentsToRefresh.forEach(segment -> dataSourcesToQuery.add(segment.getDataSource()));

    Map<String, DatasourceTable.PhysicalDatasourceMetadata> polledDataSourceMetadata = new HashMap<>();

    // Fetch dataSource information from the Coordinator
    try {
      FutureUtils.getUnchecked(coordinatorClient.fetchDataSourceInformation(dataSourcesToQuery), true)
                 .forEach(item -> polledDataSourceMetadata.put(
                     item.getDatasource(),
                     physicalDatasourceMetadataBuilder.build(item)
                 ));
    } catch (Exception e) {
      log.warn(e, "Exception querying coordinator to fetch dataSourceInformation.");
    }

    tables.putAll(polledDataSourceMetadata);

    // Remove segments of the dataSource from refresh list for which we received schema from the Coordinator.
    segmentsToRefresh.forEach(segment -> {
      if (polledDataSourceMetadata.containsKey(segment.getDataSource())) {
        segmentsToRefresh.remove(segment);
      }
    });

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
      rebuildDatasource(dataSource);
    }
  }

  @Override
  public void rebuildDatasource(String dataSource)
  {
    final DataSourceInformation druidTable = buildDruidTable(dataSource);
    if (druidTable == null) {
      log.info("dataSource [%s] no longer exists, all metadata removed.", dataSource);
      tables.remove(dataSource);
      return;
    }
    final DatasourceTable.PhysicalDatasourceMetadata physicalDatasourceMetadata = physicalDatasourceMetadataBuilder.build(druidTable);
    final DatasourceTable.PhysicalDatasourceMetadata oldTable = tables.put(dataSource, physicalDatasourceMetadata);
    if (oldTable == null || !oldTable.rowSignature().equals(physicalDatasourceMetadata.rowSignature())) {
      log.info("[%s] has new signature: %s.", dataSource, druidTable.getRowSignature());
    } else {
      log.info("[%s] signature is unchanged.", dataSource);
    }
  }

  @Override
  public Set<String> getDatasourceNames()
  {
    return tables.keySet();
  }

  @Override
  protected void removeFromTable(String s)
  {
    tables.remove(s);
  }

  @Override
  protected boolean tablesContains(String s)
  {
    return tables.containsKey(s);
  }

  public DatasourceTable.PhysicalDatasourceMetadata getPhysicalDatasourceMetadata(String name)
  {
    return tables.get(name);
  }
}
