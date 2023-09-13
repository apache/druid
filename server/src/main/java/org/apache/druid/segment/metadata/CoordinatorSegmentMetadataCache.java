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

package org.apache.druid.segment.metadata;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.timeline.SegmentId;

import java.io.IOException;
import java.util.Set;

/**
 * Coordinator-side cache of segment metadata that combines segments to identify
 * datasources. The cache provides metadata about a dataSource, see {@link DataSourceInformation}.
 */
@ManageLifecycle
public class CoordinatorSegmentMetadataCache extends AbstractSegmentMetadataCache<DataSourceInformation>
{
  private static final EmittingLogger log = new EmittingLogger(CoordinatorSegmentMetadataCache.class);

  @Inject
  public CoordinatorSegmentMetadataCache(
      QueryLifecycleFactory queryLifecycleFactory,
      TimelineServerView serverView,
      SegmentMetadataCacheConfig config,
      Escalator escalator,
      InternalQueryConfig internalQueryConfig,
      ServiceEmitter emitter
  )
  {
    super(queryLifecycleFactory, serverView, config, escalator, internalQueryConfig, emitter);
  }

  /**
   * Fires SegmentMetadataQuery to fetch schema information for each segment in the refresh list.
   * The schema information for individual segments is combined to construct a table schema, which is then cached.
   */
  @Override
  public void refresh(final Set<SegmentId> segmentsToRefresh, final Set<String> dataSourcesToRebuild) throws IOException
  {
    // Refresh the segments.
    final Set<SegmentId> refreshed = refreshSegments(segmentsToRefresh);

    synchronized (lock) {
      // Add missing segments back to the refresh list.
      segmentsNeedingRefresh.addAll(Sets.difference(segmentsToRefresh, refreshed));

      // Compute the list of dataSources to rebuild tables for.
      dataSourcesToRebuild.addAll(dataSourcesNeedingRebuild);
      refreshed.forEach(segment -> dataSourcesToRebuild.add(segment.getDataSource()));
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
      DataSourceInformation druidTable = new DataSourceInformation(dataSource, rowSignature);
      final DataSourceInformation oldTable = tables.put(dataSource, druidTable);
      if (oldTable == null || !oldTable.getRowSignature().equals(druidTable.getRowSignature())) {
        log.info("[%s] has new signature: %s.", dataSource, druidTable.getRowSignature());
      } else {
        log.debug("[%s] signature is unchanged.", dataSource);
      }
    }
  }
}
