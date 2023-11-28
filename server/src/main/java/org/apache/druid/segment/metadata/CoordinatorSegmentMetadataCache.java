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
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.ServerView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.realtime.appenderator.SinksSchema;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Coordinator-side cache of segment metadata that combines segments to build
 * datasources. The cache provides metadata about a datasource, see {@link DataSourceInformation}.
 */
@ManageLifecycle
public class CoordinatorSegmentMetadataCache extends AbstractSegmentMetadataCache<DataSourceInformation>
{
  private static final EmittingLogger log = new EmittingLogger(CoordinatorSegmentMetadataCache.class);

  private final ColumnTypeMergePolicy columnTypeMergePolicy;

  @Inject
  public CoordinatorSegmentMetadataCache(
      QueryLifecycleFactory queryLifecycleFactory,
      CoordinatorServerView serverView,
      SegmentMetadataCacheConfig config,
      Escalator escalator,
      InternalQueryConfig internalQueryConfig,
      ServiceEmitter emitter
  )
  {
    super(queryLifecycleFactory, config, escalator, internalQueryConfig, emitter);
    this.columnTypeMergePolicy = config.getMetadataColumnTypeMergePolicy();
    initServerViewTimelineCallback(serverView);
  }

  private void initServerViewTimelineCallback(final CoordinatorServerView serverView)
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
          public ServerView.CallbackAction segmentSchemaUpdate(SinksSchema sinksSchema)
          {
            updateSchemaForRealtimeSegments(sinksSchema);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  /**
   * Executes SegmentMetadataQuery to fetch schema information for each segment in the refresh list.
   * The schema information for individual segments is combined to construct a table schema, which is then cached.
   *
   * @param segmentsToRefresh    segments for which the schema might have changed
   * @param dataSourcesToRebuild datasources for which the schema might have changed
   * @throws IOException         when querying segment from data nodes and tasks
   */
  @Override
  public void refresh(final Set<SegmentId> segmentsToRefresh, final Set<String> dataSourcesToRebuild) throws IOException
  {
    // Refresh the segments.
    final Set<SegmentId> refreshed = refreshSegments(segmentsToRefresh);

    synchronized (lock) {
      // Add missing segments back to the refresh list.
      segmentsNeedingRefresh.addAll(Sets.difference(segmentsToRefresh, refreshed));

      // Compute the list of datasources to rebuild tables for.
      dataSourcesToRebuild.addAll(dataSourcesNeedingRebuild);
      refreshed.forEach(segment -> dataSourcesToRebuild.add(segment.getDataSource()));
      dataSourcesNeedingRebuild.clear();
    }

    // Rebuild the datasources.
    for (String dataSource : dataSourcesToRebuild) {
      final RowSignature rowSignature = buildDataSourceRowSignature(dataSource);
      if (rowSignature == null) {
        log.info("RowSignature null for dataSource [%s], implying it no longer exists, all metadata removed.", dataSource);
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

  public void updateSchemaForRealtimeSegments(SinksSchema sinksSchema)
  {
    // add datasource to SinksSchema
    String dataSource = null;

    Map<SegmentId, SinksSchema.SinkSchemaChange> segmentIdSinkSchemaChangeMap = sinksSchema.getSinksSchemaChangeMap();

    synchronized (lock) {
      segmentMetadataInfo.compute(
          dataSource,
          (datasourceKey, segmentsMap) -> {
            if (segmentsMap == null) {
              segmentsMap = new ConcurrentSkipListMap<>(SEGMENT_ORDER);
            }
            for (Map.Entry<SegmentId, SinksSchema.SinkSchemaChange> entry : segmentIdSinkSchemaChangeMap.entrySet()) {
              segmentsMap.compute(entry.getKey(), (segmentId, segmentMetadata) -> {
                      if (segmentMetadata == null) {
                        // ignore
                        // log, alert, counter
                        // queue
                        // by design can't occur, alert log
                      } else {
                        // We know this segment.
                        segmentMetadata = AvailableSegmentMetadata
                            .from(segmentMetadata)
                            .withRowSignature(mergeOrCreateRowSignature(segmentMetadata.getRowSignature(), sinksSchema.getColumnMapping(), entry.getValue()))
                            .build();
                      }
                      return segmentMetadata;
                    }
                );
              }
            return segmentsMap;
          });
            }
  }

  private RowSignature mergeOrCreateRowSignature(
      @Nullable RowSignature previous,
      Map<Integer, SinksSchema.ColumnInformation> columnMapping,
      SinksSchema.SinkSchemaChange sinkSchemaChange
  )
  {
    RowSignature.Builder builder = RowSignature.builder();
    // merge using strategy
    if (!sinkSchemaChange.isDelta()) {
      // create new
      for (Integer columnInt : sinkSchemaChange.getNewColumns()) {
        SinksSchema.ColumnInformation columnInformation = columnMapping.get(columnInt);
        builder.add(columnInformation.getColumnName(), columnInformation.getColumnType());
      }
      return builder.build();
    } else if (previous != null) {
      // merge
      final Map<String, ColumnType> columnTypes = new LinkedHashMap<>();

      for (String column : previous.getColumnNames()) {
        final ColumnType columnType =
            previous.getColumnType(column)
                        .orElseThrow(() -> new ISE("Encountered null type for column [%s]", column));

        columnTypes.compute(column, (c, existingType) -> columnTypeMergePolicy.merge(existingType, columnType));
      }

      for (Integer columnInt : sinkSchemaChange.getUpdatedColumns()) {
        SinksSchema.ColumnInformation columnInformation = columnMapping.get(columnInt);
        String columnName = columnInformation.getColumnName();
        if (!columnTypes.containsKey(columnName)) {
          log.debug("Column send in delta is not present ");
          columnTypes.put(columnName, columnInformation.getColumnType());
        } else {
          columnTypes.compute(columnName, (c, existingType) -> columnTypeMergePolicy.merge(existingType, columnInformation.getColumnType()));
        }
      }

      for (Integer columnInt : sinkSchemaChange.getNewColumns()) {
        SinksSchema.ColumnInformation columnInformation = columnMapping.get(columnInt);
        String columnName = columnInformation.getColumnName();
        if (columnTypes.containsKey(columnName)) {
          log.debug("How come new column is already present?");
          columnTypes.compute(columnName, (c, existingType) -> columnTypeMergePolicy.merge(existingType, columnInformation.getColumnType()));
        } else {
          columnTypes.put(columnName, columnInformation.getColumnType());
        }
      }
      columnTypes.forEach(builder::add);
      return builder.build();
    } else {
      // received delta but we don't have previous message, log and skip
      log.debug("");
      return null;
    }
  }
}
