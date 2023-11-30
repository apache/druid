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

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
            log.info("Received segment update event %s", segment.getId());
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
          public ServerView.CallbackAction segmentSchemaUpdate(SegmentSchemas segmentSchemas)
          {
            log.info("Received segment schema update event %s", segmentSchemas);
            updateSchemaForSegments(segmentSchemas);
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
        log.info("RowSignature null for dataSource [%s], implying that it no longer exists. All metadata removed.", dataSource);
        tables.remove(dataSource);
        continue;
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

  /**
   * Update schema for segments.
   */
  private void updateSchemaForSegments(SegmentSchemas segmentSchemas)
  {
    Map<Integer, SegmentSchemas.ColumnInformation> columnInformationMap = segmentSchemas.getColumnMapping();
    List<SegmentSchemas.SegmentSchema> segmentSchemaList = segmentSchemas.getSegmentSchemaList();

    for (SegmentSchemas.SegmentSchema segmentSchema : segmentSchemaList) {
      String dataSource = segmentSchema.getDataSource();
      SegmentId segmentId = SegmentId.tryParse(dataSource, segmentSchema.getSegmentId());

      log.info("Applying schema update for segmentId %s datasource %s", segmentId, dataSource);

      segmentMetadataInfo.compute(
          dataSource,
          (dataSourceKey, segmentsMap) -> {
            if (segmentsMap == null) {
              segmentsMap = new ConcurrentSkipListMap<>(SEGMENT_ORDER);
            }
            segmentsMap.compute(
                segmentId, (id, segmentMetadata) -> {
                  if (segmentMetadata == null) {
                    // By design, this case shouldn't arise since both segment and schema is announced in the same flow
                    // and messages shouldn't be lost in the poll
                    log.makeAlert("Received schema update for unknown segment id [%s]", segmentId).emit();
                  } else {
                    // We know this segment.
                    Optional<RowSignature> rowSignature =
                        mergeOrCreateRowSignature(
                            segmentId,
                            segmentMetadata.getRowSignature(),
                            columnInformationMap,
                            segmentSchema
                        );
                    if (!rowSignature.isPresent()) {
                      log.info("Oh rowsignature is empty?? ");
                    }
                    if (rowSignature.isPresent()) {
                      log.info("Build rowSignature is %s", rowSignature);
                      segmentMetadata = AvailableSegmentMetadata
                          .from(segmentMetadata)
                          .withRowSignature(rowSignature.get())
                          .build();
                    }
                  }
                  return segmentMetadata;
                }
            );
            return segmentsMap;
          }
      );
    }
  }

  /**
   * Merge the schema update with the existing signature of a segment or create new RowSignature.
   */
  @VisibleForTesting
  Optional<RowSignature> mergeOrCreateRowSignature(
      SegmentId segmentId,
      @Nullable RowSignature previousSignature,
      Map<Integer, SegmentSchemas.ColumnInformation> columnMapping,
      SegmentSchemas.SegmentSchema segmentSchema
  )
  {
    if (!segmentSchema.isDelta()) {
      // create new, doesn't matter if we already have a previous signature
      RowSignature.Builder builder = RowSignature.builder();
      for (Integer columnInt : segmentSchema.getNewColumns()) {
        SegmentSchemas.ColumnInformation columnInformation = columnMapping.get(columnInt);
        builder.add(columnInformation.getColumnName(), columnInformation.getColumnType());
      }
      return Optional.of(builder.build());
    } else if (previousSignature != null) {
      // merge the schema update with the previous signature
      RowSignature.Builder builder = RowSignature.builder();
      final Map<String, ColumnType> columnTypes = new LinkedHashMap<>();

      for (String column : previousSignature.getColumnNames()) {
        final ColumnType columnType =
            previousSignature.getColumnType(column)
                    .orElseThrow(() -> new ISE("Encountered null type for column [%s]", column));

        columnTypes.compute(column, (c, existingType) -> columnTypeMergePolicy.merge(existingType, columnType));
      }

      for (Integer columnInt : segmentSchema.getUpdatedColumns()) {
        SegmentSchemas.ColumnInformation columnInformation = columnMapping.get(columnInt);
        String columnName = columnInformation.getColumnName();
        if (!columnTypes.containsKey(columnName)) {
          log.debug("Column send in delta is not present ");
          columnTypes.put(columnName, columnInformation.getColumnType());
        } else {
          columnTypes.compute(columnName, (c, existingType) -> columnTypeMergePolicy.merge(existingType, columnInformation.getColumnType()));
        }
      }

      for (Integer columnInt : segmentSchema.getNewColumns()) {
        SegmentSchemas.ColumnInformation columnInformation = columnMapping.get(columnInt);
        String columnName = columnInformation.getColumnName();
        if (columnTypes.containsKey(columnName)) {
          log.debug("How come new column is already present?");
          columnTypes.compute(columnName, (c, existingType) -> columnTypeMergePolicy.merge(existingType, columnInformation.getColumnType()));
        } else {
          columnTypes.put(columnName, columnInformation.getColumnType());
        }
      }
      columnTypes.forEach(builder::add);
      return Optional.of(builder.build());
    } else {
      // we don't have the previous signature, but we received delta update, raise alert
      // this case shouldn't arise though
      log.makeAlert("Received delta schema update but no previous schema exists for segmentId [%s]", segmentId).emit();
      return Optional.empty();
    }
  }
}
