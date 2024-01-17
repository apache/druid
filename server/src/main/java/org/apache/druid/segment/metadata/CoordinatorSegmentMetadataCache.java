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

  private final boolean realtimeSegmentSchemaAnnouncement;
  private final ColumnTypeMergePolicy columnTypeMergePolicy;

  @Inject
  public CoordinatorSegmentMetadataCache(
      QueryLifecycleFactory queryLifecycleFactory,
      CoordinatorServerView serverView,
      SegmentMetadataCacheConfig config,
      Escalator escalator,
      InternalQueryConfig internalQueryConfig,
      ServiceEmitter emitter,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    super(queryLifecycleFactory, config, escalator, internalQueryConfig, emitter);
    this.columnTypeMergePolicy = config.getMetadataColumnTypeMergePolicy();
    this.realtimeSegmentSchemaAnnouncement =
        centralizedDatasourceSchemaConfig.isEnabled() && centralizedDatasourceSchemaConfig.announceRealtimeSegmentSchema();
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
          public ServerView.CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
          {
            if (realtimeSegmentSchemaAnnouncement) {
              updateSchemaForSegments(segmentSchemas);
            }
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
    final Set<SegmentId> refreshed = refreshSegments(filterMutableSegments(segmentsToRefresh));

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

  private Set<SegmentId> filterMutableSegments(Set<SegmentId> segmentIds)
  {
    if (realtimeSegmentSchemaAnnouncement) {
      synchronized (lock) {
        segmentIds.removeAll(mutableSegments);
      }
    }
    return segmentIds;
  }

  /**
   * Update schema for segments.
   */
  @VisibleForTesting
  void updateSchemaForSegments(SegmentSchemas segmentSchemas)
  {
    List<SegmentSchemas.SegmentSchema> segmentSchemaList = segmentSchemas.getSegmentSchemaList();

    for (SegmentSchemas.SegmentSchema segmentSchema : segmentSchemaList) {
      String dataSource = segmentSchema.getDataSource();
      SegmentId segmentId = SegmentId.tryParse(dataSource, segmentSchema.getSegmentId());

      if (segmentId == null) {
        log.error("Could not apply schema update. Failed parsing segmentId [%s]", segmentSchema.getSegmentId());
        continue;
      }

      log.debug("Applying schema update for segmentId [%s] datasource [%s]", segmentId, dataSource);

      segmentMetadataInfo.compute(
          dataSource,
          (dataSourceKey, segmentsMap) -> {
            if (segmentsMap == null) {
              segmentsMap = new ConcurrentSkipListMap<>(SEGMENT_ORDER);
            }
            segmentsMap.compute(
                segmentId,
                (id, segmentMetadata) -> {
                  if (segmentMetadata == null) {
                    // By design, this case shouldn't arise since both segment and schema is announced in the same flow
                    // and messages shouldn't be lost in the poll
                    // also segment announcement should always precede schema announcement
                    // and there shouldn't be any schema updated for removed segments
                    log.makeAlert("Schema update [%s] for unknown segment [%s]", segmentSchema, segmentId).emit();
                  } else {
                    // We know this segment.
                    Optional<RowSignature> rowSignature =
                        mergeOrCreateRowSignature(
                            segmentId,
                            segmentMetadata.getRowSignature(),
                            segmentSchema
                        );
                    if (rowSignature.isPresent()) {
                      log.debug(
                          "Segment [%s] signature [%s] after applying schema update.",
                          segmentId,
                          rowSignature.get()
                      );
                      // mark the datasource for rebuilding
                      markDataSourceAsNeedRebuild(dataSource);

                      segmentMetadata = AvailableSegmentMetadata
                          .from(segmentMetadata)
                          .withRowSignature(rowSignature.get())
                          .withNumRows(segmentSchema.getNumRows())
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
   * Merge or create a new RowSignature using the existing RowSignature and schema update.
   */
  @VisibleForTesting
  Optional<RowSignature> mergeOrCreateRowSignature(
      SegmentId segmentId,
      @Nullable RowSignature existingSignature,
      SegmentSchemas.SegmentSchema segmentSchema
  )
  {
    if (!segmentSchema.isDelta()) {
      // absolute schema
      // override the existing signature
      // this case could arise when the server restarts or counter mismatch between client and server
      RowSignature.Builder builder = RowSignature.builder();
      Map<String, ColumnType> columnMapping = segmentSchema.getColumnTypeMap();
      for (String column : segmentSchema.getNewColumns()) {
        builder.add(column, columnMapping.get(column));
      }
      return Optional.of(ROW_SIGNATURE_INTERNER.intern(builder.build()));
    } else if (existingSignature != null) {
      // delta update
      // merge with the existing signature
      RowSignature.Builder builder = RowSignature.builder();
      final Map<String, ColumnType> mergedColumnTypes = new LinkedHashMap<>();

      for (String column : existingSignature.getColumnNames()) {
        final ColumnType columnType =
            existingSignature.getColumnType(column)
                    .orElseThrow(() -> new ISE("Encountered null type for column [%s]", column));

        mergedColumnTypes.put(column, columnType);
      }

      Map<String, ColumnType> columnMapping = segmentSchema.getColumnTypeMap();

      // column type to be updated is not present in the existing schema
      boolean missingUpdateColumns = false;
      // new column to be added is already present in the existing schema
      boolean existingNewColumns = false;

      for (String column : segmentSchema.getUpdatedColumns()) {
        if (!mergedColumnTypes.containsKey(column)) {
          missingUpdateColumns = true;
          mergedColumnTypes.put(column, columnMapping.get(column));
        } else {
          mergedColumnTypes.compute(column, (c, existingType) -> columnTypeMergePolicy.merge(existingType, columnMapping.get(column)));
        }
      }

      for (String column : segmentSchema.getNewColumns()) {
        if (mergedColumnTypes.containsKey(column)) {
          existingNewColumns = true;
          mergedColumnTypes.compute(column, (c, existingType) -> columnTypeMergePolicy.merge(existingType, columnMapping.get(column)));
        } else {
          mergedColumnTypes.put(column, columnMapping.get(column));
        }
      }

      if (missingUpdateColumns || existingNewColumns) {
        log.makeAlert(
            "Error merging delta schema update with existing row signature. segmentId [%s], "
            + "existingSignature [%s], deltaSchema [%s], missingUpdateColumns [%s], existingNewColumns [%s].",
            segmentId,
            existingSignature,
            segmentSchema,
            missingUpdateColumns,
            existingNewColumns
        ).emit();
      }

      mergedColumnTypes.forEach(builder::add);
      return Optional.of(ROW_SIGNATURE_INTERNER.intern(builder.build()));
    } else {
      // delta update
      // we don't have the previous signature, but we received delta update, raise alert
      // this case shouldn't arise by design
      // this can happen if a new segment is added and this is the very first schema update,
      // implying we lost the absolute schema update
      // which implies either the absolute schema update was never computed or lost in polling
      log.makeAlert("Received delta schema update [%s] for a segment [%s] with no previous schema. ",
                    segmentSchema, segmentId
      ).emit();
      return Optional.empty();
    }
  }
}
