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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.ServerView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.segment.SchemaPayloadPlus;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coordinator-side cache of segment metadata that combines segments to build
 * datasources. The cache provides metadata about a datasource, see {@link DataSourceInformation}.
 * <p>
 * Major differences from the other implementation {@code BrokerSegmentMetadataCache} are,
 * <li>The refresh is executed only on the leader Coordinator node.</li>
 * <li>Realtime segment schema refresh. Schema update for realtime segment is pushed periodically.
 * The schema is merged with any existing schema for the segment and the cache is updated.
 * Corresponding datasource is marked for refresh.</li>
 * <li>The refresh mechanism is significantly different from the other implementation,
 * <ul><li>SMQ is executed only for those non-realtime segments for which the schema is not cached.</li>
 * <li>Datasources marked for refresh are then rebuilt.</li></ul>
 * </li>
 */
@ManageLifecycle
public class CoordinatorSegmentMetadataCache extends AbstractSegmentMetadataCache<DataSourceInformation>
{
  private static final EmittingLogger log = new EmittingLogger(CoordinatorSegmentMetadataCache.class);

  private final SegmentMetadataCacheConfig config;
  private final ColumnTypeMergePolicy columnTypeMergePolicy;
  private final SegmentSchemaCache segmentSchemaCache;
  private final SegmentSchemaBackFillQueue segmentSchemaBackfillQueue;
  private @Nullable Future<?> cacheExecFuture = null;

  @Inject
  public CoordinatorSegmentMetadataCache(
      QueryLifecycleFactory queryLifecycleFactory,
      CoordinatorServerView serverView,
      SegmentMetadataCacheConfig config,
      Escalator escalator,
      InternalQueryConfig internalQueryConfig,
      ServiceEmitter emitter,
      SegmentSchemaCache segmentSchemaCache,
      SegmentSchemaBackFillQueue segmentSchemaBackfillQueue
  )
  {
    super(queryLifecycleFactory, config, escalator, internalQueryConfig, emitter);
    this.config = config;
    this.columnTypeMergePolicy = config.getMetadataColumnTypeMergePolicy();
    this.segmentSchemaCache = segmentSchemaCache;
    this.segmentSchemaBackfillQueue = segmentSchemaBackfillQueue;

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
            updateSchemaForRealtimeSegments(segmentSchemas);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  @LifecycleStart
  @Override
  public void start()
  {
    // noop, refresh is started only on leader node
  }

  @LifecycleStop
  @Override
  public void stop()
  {
    callbackExec.shutdownNow();
    cacheExec.shutdownNow();
    segmentSchemaCache.onLeaderStop();
    segmentSchemaBackfillQueue.onLeaderStop();
    if (cacheExecFuture != null) {
      cacheExecFuture.cancel(true);
    }
  }

  public void onLeaderStart()
  {
    log.info("Initializing cache on leader node.");
    try {
      segmentSchemaBackfillQueue.onLeaderStart();
      cacheExecFuture = cacheExec.submit(this::cacheExecLoop);
      if (config.isAwaitInitializationOnStart()) {
        awaitInitialization();
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void onLeaderStop()
  {
    log.info("No longer leader, stopping cache.");
    if (cacheExecFuture != null) {
      cacheExecFuture.cancel(true);
    }
    segmentSchemaCache.onLeaderStop();
    segmentSchemaBackfillQueue.onLeaderStop();
  }

  /**
   * This method ensures that the refresh goes through only when schemaCache is initialized.
   */
  @Override
  public synchronized void refreshWaitCondition() throws InterruptedException
  {
    segmentSchemaCache.awaitInitialization();
  }

  @Override
  protected void unmarkSegmentAsMutable(SegmentId segmentId)
  {
    synchronized (lock) {
      log.debug("SegmentId [%s] is marked as finalized.", segmentId);
      mutableSegments.remove(segmentId);
      // remove it from the realtime schema cache
      segmentSchemaCache.realtimeSegmentRemoved(segmentId);
    }
  }

  @Override
  protected void removeSegmentAction(SegmentId segmentId)
  {
    log.debug("SegmentId [%s] is removed.", segmentId);
    segmentSchemaCache.segmentRemoved(segmentId);
  }

  @Override
  protected boolean fetchAggregatorsInSegmentMetadataQuery()
  {
    return true;
  }

  @Override
  protected boolean segmentMetadataQueryResultHandler(
      String dataSource,
      SegmentId segmentId,
      RowSignature rowSignature,
      SegmentAnalysis analysis
  )
  {
    AtomicBoolean added = new AtomicBoolean(false);
    segmentMetadataInfo.compute(
        dataSource,
        (datasourceKey, dataSourceSegments) -> {
          if (dataSourceSegments == null) {
            // Datasource may have been removed or become unavailable while this refresh was ongoing.
            log.warn(
                "No segment map found with datasource [%s], skipping refresh of segment [%s]",
                datasourceKey,
                segmentId
            );
            return null;
          } else {
            dataSourceSegments.compute(
                segmentId,
                (segmentIdKey, segmentMetadata) -> {
                  if (segmentMetadata == null) {
                    log.warn("No segment [%s] found, skipping refresh", segmentId);
                    return null;
                  } else {
                    long numRows = analysis.getNumRows();
                    log.debug("Publishing segment schema. SegmentId [%s], RowSignature [%s], numRows [%d]", segmentId, rowSignature, numRows);
                    Map<String, AggregatorFactory> aggregators = analysis.getAggregators();
                    // cache the signature
                    segmentSchemaCache.addInTransitSMQResult(segmentId, rowSignature, aggregators, numRows);
                    // queue the schema for publishing to the DB
                    segmentSchemaBackfillQueue.add(segmentId, rowSignature, aggregators, numRows);
                    added.set(true);
                    return segmentMetadata;
                  }
                }
            );

            if (dataSourceSegments.isEmpty()) {
              return null;
            } else {
              return dataSourceSegments;
            }
          }
        }
    );

    return added.get();
  }

  @Override
  public Map<SegmentId, AvailableSegmentMetadata> getSegmentMetadataSnapshot()
  {
    final Map<SegmentId, AvailableSegmentMetadata> segmentMetadata = Maps.newHashMapWithExpectedSize(getTotalSegments());
    for (ConcurrentSkipListMap<SegmentId, AvailableSegmentMetadata> val : segmentMetadataInfo.values()) {
      for (Map.Entry<SegmentId, AvailableSegmentMetadata> entry : val.entrySet()) {
        Optional<SchemaPayloadPlus> metadata = segmentSchemaCache.getSchemaForSegment(entry.getKey());
        AvailableSegmentMetadata availableSegmentMetadata = entry.getValue();
        if (metadata.isPresent()) {
          availableSegmentMetadata = AvailableSegmentMetadata.from(entry.getValue())
                                           .withRowSignature(metadata.get().getSchemaPayload().getRowSignature())
                                           .withNumRows(metadata.get().getNumRows())
                                           .build();
        } else {
          // mark it for refresh, however, this case shouldn't arise by design
          markSegmentAsNeedRefresh(entry.getKey());
          log.debug("SchemaMetadata for segmentId [%s] is absent.", entry.getKey());
        }
        segmentMetadata.put(entry.getKey(), availableSegmentMetadata);
      }
    }
    return segmentMetadata;
  }

  @Nullable
  @Override
  public AvailableSegmentMetadata getAvailableSegmentMetadata(String datasource, SegmentId segmentId)
  {
    ConcurrentSkipListMap<SegmentId, AvailableSegmentMetadata> segmentMap = segmentMetadataInfo.get(datasource);
    AvailableSegmentMetadata availableSegmentMetadata = null;
    if (segmentMap != null) {
      availableSegmentMetadata = segmentMap.get(segmentId);
    }
    if (availableSegmentMetadata == null) {
      return null;
    }
    Optional<SchemaPayloadPlus> metadata = segmentSchemaCache.getSchemaForSegment(segmentId);
    if (metadata.isPresent()) {
      availableSegmentMetadata = AvailableSegmentMetadata.from(availableSegmentMetadata)
                                       .withRowSignature(metadata.get().getSchemaPayload().getRowSignature())
                                       .withNumRows(metadata.get().getNumRows())
                                       .build();
    } else {
      // mark it for refresh, however, this case shouldn't arise by design
      markSegmentAsNeedRefresh(segmentId);
      log.debug("SchemaMetadata for segmentId [%s] is absent.", segmentId);
    }
    return availableSegmentMetadata;
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
    log.debug("Segments to refresh [%s], dataSourcesToRebuild [%s]", segmentsToRefresh, dataSourcesToRebuild);

    filterRealtimeSegments(segmentsToRefresh);

    log.debug("SegmentsToRefreshMinusRealtimeSegments [%s]", segmentsToRefresh);

    final Set<SegmentId> cachedSegments = filterSegmentWithCachedSchema(segmentsToRefresh);

    log.debug("SegmentsToRefreshMinusCachedSegments [%s], cachedSegments [%s]", segmentsToRefresh, cachedSegments);

    // Refresh the segments.
    Set<SegmentId> refreshed = Collections.emptySet();

    if (!config.isDisableSegmentMetadataQueries()) {
      refreshed = refreshSegments(segmentsToRefresh);
      log.debug("Refreshed segments are [%s]", refreshed);
    }

    synchronized (lock) {
      // Add missing segments back to the refresh list.
      segmentsNeedingRefresh.addAll(Sets.difference(segmentsToRefresh, refreshed));

      // Compute the list of datasources to rebuild tables for.
      dataSourcesToRebuild.addAll(dataSourcesNeedingRebuild);

      refreshed.forEach(segment -> dataSourcesToRebuild.add(segment.getDataSource()));
      cachedSegments.forEach(segment -> dataSourcesToRebuild.add(segment.getDataSource()));

      dataSourcesNeedingRebuild.clear();
    }

    log.debug("Datasources to rebuild are [%s]", dataSourcesToRebuild);
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

  private void filterRealtimeSegments(Set<SegmentId> segmentIds)
  {
    synchronized (lock) {
      segmentIds.removeAll(mutableSegments);
    }
  }

  private Set<SegmentId> filterSegmentWithCachedSchema(Set<SegmentId> segmentIds)
  {
    Set<SegmentId> cachedSegments = new HashSet<>();
    for (SegmentId id : segmentIds) {
      if (segmentSchemaCache.isSchemaCached(id)) {
        cachedSegments.add(id);
      }
    }

    segmentIds.removeAll(cachedSegments);
    return cachedSegments;
  }

  @VisibleForTesting
  @Nullable
  @Override
  public RowSignature buildDataSourceRowSignature(final String dataSource)
  {
    ConcurrentSkipListMap<SegmentId, AvailableSegmentMetadata> segmentsMap = segmentMetadataInfo.get(dataSource);

    // Preserve order.
    final Map<String, ColumnType> columnTypes = new LinkedHashMap<>();

    if (segmentsMap != null && !segmentsMap.isEmpty()) {
      for (SegmentId segmentId : segmentsMap.keySet()) {
        Optional<SchemaPayloadPlus> optionalSchema = segmentSchemaCache.getSchemaForSegment(segmentId);
        if (optionalSchema.isPresent()) {
          RowSignature rowSignature = optionalSchema.get().getSchemaPayload().getRowSignature();
          for (String column : rowSignature.getColumnNames()) {
            final ColumnType columnType =
                rowSignature.getColumnType(column)
                            .orElseThrow(() -> new ISE("Encountered null type for column [%s]", column));

            columnTypes.compute(column, (c, existingType) -> columnTypeMergePolicy.merge(existingType, columnType));
          }
        } else {
          // mark it for refresh, however, this case shouldn't arise by design
          markSegmentAsNeedRefresh(segmentId);
          log.debug("SchemaMetadata for segmentId [%s] is absent.", segmentId);
        }
      }
    } else {
      // table has no segments
      return null;
    }

    final RowSignature.Builder builder = RowSignature.builder();
    columnTypes.forEach(builder::add);

    return builder.build();
  }

  /**
   * Update schema for segments.
   */
  @VisibleForTesting
  void updateSchemaForRealtimeSegments(SegmentSchemas segmentSchemas)
  {
    log.debug("SchemaUpdate for realtime segments [%s].", segmentSchemas);

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
              // Datasource may have been removed or become unavailable while this refresh was ongoing.
              log.warn(
                  "No segment map found with datasource [%s], skipping refresh of segment [%s]",
                  dataSourceKey,
                  segmentId
              );
              return null;
            } else {
              segmentsMap.compute(
                  segmentId,
                  (id, segmentMetadata) -> {
                    if (segmentMetadata == null) {
                      // By design, this case shouldn't arise since both segment and schema is announced in the same flow
                      // and messages shouldn't be lost in the poll
                      // also segment announcement should always precede schema announcement
                      // and there shouldn't be any schema updates for removed segments
                      log.makeAlert("Schema update [%s] for unknown segment [%s]", segmentSchema, segmentId).emit();
                    } else {
                      // We know this segment.
                      Optional<SchemaPayloadPlus> schemaMetadata = segmentSchemaCache.getSchemaForSegment(segmentId);

                      Optional<RowSignature> rowSignature =
                          mergeOrCreateRowSignature(
                              segmentId,
                              schemaMetadata.map(
                                  segmentSchemaMetadata -> segmentSchemaMetadata.getSchemaPayload().getRowSignature())
                                            .orElse(null),
                              segmentSchema
                          );
                      if (rowSignature.isPresent()) {
                        log.debug(
                            "Segment [%s] signature [%s] after applying schema update.",
                            segmentId,
                            rowSignature.get()
                        );
                        segmentSchemaCache.addRealtimeSegmentSchema(segmentId, rowSignature.get(), segmentSchema.getNumRows());

                        // mark the datasource for rebuilding
                        markDataSourceAsNeedRebuild(dataSource);
                      }
                    }
                    return segmentMetadata;
                  }
              );
              return segmentsMap;
            }
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
