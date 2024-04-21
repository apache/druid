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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.SegmentMetadata;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory cache of segment schema.
 * <p>
 * Internally, mapping of segmentId to segment level information like schemaId & numRows is maintained.
 * This mapping is updated on each database poll {@link SegmentSchemaCache#finalizedSegmentMetadata}.
 * Segment schema created since last DB poll is also fetched and updated in the cache {@code finalizedSegmentSchema}.
 * <p>
 * Additionally, this class caches schema for realtime segments in {@link SegmentSchemaCache#realtimeSegmentSchemaMap}. This mapping
 * is cleared either when the segment is removed or marked as finalized.
 * <p>
 * Finalized segments which do not have their schema information present in the DB, fetch their schema via SMQ.
 * SMQ results are cached in {@link SegmentSchemaCache#inTransitSMQResults}. Once the schema information is backfilled
 * in the DB, it is removed from {@link SegmentSchemaCache#inTransitSMQResults} and added to {@link SegmentSchemaCache#inTransitSMQPublishedResults}.
 * {@link SegmentSchemaCache#inTransitSMQPublishedResults} is cleared on each successfull DB poll.
 * <p>
 * {@link CoordinatorSegmentMetadataCache} uses this cache to fetch schema for a segment.
 * <p>
 * Schema corresponding to the specified version in {@link CentralizedDatasourceSchemaConfig#SCHEMA_VERSION} is cached.
 */
@LazySingleton
public class SegmentSchemaCache
{
  private static final Logger log = new Logger(SegmentSchemaCache.class);

  // Cache is marked initialized after first DB poll.
  private final AtomicReference<CountDownLatch> initialized = new AtomicReference<>(new CountDownLatch(1));

  /**
   * Mapping from segmentId to segment level information which includes numRows and schemaId.
   * This mapping is updated on each database poll.
   */
  private volatile ImmutableMap<SegmentId, SegmentMetadata> finalizedSegmentMetadata = ImmutableMap.of();

  /**
   * Mapping from schemaId to payload. Gets updated after DB poll.
   */
  private volatile ImmutableMap<String, SchemaPayload> finalizedSegmentSchema = ImmutableMap.of();

  /**
   * Schema information for realtime segment. This mapping is updated when schema for realtime segment is received.
   * The mapping is removed when the segment is either removed or marked as finalized.
   */
  private final ConcurrentMap<SegmentId, SchemaPayloadPlus> realtimeSegmentSchemaMap = new ConcurrentHashMap<>();

  /**
   * If the segment schema is fetched via SMQ, subsequently it is added here.
   * The mapping is removed when the schema information is backfilled in the DB.
   */
  private final ConcurrentMap<SegmentId, SchemaPayloadPlus> inTransitSMQResults = new ConcurrentHashMap<>();

  private final ServiceEmitter emitter;

  @Inject
  public SegmentSchemaCache(ServiceEmitter emitter)
  {
    this.emitter = emitter;
  }

  /**
   * Once the schema information is backfilled in the DB, it is added here.
   * This map is cleared after each DB poll.
   * After the DB poll and before clearing this map it is possible that some results were added to this map.
   * These results would get lost after clearing this map.
   * But, it should be fine since the schema could be retrieved if needed using SMQ, also the schema would be available in the next poll.
   */
  private final ConcurrentMap<SegmentId, SchemaPayloadPlus> inTransitSMQPublishedResults = new ConcurrentHashMap<>();

  public void setInitialized()
  {
    log.info("[%s] initializing.", getClass().getSimpleName());
    if (initialized.get().getCount() == 1) {
      initialized.get().countDown();
      log.info("[%s] is initialized.", getClass().getSimpleName());
    }
  }

  /**
   * Uninitialize is called when the current node is no longer the leader.
   * The schema is cleared except for {@code realtimeSegmentSchemaMap}.
   * Schema map continues to be updated on both the leader and follower nodes.
   */
  public void uninitialize()
  {
    log.info("[%s] is uninitializing.", getClass().getSimpleName());
    initialized.set(new CountDownLatch(1));

    finalizedSegmentMetadata = ImmutableMap.of();
    finalizedSegmentSchema = ImmutableMap.of();
    inTransitSMQResults.clear();
    inTransitSMQPublishedResults.clear();
  }

  /**
   * {@link CoordinatorSegmentMetadataCache} startup waits on the cache initialization.
   * This is being done to ensure that we don't execute SMQ for segment with schema already present in the DB.
   */
  public void awaitInitialization() throws InterruptedException
  {
    initialized.get().await();
  }

  /**
   * This method is called after each DB poll.
   */
  public void updateFinalizedSegmentMetadataReference(ImmutableMap<SegmentId, SegmentMetadata> segmentStatsMap)
  {
    this.finalizedSegmentMetadata = segmentStatsMap;
  }

  /**
   * This method is called on full schema refresh from the DB.
   */
  public void updateFinalizedSegmentSchemaReference(ImmutableMap<String, SchemaPayload> schemaPayloadMap)
  {
    finalizedSegmentSchema = schemaPayloadMap;
  }

  /**
   * Cache schema for realtime segment. This is cleared when segment is published.
   */
  public void addRealtimeSegmentSchema(SegmentId segmentId, RowSignature rowSignature, long numRows)
  {
    realtimeSegmentSchemaMap.put(segmentId, new SchemaPayloadPlus(new SchemaPayload(rowSignature), numRows));
  }

  /**
   * Cache SMQ result. This entry is cleared when SMQ result is published to the DB.
   */
  public void addInTransitSMQResult(
      SegmentId segmentId,
      RowSignature rowSignature,
      Map<String, AggregatorFactory> aggregatorFactories,
      long numRows
  )
  {
    inTransitSMQResults.put(segmentId, new SchemaPayloadPlus(new SchemaPayload(rowSignature, aggregatorFactories), numRows));
  }

  /**
   * After, SMQ result is published to the DB, it is removed from the {@code inTransitSMQResults}
   * and added to {@code inTransitSMQPublishedResults}.
   */
  public void markInTransitSMQResultPublished(SegmentId segmentId)
  {
    if (!inTransitSMQResults.containsKey(segmentId)) {
      log.error("SegmentId [%s] not found in InTransitSMQResultPublished map.", segmentId);
    }

    inTransitSMQPublishedResults.put(segmentId, inTransitSMQResults.get(segmentId));
    inTransitSMQResults.remove(segmentId);
  }

  /**
   * {@code inTransitSMQPublishedResults} is reset on each DB poll.
   */
  public void resetInTransitSMQResultPublishedOnDBPoll()
  {
    inTransitSMQPublishedResults.clear();
  }

  /**
   * Fetch schema for a given segment. Note, since schema corresponding to the current schema version in
   * {@link CentralizedDatasourceSchemaConfig#SCHEMA_VERSION} is cached, there is no check on version here.
   * Any change in version would require a service restart, so we will never end up with multi version schema.
   */
  public Optional<SchemaPayloadPlus> getSchemaForSegment(SegmentId segmentId)
  {
    // We first look up the schema in the realtime map. This ensures that during handoff
    // there is no window where segment schema is missing from the cache.
    // If were to look up the finalized segment map first, during handoff it is possible
    // that segment schema isn't polled yet and thus missing from the map and by the time
    // we look up the schema in the realtime map, it has been removed.
    if (realtimeSegmentSchemaMap.containsKey(segmentId)) {
      return Optional.of(realtimeSegmentSchemaMap.get(segmentId));
    }

    // it is important to lookup {@code inTransitSMQResults} before {@code inTransitSMQPublishedResults}
    // other way round, if a segment schema is just published it is possible that the schema is missing
    // in {@code inTransitSMQPublishedResults} and by the time we check {@code inTransitSMQResults} it is removed.

    // segment schema has been fetched via SMQ
    if (inTransitSMQResults.containsKey(segmentId)) {
      return Optional.of(inTransitSMQResults.get(segmentId));
    }

    // segment schema has been fetched via SMQ and the schema has been published to the DB
    if (inTransitSMQPublishedResults.containsKey(segmentId)) {
      return Optional.of(inTransitSMQPublishedResults.get(segmentId));
    }

    SegmentMetadata segmentMetadata = finalizedSegmentMetadata.get(segmentId);
    // segment schema has been polled from the DB
    if (segmentMetadata != null) {
      SchemaPayload schemaPayload = finalizedSegmentSchema.get(segmentMetadata.getSchemaFingerprint());
      if (schemaPayload != null) {
        return Optional.of(
            new SchemaPayloadPlus(
                schemaPayload,
                segmentMetadata.getNumRows()
            )
        );
      }
    }

    return Optional.empty();
  }

  /**
   * Check if the schema is cached.
   */
  public boolean isSchemaCached(SegmentId segmentId)
  {
    return realtimeSegmentSchemaMap.containsKey(segmentId) ||
           inTransitSMQResults.containsKey(segmentId) ||
           inTransitSMQPublishedResults.containsKey(segmentId) ||
           isFinalizedSegmentSchemaCached(segmentId);
  }

  private boolean isFinalizedSegmentSchemaCached(SegmentId segmentId)
  {
    SegmentMetadata segmentMetadata = finalizedSegmentMetadata.get(segmentId);
    if (segmentMetadata != null) {
      return finalizedSegmentSchema.containsKey(segmentMetadata.getSchemaFingerprint());
    }
    return false;
  }

  /**
   * On segment removal, remove cached schema for the segment.
   */
  public boolean segmentRemoved(SegmentId segmentId)
  {
    if (!isSchemaCached(segmentId)) {
      return false;
    }
    // remove the segment from all the maps
    realtimeSegmentSchemaMap.remove(segmentId);
    inTransitSMQResults.remove(segmentId);
    inTransitSMQPublishedResults.remove(segmentId);

    // Stale schema payload from finalizedSegmentSchema is not removed
    // as it could be referenced by other segments.
    // Stale schema from the cache would get cleared on full schema refresh from the DB,
    // which would be triggered after any stale schema is deleted from the DB.
    // Also, segment is not cleared from finalizedSegmentStats since it updated on each DB poll.
    return true;
  }

  /**
   * Remove schema for realtime segment.
   */
  public boolean realtimeSegmentRemoved(SegmentId segmentId)
  {
    return realtimeSegmentSchemaMap.remove(segmentId) != null;
  }

  public void emitStats()
  {
    emitter.emit(ServiceMetricEvent.builder().setMetric("schemacache/realtime/size", realtimeSegmentSchemaMap.size()));
    emitter.emit(ServiceMetricEvent.builder().setMetric("schemacache/finalizedSegmentMetadata/size", finalizedSegmentMetadata.size()));
    emitter.emit(ServiceMetricEvent.builder().setMetric("schemacache/finalizedSchemaPayload/size", finalizedSegmentSchema.size()));
    emitter.emit(ServiceMetricEvent.builder().setMetric("schemacache/inTransitSMQResults/size", inTransitSMQResults.size()));
    emitter.emit(ServiceMetricEvent.builder().setMetric("schemacache/inTransitSMQPublishedResults/size", inTransitSMQPublishedResults.size()));
  }
}
