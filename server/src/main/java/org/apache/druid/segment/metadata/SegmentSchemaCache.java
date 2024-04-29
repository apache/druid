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
 * This mapping is updated on each database poll {@link SegmentSchemaCache#finalizedSegmentSchemaInfo}.
 * Segment schema created since last DB poll is also fetched and updated in the cache {@code finalizedSegmentSchema}.
 * <p>
 * Additionally, this class caches schema for realtime segments in {@link SegmentSchemaCache#realtimeSegmentSchema}. This mapping
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

  /**
   * Cache is marked initialized after first DB poll.
   */
  private final AtomicReference<CountDownLatch> initialized = new AtomicReference<>(new CountDownLatch(1));

  /**
   * Finalized segment schema information.
   */
  private volatile FinalizedSegmentSchemaInfo finalizedSegmentSchemaInfo =
      new FinalizedSegmentSchemaInfo(ImmutableMap.of(), ImmutableMap.of());

  /**
   * Schema information for realtime segment. This mapping is updated when schema for realtime segment is received.
   * The mapping is removed when the segment is either removed or marked as finalized.
   */
  private final ConcurrentMap<SegmentId, SchemaPayloadPlus> realtimeSegmentSchema = new ConcurrentHashMap<>();

  /**
   * If the segment schema is fetched via SMQ, subsequently it is added here.
   * The mapping is removed when the schema information is backfilled in the DB.
   */
  private final ConcurrentMap<SegmentId, SchemaPayloadPlus> inTransitSMQResults = new ConcurrentHashMap<>();

  /**
   * Once the schema information is backfilled in the DB, it is added here.
   * This map is cleared after each DB poll.
   * After the DB poll and before clearing this map it is possible that some results were added to this map.
   * These results would get lost after clearing this map.
   * But, it should be fine since the schema could be retrieved if needed using SMQ, also the schema would be available in the next poll.
   */
  private final ConcurrentMap<SegmentId, SchemaPayloadPlus> inTransitSMQPublishedResults = new ConcurrentHashMap<>();

  private final ServiceEmitter emitter;

  @Inject
  public SegmentSchemaCache(ServiceEmitter emitter)
  {
    this.emitter = emitter;
  }

  public void setInitialized()
  {
    if (!isInitialized()) {
      initialized.get().countDown();
      log.info("SegmentSchemaCache is initialized.");
    }
  }

  /**
   * This method is called when the current node is no longer the leader.
   * The schema is cleared except for {@code realtimeSegmentSchemaMap}.
   * Realtime schema continues to be updated on both the leader and follower nodes.
   */
  public void onLeaderStop()
  {
    initialized.set(new CountDownLatch(1));

    finalizedSegmentSchemaInfo = new FinalizedSegmentSchemaInfo(ImmutableMap.of(), ImmutableMap.of());
    inTransitSMQResults.clear();
    inTransitSMQPublishedResults.clear();
  }

  public boolean isInitialized()
  {
    return initialized.get().getCount() == 0;
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
   * This method is called after each DB Poll. It updates reference for segment metadata and schema maps.
   */
  public void updateFinalizedSegmentSchema(FinalizedSegmentSchemaInfo finalizedSegmentSchemaInfo)
  {
    this.finalizedSegmentSchemaInfo = finalizedSegmentSchemaInfo;
    setInitialized();
  }

  /**
   * Cache schema for realtime segment. This is cleared when segment is published.
   */
  public void addRealtimeSegmentSchema(SegmentId segmentId, RowSignature rowSignature, long numRows)
  {
    realtimeSegmentSchema.put(segmentId, new SchemaPayloadPlus(new SchemaPayload(rowSignature), numRows));
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
    // First look up the schema in the realtime map. This ensures that during handoff
    // there is no window where segment schema is missing from the cache.
    // If were to look up the finalized segment map first, during handoff it is possible
    // that segment schema isn't polled yet and thus missing from the map and by the time
    // we look up the schema in the realtime map, it has been removed.
    SchemaPayloadPlus payloadPlus = realtimeSegmentSchema.get(segmentId);
    if (payloadPlus != null) {
      return Optional.of(payloadPlus);
    }

    // it is important to lookup {@code inTransitSMQResults} before {@code inTransitSMQPublishedResults}
    // other way round, if a segment schema is just published it is possible that the schema is missing
    // in {@code inTransitSMQPublishedResults} and by the time we check {@code inTransitSMQResults} it is removed.

    // segment schema has been fetched via SMQ
    payloadPlus = inTransitSMQResults.get(segmentId);
    if (payloadPlus != null) {
      return Optional.of(payloadPlus);
    }

    // segment schema has been fetched via SMQ and the schema has been published to the DB
    payloadPlus = inTransitSMQPublishedResults.get(segmentId);
    if (payloadPlus != null) {
      return Optional.of(payloadPlus);
    }

    // segment schema has been polled from the DB
    SegmentMetadata segmentMetadata = getSegmentMetadataMap().get(segmentId);
    if (segmentMetadata != null) {
      SchemaPayload schemaPayload = getSchemaPayloadMap().get(segmentMetadata.getSchemaFingerprint());
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
    return realtimeSegmentSchema.containsKey(segmentId) ||
           inTransitSMQResults.containsKey(segmentId) ||
           inTransitSMQPublishedResults.containsKey(segmentId) ||
           isFinalizedSegmentSchemaCached(segmentId);
  }

  private boolean isFinalizedSegmentSchemaCached(SegmentId segmentId)
  {
    SegmentMetadata segmentMetadata = getSegmentMetadataMap().get(segmentId);
    if (segmentMetadata != null) {
      return getSchemaPayloadMap().containsKey(segmentMetadata.getSchemaFingerprint());
    }
    return false;
  }

  private ImmutableMap<SegmentId, SegmentMetadata> getSegmentMetadataMap()
  {
    return finalizedSegmentSchemaInfo.getFinalizedSegmentMetadata();
  }

  private ImmutableMap<String, SchemaPayload> getSchemaPayloadMap()
  {
    return finalizedSegmentSchemaInfo.getFinalizedSegmentSchema();
  }

  /**
   * On segment removal, remove cached schema for the segment.
   */
  public boolean segmentRemoved(SegmentId segmentId)
  {
    // remove the segment from all the maps
    realtimeSegmentSchema.remove(segmentId);
    inTransitSMQResults.remove(segmentId);
    inTransitSMQPublishedResults.remove(segmentId);

    // Since finalizedSegmentMetadata & finalizedSegmentSchema is updated on each DB poll,
    // there is no need to remove segment from them.
    return true;
  }

  /**
   * Remove schema for realtime segment.
   */
  public void realtimeSegmentRemoved(SegmentId segmentId)
  {
    realtimeSegmentSchema.remove(segmentId);
  }

  public void emitStats()
  {
    emitter.emit(ServiceMetricEvent.builder().setMetric("schemacache/realtime/count", realtimeSegmentSchema.size()));
    emitter.emit(ServiceMetricEvent.builder().setMetric("schemacache/finalizedSegmentMetadata/count", getSegmentMetadataMap().size()));
    emitter.emit(ServiceMetricEvent.builder().setMetric("schemacache/finalizedSchemaPayload/count", getSchemaPayloadMap().size()));
    emitter.emit(ServiceMetricEvent.builder().setMetric("schemacache/inTransitSMQResults/count", inTransitSMQResults.size()));
    emitter.emit(ServiceMetricEvent.builder().setMetric("schemacache/inTransitSMQPublishedResults/count", inTransitSMQPublishedResults.size()));
  }

  /**
   * This class encapsulates schema information for segments polled from the DB.
   */
  public static class FinalizedSegmentSchemaInfo
  {
    /**
     * Mapping from segmentId to segment level information which includes numRows and schemaFingerprint.
     * This mapping is updated on each database poll.
     */
    private final ImmutableMap<SegmentId, SegmentMetadata> finalizedSegmentMetadata;

    /**
     * Mapping from schemaFingerprint to payload.
     */
    private final ImmutableMap<String, SchemaPayload> finalizedSegmentSchema;

    public FinalizedSegmentSchemaInfo(
        final ImmutableMap<SegmentId, SegmentMetadata> finalizedSegmentMetadata,
        final ImmutableMap<String, SchemaPayload> finalizedSegmentSchema
    )
    {
      this.finalizedSegmentMetadata = finalizedSegmentMetadata;
      this.finalizedSegmentSchema = finalizedSegmentSchema;
    }

    public ImmutableMap<SegmentId, SegmentMetadata> getFinalizedSegmentMetadata()
    {
      return finalizedSegmentMetadata;
    }

    public ImmutableMap<String, SchemaPayload> getFinalizedSegmentSchema()
    {
      return finalizedSegmentSchema;
    }
  }
}
