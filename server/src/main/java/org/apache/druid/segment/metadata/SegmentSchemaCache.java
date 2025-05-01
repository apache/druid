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
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.SegmentMetadata;
import org.apache.druid.timeline.SegmentId;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory cache of segment schema used by {@link CoordinatorSegmentMetadataCache}.
 * <p>
 * The schema for a given segment ID may be present in one of the following
 * data-structures:
 * <ul>
 * <li>{@link #realtimeSegmentSchemas}: Schema for realtime segments retrieved
 * from realtime tasks</li>
 * <li>{@link #schemasPendingBackfill}: Schema for published segments
 * fetched from data nodes using metadata queries.</li>
 * <li>{@link #recentlyBackfilledSchemas}: Schema for segments recently persisted
 * to the DB. This is needed only to maintain continuity until the next DB poll.</li>
 * <li>{@link #publishedSegmentSchemas}: Schema for used segments as polled from
 * the metadata store.</li>
 * </ul>
 * The cache always contains segment schemas with version
 * {@link CentralizedDatasourceSchemaConfig#SCHEMA_VERSION}.
 */
@LazySingleton
public class SegmentSchemaCache
{
  private static final Logger log = new Logger(SegmentSchemaCache.class);

  /**
   * Cache is marked initialized when the first DB poll finishes after becoming
   * leader.
   */
  private final AtomicReference<CountDownLatch> initialized = new AtomicReference<>(new CountDownLatch(1));

  /**
   * Finalized segment schema information.
   */
  private final AtomicReference<PublishedSegmentSchemas> publishedSegmentSchemas
      = new AtomicReference<>(PublishedSegmentSchemas.EMPTY);

  /**
   * Schema information for realtime segments retrieved by inventory view from
   * tasks. The entry for a segment is removed from this map when the segment is
   * either removed or published.
   */
  private final ConcurrentMap<SegmentId, SchemaPayloadPlus> realtimeSegmentSchemas = new ConcurrentHashMap<>();

  /**
   * Segment schemas fetched from data nodes via segment metadata queries.
   * Once the information is persisted to DB, it is removed from this map.
   */
  private final ConcurrentMap<SegmentId, SchemaPayloadPlus> schemasPendingBackfill = new ConcurrentHashMap<>();

  /**
   * Segment schemas recently persisted to DB via backfill. This map is needed
   * only to keep recently persisted schemas cached until the next DB poll.
   */
  private final ConcurrentMap<SegmentId, SchemaPayloadPlus> recentlyBackfilledSchemas = new ConcurrentHashMap<>();

  /**
   * Number of cache misses since last metric emission period.
   */
  private final AtomicInteger cacheMissCount = new AtomicInteger(0);

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

    publishedSegmentSchemas.set(PublishedSegmentSchemas.EMPTY);
    schemasPendingBackfill.clear();
    recentlyBackfilledSchemas.clear();
  }

  public boolean isInitialized()
  {
    return initialized.get().getCount() == 0;
  }

  /**
   * {@link CoordinatorSegmentMetadataCache} startup waits on the cache initialization.
   * This is being done to ensure that we don't execute metadata query for segment with schema already present in the DB.
   */
  public void awaitInitialization() throws InterruptedException
  {
    initialized.get().await();
  }

  /**
   * Resets the schema in the cache for published (non-realtime) segments.
   * This method is called after each successful poll of used segments and
   * schemas from the metadata store.
   *
   * @param usedSegmentIdToMetadata    Map from used segment ID to corresponding metadata
   * @param schemaFingerprintToPayload Map from schema fingerprint to payload
   */
  public void resetSchemaForPublishedSegments(
      Map<SegmentId, SegmentMetadata> usedSegmentIdToMetadata,
      Map<String, SchemaPayload> schemaFingerprintToPayload
  )
  {
    this.publishedSegmentSchemas.set(
        new PublishedSegmentSchemas(usedSegmentIdToMetadata, schemaFingerprintToPayload)
    );

    // remove metadata for segments which have been polled in the last database poll
    recentlyBackfilledSchemas
        .keySet()
        .removeAll(publishedSegmentSchemas.get().segmentIdToMetadata.keySet());

    setInitialized();
  }

  /**
   * Adds schema for a realtime segment to the cache.
   */
  public void addRealtimeSegmentSchema(SegmentId segmentId, SchemaPayloadPlus schema)
  {
    realtimeSegmentSchemas.put(segmentId, schema);
  }

  /**
   * Adds a temporary schema for the given segment ID to the cache. This schema
   * is typically fetched from data nodes by issuing segment metadata queries.
   * Once this schema is persisted to DB, call {@link #markSchemaPersisted}.
   */
  public void addTemporarySchema(
      SegmentId segmentId,
      SchemaPayloadPlus schema
  )
  {
    schemasPendingBackfill.put(segmentId, schema);
  }

  /**
   * Marks the schema for the given segment ID as persisted to the DB.
   */
  public void markSchemaPersisted(SegmentId segmentId)
  {
    SchemaPayloadPlus segmentSchema = schemasPendingBackfill.get(segmentId);
    if (segmentSchema == null) {
      log.info("SegmentId[%s] has no schema pending backfill.", segmentId);
    } else {
      recentlyBackfilledSchemas.put(segmentId, segmentSchema);
    }

    schemasPendingBackfill.remove(segmentId);
  }

  /**
   * Reads the schema for a given segment ID from the cache.
   * <p>
   * Note that there is no check on schema version in this method, since only
   * schema corresponding to a single schema version is present in the cache at
   * any time. Any change in version requires a service restart and the cache is rebuilt.
   */
  public Optional<SchemaPayloadPlus> getSchemaForSegment(SegmentId segmentId)
  {
    // First look up the schema in the realtime map. This ensures that during handoff
    // there is no window where segment schema is missing from the cache.
    // If were to look up the finalized segment map first, during handoff it is possible
    // that segment schema isn't polled yet and thus missing from the map and by the time
    // we look up the schema in the realtime map, it has been removed.
    SchemaPayloadPlus payloadPlus = realtimeSegmentSchemas.get(segmentId);
    if (payloadPlus != null) {
      return Optional.of(payloadPlus);
    }

    // it is important to lookup temporaryMetadataQueryResults before temporaryPublishedMetadataQueryResults
    // other way round, if a segment schema is just published it is possible that the schema is missing
    // in temporaryPublishedMetadataQueryResults and by the time we check temporaryMetadataQueryResults it is removed.

    // segment schema has been fetched via metadata query
    payloadPlus = schemasPendingBackfill.get(segmentId);
    if (payloadPlus != null) {
      return Optional.of(payloadPlus);
    }

    // segment schema has been fetched via metadata query and the schema has been published to the DB
    payloadPlus = recentlyBackfilledSchemas.get(segmentId);
    if (payloadPlus != null) {
      return Optional.of(payloadPlus);
    }

    // segment schema has been polled from the DB
    SegmentMetadata segmentMetadata = getSegmentMetadataMap().get(segmentId);
    if (segmentMetadata != null) {
      SchemaPayload schemaPayload = getSchemaPayloadMap().get(segmentMetadata.getSchemaFingerprint());
      if (schemaPayload != null) {
        return Optional.of(
            new SchemaPayloadPlus(schemaPayload, segmentMetadata.getNumRows())
        );
      }
    }

    cacheMissCount.incrementAndGet();
    return Optional.empty();
  }

  /**
   * Check if the cache contains schema for the given segment ID.
   */
  public boolean isSchemaCached(SegmentId segmentId)
  {
    return realtimeSegmentSchemas.containsKey(segmentId) ||
           schemasPendingBackfill.containsKey(segmentId) ||
           recentlyBackfilledSchemas.containsKey(segmentId) ||
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

  private Map<SegmentId, SegmentMetadata> getSegmentMetadataMap()
  {
    return publishedSegmentSchemas.get().segmentIdToMetadata;
  }

  private Map<String, SchemaPayload> getSchemaPayloadMap()
  {
    return publishedSegmentSchemas.get().schemaFingerprintToPayload;
  }

  /**
   * Removes schema cached for this segment ID.
   */
  public void segmentRemoved(SegmentId segmentId)
  {
    // remove the segment from all the maps
    realtimeSegmentSchemas.remove(segmentId);
    schemasPendingBackfill.remove(segmentId);
    recentlyBackfilledSchemas.remove(segmentId);

    // Since finalizedSegmentMetadata & finalizedSegmentSchema is updated on each DB poll,
    // there is no need to remove segment from them.
  }

  /**
   * Removes schema for realtime segment.
   */
  public void realtimeSegmentRemoved(SegmentId segmentId)
  {
    realtimeSegmentSchemas.remove(segmentId);
  }

  /**
   * @return Summary stats of the current contents of the cache.
   */
  public Map<String, Integer> getStats()
  {
    return Map.of(
        Metric.CACHE_MISSES, cacheMissCount.getAndSet(0),
        Metric.REALTIME_SEGMENT_SCHEMAS, realtimeSegmentSchemas.size(),
        Metric.USED_SEGMENT_SCHEMAS, getSegmentMetadataMap().size(),
        Metric.USED_SEGMENT_SCHEMA_FINGERPRINTS, getSchemaPayloadMap().size(),
        Metric.SCHEMAS_PENDING_BACKFILL, schemasPendingBackfill.size()
    );
  }

  @VisibleForTesting
  SchemaPayloadPlus getTemporaryPublishedMetadataQueryResults(SegmentId id)
  {
    return recentlyBackfilledSchemas.get(id);
  }

  /**
   * Contains schema information for published segments polled from the DB.
   */
  private static class PublishedSegmentSchemas
  {
    private static final PublishedSegmentSchemas EMPTY = new PublishedSegmentSchemas(Map.of(), Map.of());

    private final Map<SegmentId, SegmentMetadata> segmentIdToMetadata;
    private final Map<String, SchemaPayload> schemaFingerprintToPayload;

    private PublishedSegmentSchemas(
        final Map<SegmentId, SegmentMetadata> segmentIdToMetadata,
        final Map<String, SchemaPayload> schemaFingerprintToPayload
    )
    {
      // Make immutable copies
      this.segmentIdToMetadata = Map.copyOf(segmentIdToMetadata);
      this.schemaFingerprintToPayload = Map.copyOf(schemaFingerprintToPayload);
    }
  }
}
