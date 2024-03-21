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

import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SegmentSchemaMetadata;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

/**
 * In-memory cache of segment schema.
 * <p>
 * Internally, mapping of segmentId to segment level information like schemaId & numRows is maintained.
 * This mapping is updated on each database poll {@code finalizedSegmentStats}.
 * Segment schema created since last DB poll is also fetched and updated in the cache {@code finalizedSegmentSchema}.
 * <p>
 * Additionally, this class caches schema for realtime segments in {@code realtimeSegmentSchemaMap}. This mapping
 * is cleared either when the segment is removed or marked as finalized.
 * <p>
 * Finalized segments which do not have their schema information present in the DB, fetch their schema via SMQ.
 * SMQ results are cached in {@code inTransitSMQResults}. Once the schema information is backfilled
 * in the DB, it is removed from {@code inTransitSMQResults} and added to {@code inTransitSMQPublishedResults}.
 * {@code inTransitSMQPublishedResults} is cleared on each successfull DB poll.
 */
@LazySingleton
public class SegmentSchemaCache
{
  private static final EmittingLogger log = new EmittingLogger(SegmentSchemaCache.class);

  // Cache is marked initialized after first DB poll.
  private CountDownLatch initialized = new CountDownLatch(1);

  /**
   * Mapping from segmentId to segment level information which includes numRows and schemaId.
   * This mapping is updated on each database poll.
   */
  private volatile ConcurrentMap<SegmentId, SegmentStats> finalizedSegmentStats = new ConcurrentHashMap<>();

  /**
   * Mapping from schemaId to payload. Gets updated after DB poll.
   */
  private volatile ConcurrentMap<Long, SchemaPayload> finalizedSegmentSchema = new ConcurrentHashMap<>();

  /**
   * Schema information for realtime segment. This mapping is updated when schema for realtime segment is received.
   * The mapping is removed when the segment is either removed or marked as finalized.
   */
  private final ConcurrentMap<SegmentId, SegmentSchemaMetadata> realtimeSegmentSchemaMap = new ConcurrentHashMap<>();

  /**
   * If the segment schema is fetched via SMQ, subsequently it is added here.
   * The mapping is removed when the schema information is backfilled in the DB.
   */
  private final ConcurrentMap<SegmentId, SegmentSchemaMetadata> inTransitSMQResults = new ConcurrentHashMap<>();

  /**
   * Once the schema information is backfilled in the DB, it is added here.
   * This map is cleared after each DB poll.
   */
  private volatile ConcurrentMap<SegmentId, SegmentSchemaMetadata> inTransitSMQPublishedResults = new ConcurrentHashMap<>();

  public void setInitialized()
  {
    initialized.countDown();
    log.info("SegmentSchemaCache is initialized.");
  }

  public void uninitialize()
  {
    initialized = new CountDownLatch(1);
    log.info("SegmentSchemaCache is uninitialized.");
  }

  public void awaitInitialization() throws InterruptedException
  {
    initialized.await();
  }

  public void updateFinalizedSegmentStatsReference(ConcurrentMap<SegmentId, SegmentStats> segmentStatsMap)
  {
    log.debug("SegmentStatsMap is [%s].", segmentStatsMap);
    this.finalizedSegmentStats = segmentStatsMap;
  }

  public void addFinalizedSegmentSchema(long schemaId, SchemaPayload schemaPayload)
  {
    finalizedSegmentSchema.put(schemaId, schemaPayload);
  }

  public void updateFinalizedSegmentSchemaReference(ConcurrentMap<Long, SchemaPayload> schemaPayloadMap)
  {
    finalizedSegmentSchema = schemaPayloadMap;
  }

  public void addRealtimeSegmentSchema(SegmentId segmentId, RowSignature rowSignature, long numRows)
  {
    realtimeSegmentSchemaMap.put(segmentId, new SegmentSchemaMetadata(new SchemaPayload(rowSignature), numRows));
  }

  public void addInTransitSMQResult(SegmentId segmentId, RowSignature rowSignature, long numRows)
  {
    inTransitSMQResults.put(segmentId, new SegmentSchemaMetadata(new SchemaPayload(rowSignature), numRows));
  }

  /**
   * When the SMQ result is published to the DB, it is removed from the {@code inTransitSMQResults}
   * and added to {@code inTransitSMQPublishedResults}.
   */
  public void markInTransitSMQResultPublished(SegmentId segmentId)
  {
    if (!inTransitSMQResults.containsKey(segmentId)) {
      log.error("SegmentId [%s] not found in InTransitSMQResultPublished map.", segmentId);
      return;
    }

    inTransitSMQPublishedResults.put(segmentId, inTransitSMQResults.get(segmentId));
    inTransitSMQResults.remove(segmentId);
  }

  /**
   * {@code inTransitSMQPublishedResults} is reset on each DB poll.
   */
  public void resetInTransitSMQResultPublishedOnDBPoll()
  {
    inTransitSMQPublishedResults = new ConcurrentHashMap<>();
  }

  public Optional<SegmentSchemaMetadata> getSchemaForSegment(SegmentId segmentId)
  {
    // realtime segment
    if (realtimeSegmentSchemaMap.containsKey(segmentId)) {
      return Optional.of(realtimeSegmentSchemaMap.get(segmentId));
    }

    // segment schema has been fetched via SMQ
    if (inTransitSMQResults.containsKey(segmentId)) {
      return Optional.of(inTransitSMQResults.get(segmentId));
    }

    // segment schema has been fetched via SMQ and the schema has been published to the DB
    if (inTransitSMQPublishedResults.containsKey(segmentId)) {
      return Optional.of(inTransitSMQPublishedResults.get(segmentId));
    }

    // segment schema has been polled from the DB
    if (finalizedSegmentStats.containsKey(segmentId)) {
      SegmentStats segmentStats = finalizedSegmentStats.get(segmentId);
      Long schemaId = segmentStats.getSchemaId();
      if (schemaId == null) {
        log.error("SchemaId for segment [%s] is null.", segmentId);
      }

      if (segmentStats.getNumRows() == null) {
        log.error("NumRows for segment [%s] is null.", segmentId);
      }

      if (schemaId != null && finalizedSegmentSchema.containsKey(schemaId)) {
        return Optional.of(
            new SegmentSchemaMetadata(
                finalizedSegmentSchema.get(schemaId),
                segmentStats.getNumRows() == null ? 0 : segmentStats.getNumRows()
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
    if (finalizedSegmentStats.containsKey(segmentId)) {
      Long schemaId = finalizedSegmentStats.get(segmentId).getSchemaId();
      if (schemaId != null && finalizedSegmentSchema.containsKey(schemaId)) {
        return true;
      }
    }

    return realtimeSegmentSchemaMap.containsKey(segmentId) ||
           inTransitSMQResults.containsKey(segmentId) ||
           inTransitSMQPublishedResults.containsKey(segmentId);
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

    // Stale schema payload from {@code finalizedSegmentSchema} is not removed
    // as it could be referenced by other segments.
    // Stale schema from the cache would get cleared on full schema refresh from the DB,
    // which would be triggered after any stale schema is deleted from the DB.
    finalizedSegmentStats.remove(segmentId);
    return true;
  }

  /**
   * Remove schema for realtime segment.
   */
  public boolean realtimeSegmentRemoved(SegmentId segmentId)
  {
    return realtimeSegmentSchemaMap.remove(segmentId) != null;
  }

  /**
   * Encapsulates segment level information like numRows, schemaId.
   */
  public static class SegmentStats
  {
    @Nullable
    private final Long schemaId;
    @Nullable
    private final Long numRows;

    public SegmentStats(
        @Nullable Long schemaId,
        @Nullable Long numRows
    )
    {
      this.schemaId = schemaId;
      this.numRows = numRows;
    }

    public Long getSchemaId()
    {
      return schemaId;
    }

    public Long getNumRows()
    {
      return numRows;
    }

    @Override
    public String toString()
    {
      return "SegmentStats{" +
             "schemaId=" + schemaId +
             ", numRows=" + numRows +
             '}';
    }
  }
}
