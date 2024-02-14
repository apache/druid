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
 *
 */
@LazySingleton
public class SegmentSchemaCache
{
  private static final EmittingLogger log = new EmittingLogger(SchemaManager.class);
  private final CountDownLatch initialized = new CountDownLatch(1);

  // Mapping from segmentId to segmentStats, reference is updated on each database poll.
  // edge case what happens if first this map is build from db
  // next new schema is fetch
  // in between
  private volatile ConcurrentMap<SegmentId, SegmentStats> finalizedSegmentStats = new ConcurrentHashMap<>();

  // Mapping from schemaId to schema fingerprint & schema fingerprint to schema payload
  // On each database poll, fetch newly added schema since last poll.
  // Coordinator schema cleanup duty, removes orphan schema in the same transaction also remove schema from here
  // if transaction failed revert stuff by storing in temporary structure
  private volatile ConcurrentMap<Long, SchemaPayload> finalizedSegmentSchema = new ConcurrentHashMap<>();

  private final ConcurrentMap<SegmentId, SegmentSchemaMetadata> realtimeSegmentSchemaMap = new ConcurrentHashMap<>();

  private final ConcurrentMap<SegmentId, SegmentSchemaMetadata> inTransitSMQResults = new ConcurrentHashMap<>();
  private volatile ConcurrentMap<SegmentId, SegmentSchemaMetadata> inTransitSMQPublishedResults = new ConcurrentHashMap<>();

  public void setInitialized()
  {
    initialized.countDown();
  }

  public void awaitInitialization() throws InterruptedException
  {
    initialized.await();
  }

  public void updateFinalizedSegmentStatsReference(ConcurrentMap<SegmentId, SegmentStats> segmentStatsMap)
  {
    this.finalizedSegmentStats = segmentStatsMap;
  }

  public void updateFinalizedSegmentSchemaReference(
      ConcurrentMap<Long, SchemaPayload> schemaPayloadMap
  )
  {
    this.finalizedSegmentSchema = schemaPayloadMap;
  }

  public void addFinalizedSegmentSchema(long schemaId, SchemaPayload schemaPayload)
  {
    finalizedSegmentSchema.put(schemaId, schemaPayload);
  }

  public void addRealtimeSegmentSchema(SegmentId segmentId, RowSignature rowSignature, long numRows)
  {
    realtimeSegmentSchemaMap.put(segmentId, new SegmentSchemaMetadata(new SchemaPayload(rowSignature), numRows));
  }

  public void addInTransitSMQResult(SegmentId segmentId, RowSignature rowSignature, long numRows)
  {
    inTransitSMQResults.put(segmentId, new SegmentSchemaMetadata(new SchemaPayload(rowSignature), numRows));
  }

  public void markInTransitSMQResultPublished(SegmentId segmentId)
  {
    if (!inTransitSMQResults.containsKey(segmentId)) {
      log.error("Segment not found in InTransitSMQResultPublished map");
      return;
    }

    inTransitSMQPublishedResults.put(segmentId, inTransitSMQResults.get(segmentId));
    inTransitSMQResults.remove(segmentId);
  }

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
      if (schemaId != null && finalizedSegmentSchema.containsKey(schemaId)) {
        return Optional.of(
            new SegmentSchemaMetadata(
                finalizedSegmentSchema.get(schemaId),
                segmentStats.getNumRows() == null ? 0 : segmentStats.getNumRows()
            ));
      }
    }

    return Optional.empty();
  }

  public boolean isSchemaCached(SegmentId segmentId)
  {
    return realtimeSegmentSchemaMap.containsKey(segmentId) ||
           inTransitSMQResults.containsKey(segmentId) ||
           inTransitSMQPublishedResults.containsKey(segmentId) ||
           finalizedSegmentStats.containsKey(segmentId) &&
           finalizedSegmentSchema.containsKey(finalizedSegmentStats.get(segmentId).getSchemaId());
  }

  public boolean segmentRemoved(SegmentId segmentId)
  {
    if (!isSchemaCached(segmentId)) {
      return false;
    }
    // remove the segment from all the maps
    realtimeSegmentSchemaMap.remove(segmentId);
    inTransitSMQResults.remove(segmentId);
    inTransitSMQPublishedResults.remove(segmentId);
    finalizedSegmentStats.remove(segmentId);
    return true;
  }

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
