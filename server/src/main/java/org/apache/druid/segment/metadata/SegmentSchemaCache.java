package org.apache.druid.segment.metadata;

import org.apache.druid.guice.LazySingleton;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SegmentSchemaMetadata;
import org.apache.druid.timeline.SegmentId;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

// schema cache for published segments only
//
@LazySingleton
public class SegmentSchemaCache
{

  private CountDownLatch initialized = new CountDownLatch(1);

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

  private ConcurrentMap<SegmentId, SegmentSchemaMetadata> realtimeSegmentSchemaMap = new ConcurrentHashMap<>();

  private ConcurrentMap<SegmentId, SegmentSchemaMetadata> inTransitSMQResults = new ConcurrentHashMap<>();
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
      // error how come it is not present?
    }

    inTransitSMQPublishedResults.put(segmentId, inTransitSMQResults.get(segmentId));
  }

  public void resetInTransitSMQResultPublishedOnDBPoll()
  {
    inTransitSMQPublishedResults = new ConcurrentHashMap<>();
  }

  public Optional<SegmentSchemaMetadata> getSchemaForSegment(SegmentId segmentId)
  {
    // check realtime
     if (!finalizedSegmentStats.containsKey(segmentId)) {
       return Optional.empty();
     }

     SegmentStats segmentStats = finalizedSegmentStats.get(segmentId);
     if (!finalizedSegmentSchema.containsKey(segmentStats.getSchemaId())) {
       return Optional.empty();
     }

     long schemaId = segmentStats.getSchemaId();

    return Optional.of(new SegmentSchemaMetadata(
        finalizedSegmentSchema.get(schemaId),
        segmentStats.getNumRows()
    ));
  }

  public boolean isSchemaCached(SegmentId segmentId)
  {
    return finalizedSegmentStats.containsKey(segmentId);
  }

  public boolean segmentRemoved(SegmentId segmentId) {
    return false;
  }

  public static class SegmentStats {
    private final long schemaId;
    private final long numRows;

    public SegmentStats(long schemaId, long numRows)
    {
      this.schemaId = schemaId;
      this.numRows = numRows;
    }

    public long getSchemaId()
    {
      return schemaId;
    }

    public long getNumRows()
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
