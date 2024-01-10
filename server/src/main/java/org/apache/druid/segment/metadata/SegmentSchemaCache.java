package org.apache.druid.segment.metadata;

import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SchemaPayloadWithNumRows;
import org.apache.druid.timeline.SegmentId;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// schema cache for published segments only
//
public class SegmentSchemaCache
{

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

  private ConcurrentMap<SegmentId, SchemaPayloadWithNumRows> realtimeSegmentSchemaMap = new ConcurrentHashMap<>();

  private ConcurrentMap<SegmentId, SchemaPayloadWithNumRows> inTransitSMQResults = new ConcurrentHashMap<>();
  private volatile ConcurrentMap<SegmentId, SchemaPayloadWithNumRows> inTransitSMQPublishedResults = new ConcurrentHashMap<>();

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

  public void addRealtimeSegmentSchema(SegmentId segmentId, SchemaPayloadWithNumRows schemaPayloadWithNumRows)
  {
    realtimeSegmentSchemaMap.put(segmentId, schemaPayloadWithNumRows);
  }

  public void addInTransitSMQResult(SegmentId segmentId, SchemaPayloadWithNumRows schemaPayloadWithNumRows)
  {
    inTransitSMQResults.put(segmentId, schemaPayloadWithNumRows);
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

  public Optional<SchemaPayloadWithNumRows> getSchemaForSegment(SegmentId segmentId)
  {
     if (!finalizedSegmentStats.containsKey(segmentId)) {
       return Optional.empty();
     }

     SegmentStats segmentStats = finalizedSegmentStats.get(segmentId);
     if (!finalizedSegmentSchema.containsKey(segmentStats.getSchemaId())) {
       return Optional.empty();
     }

     long schemaId = segmentStats.getSchemaId();

    return Optional.of(new SchemaPayloadWithNumRows(
        schemaId,
        segmentId.toString(),
        segmentStats.getNumRows(),
        finalizedSegmentSchema.get(schemaId)
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
  }
}
