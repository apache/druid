package org.apache.druid.segment.metadata;

import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SchemaPayloadWithNumRows;
import org.apache.druid.timeline.SegmentId;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// schema cache for published segments only
//
public class FinalizedSegmentSchemaCache
{
  // Mapping from schemaId to schema fingerprint & schema fingerprint to schema payload
  // On each database poll, fetch newly added schema since last poll.
  // Coordinator schema cleanup duty, removes orphan schema in the same transaction also remove schema from here
  // if transaction failed revert stuff by storing in temporary structure

  private volatile ConcurrentMap<Long, SchemaPayload> schemaPayloadMap = new ConcurrentHashMap<>();

  // Mapping from segmentId to segmentStats, reference is updated on each database poll.
  // edge case what happens if first this map is build from db
  // next new schema is fetch
  // in between
  private volatile ConcurrentMap<SegmentId, SegmentStats> segmentStatsMap = new ConcurrentHashMap<>();

  private ConcurrentMap<SegmentId, >

  public void updateSegmentStatsReference(ConcurrentMap<SegmentId, SegmentStats> segmentStatsMap)
  {
    this.segmentStatsMap = segmentStatsMap;
  }

  public void updateSchemaPayloadMapReference(
      ConcurrentMap<Long, SchemaPayload> schemaPayloadMap
  )
  {
    this.schemaPayloadMap = schemaPayloadMap;
  }

  public void addSchema(long schemaId, SchemaPayload schemaPayload)
  {
    schemaPayloadMap.put(schemaId, schemaPayload);
  }

  public Optional<SchemaPayloadWithNumRows> getSchemaForSegment(SegmentId segmentId)
  {
     if (!segmentStatsMap.containsKey(segmentId)) {
       return Optional.empty();
     }

     SegmentStats segmentStats = segmentStatsMap.get(segmentId);
     if (!schemaPayloadMap.containsKey(segmentStats.getSchemaId())) {
       return Optional.empty();
     }

     long schemaId = segmentStats.getSchemaId();

    return Optional.of(new SchemaPayloadWithNumRows(
        schemaId,
        segmentId.toString(),
        segmentStats.getNumRows(),
        schemaPayloadMap.get(schemaId)
    ));
  }

  public boolean isSchemaCached(SegmentId segmentId)
  {
    return segmentStatsMap.containsKey(segmentId);
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
