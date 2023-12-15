package org.apache.druid.segment.metadata;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.timeline.SegmentId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

// schema cache for published segments only
//
public class SegmentSchemaCache
{
  private final Map<Long, String> schemaIdFingerprintMap = new HashMap<>();
  private final Map<String, SchemaPayload> schemaPayloadMap = new HashMap<>();

  // every minute full snaphot of segments
  // remove
  private final Map<SegmentId, SegmentStats> segmentStatsMap = new HashMap<>();

  public void addSchema(long schemaId, String fingerprint, SchemaPayload schemaPayload)
  {
    schemaIdFingerprintMap.put(schemaId, fingerprint);
    schemaPayloadMap.put(fingerprint, schemaPayload);
  }

  public void addSegmentStats(SegmentId segmentId, long schemaId, long numRows)
  {
    if (segmentStatsMap.containsKey(segmentId)) {
      // already present
    }

    if (!schemaIdFingerprintMap.containsKey(schemaId)) {
      // log and skip
    }
    segmentStatsMap.put(segmentId, new SegmentStats(schemaId, numRows));
  }

  public void fullSync(
      Map<SegmentId, Pair<Long, Long>> segmentMap,
      Map<Long, Pair<String, SchemaPayload>> schemaMap
  )
  {

    schemaMap.forEach((k, v) -> addSchema(k, v.lhs, v.rhs));
    segmentMap.forEach((k, v) -> addSegmentStats(k, v.lhs, v.rhs));
  }

  public Optional<SegmentSchema> getSchemaForSegment(SegmentId segmentId)
  {
     if (!segmentStatsMap.containsKey(segmentId)) {
       return Optional.empty();
     }

     SegmentStats segmentStats = segmentStatsMap.get(segmentId);
     if (!schemaIdFingerprintMap.containsKey(segmentStats.getSchemaId())) {
       return Optional.empty();
     }

     String fingerprint = schemaIdFingerprintMap.get(segmentStats.getSchemaId());

     return Optional.of(new SegmentSchema(fingerprint, segmentId.toString(), segmentStats.getNumRows(), schemaPayloadMap.get(fingerprint)));
  }

  public boolean isSchemaCached(SegmentId segmentId)
  {
    return segmentStatsMap.containsKey(segmentId);
  }

  private static class SegmentStats {
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
