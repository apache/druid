package io.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.timeline.DataSegment;

import java.util.List;

public interface MetadataUpdaterJobHandler
{
  public void publishSegments(String tableName, List<DataSegment> segments, ObjectMapper mapper);
}
