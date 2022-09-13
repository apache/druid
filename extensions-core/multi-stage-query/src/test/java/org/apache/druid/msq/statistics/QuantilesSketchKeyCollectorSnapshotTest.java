package org.apache.druid.msq.statistics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class QuantilesSketchKeyCollectorSnapshotTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSnapshotSerde() throws JsonProcessingException
  {
    QuantilesSketchKeyCollectorSnapshot snapshot = new QuantilesSketchKeyCollectorSnapshot("sketchString", 100);
    String jsonStr = jsonMapper.writeValueAsString(snapshot);
    Assert.assertEquals(snapshot, jsonMapper.readValue(jsonStr, QuantilesSketchKeyCollectorSnapshot.class));
  }
}