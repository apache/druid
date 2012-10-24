package com.metamx.druid.coordination;

import com.google.common.collect.ImmutableMap;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.shard.NoneShardSpec;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

/**
 */
public class SegmentChangeRequestDropTest
{
  @Test
  public void testV1Serialization() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    final Interval interval = new Interval("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.<String, Object>of("something", "or_other");

    DataSegment segment = new DataSegment(
        "something",
        interval,
        "1",
        loadSpec,
        Arrays.asList("dim1", "dim2"),
        Arrays.asList("met1", "met2"),
        new NoneShardSpec(),
        1
    );

    final SegmentChangeRequestDrop segmentDrop = new SegmentChangeRequestDrop(segment);

    Map<String, Object> objectMap = mapper.readValue(
        mapper.writeValueAsString(segmentDrop), new TypeReference<Map<String, Object>>(){}
    );

    Assert.assertEquals(10, objectMap.size());
    Assert.assertEquals("drop", objectMap.get("action"));
    Assert.assertEquals("something", objectMap.get("dataSource"));
    Assert.assertEquals(interval.toString(), objectMap.get("interval"));
    Assert.assertEquals("1", objectMap.get("version"));
    Assert.assertEquals(loadSpec, objectMap.get("loadSpec"));
    Assert.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assert.assertEquals("met1,met2", objectMap.get("metrics"));
    Assert.assertEquals(ImmutableMap.of("type", "none"), objectMap.get("shardSpec"));
    Assert.assertEquals(1, objectMap.get("size"));
  }
}
