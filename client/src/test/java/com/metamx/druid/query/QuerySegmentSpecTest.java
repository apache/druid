package com.metamx.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.query.segment.LegacySegmentSpec;
import com.metamx.druid.query.segment.MultipleIntervalSegmentSpec;
import com.metamx.druid.query.segment.MultipleSpecificSegmentSpec;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import com.metamx.druid.query.segment.SegmentDescriptor;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 */
public class QuerySegmentSpecTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSerializationLegacyString() throws Exception
  {
    QuerySegmentSpec spec = jsonMapper.readValue(
        "\"2011-10-01/2011-10-10,2011-11-01/2011-11-10\"", QuerySegmentSpec.class
    );
    Assert.assertTrue(spec instanceof LegacySegmentSpec);
    Assert.assertEquals(
        ImmutableList.of(new Interval("2011-10-01/2011-10-10"), new Interval("2011-11-01/2011-11-10")),
        spec.getIntervals()
    );
  }

  @Test
  public void testSerializationLegacyArray() throws Exception
  {
    QuerySegmentSpec spec = jsonMapper.readValue(
        "[\"2011-09-01/2011-10-10\", \"2011-11-01/2011-11-10\"]", QuerySegmentSpec.class
    );
    Assert.assertTrue(spec instanceof LegacySegmentSpec);
    Assert.assertEquals(
        ImmutableList.of(new Interval("2011-09-01/2011-10-10"), new Interval("2011-11-01/2011-11-10")),
        spec.getIntervals()
    );
  }

  @Test
  public void testSerializationIntervals() throws Exception
  {
    QuerySegmentSpec spec = jsonMapper.readValue(
        "{\"type\": \"intervals\", \"intervals\":[\"2011-08-01/2011-10-10\", \"2011-11-01/2011-11-10\"]}",
        QuerySegmentSpec.class
    );
    Assert.assertTrue(spec instanceof MultipleIntervalSegmentSpec);
    Assert.assertEquals(
        ImmutableList.of(new Interval("2011-08-01/2011-10-10"), new Interval("2011-11-01/2011-11-10")),
        spec.getIntervals()
    );
  }

  @Test
  public void testSerializationSegments() throws Exception
  {
    QuerySegmentSpec spec = jsonMapper.convertValue(
        ImmutableMap.<String, Object>of(
            "type", "segments",

            "segments", ImmutableList
            .<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "itvl", "2011-07-01/2011-10-10",
                    "ver", "1",
                    "part", 0
                ),
                ImmutableMap.<String, Object>of(
                    "itvl", "2011-07-01/2011-10-10",
                    "ver", "1",
                    "part", 1
                ),
                ImmutableMap.<String, Object>of(
                    "itvl", "2011-11-01/2011-11-10",
                    "ver", "2",
                    "part", 10
                )
            )
        ),
        QuerySegmentSpec.class
    );
    Assert.assertTrue(spec instanceof MultipleSpecificSegmentSpec);
    Assert.assertEquals(
        ImmutableList.of(new Interval("2011-07-01/2011-10-10"), new Interval("2011-11-01/2011-11-10")),
        spec.getIntervals()
    );
    Assert.assertEquals(
        ImmutableList.of(
            new SegmentDescriptor(new Interval("2011-07-01/2011-10-10"), "1", 0),
            new SegmentDescriptor(new Interval("2011-07-01/2011-10-10"), "1", 1),
            new SegmentDescriptor(new Interval("2011-11-01/2011-11-10"), "2", 10)
        ),
        ((MultipleSpecificSegmentSpec) spec).getDescriptors()
    );
  }
}
