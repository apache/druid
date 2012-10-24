package com.metamx.druid.indexer.granularity;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.Granularity;
import com.metamx.druid.jackson.DefaultObjectMapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class UniformGranularityTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSimple()
  {
    final GranularitySpec spec = new UniformGranularitySpec(
        Granularity.DAY,
        Lists.newArrayList(
            new Interval("2012-01-08T00Z/2012-01-11T00Z"),
            new Interval("2012-01-07T00Z/2012-01-08T00Z"),
            new Interval("2012-01-03T00Z/2012-01-04T00Z"),
            new Interval("2012-01-01T00Z/2012-01-03T00Z")
        )
    );

    Assert.assertEquals(
        Lists.newArrayList(
            new Interval("2012-01-01T00Z/P1D"),
            new Interval("2012-01-02T00Z/P1D"),
            new Interval("2012-01-03T00Z/P1D"),
            new Interval("2012-01-07T00Z/P1D"),
            new Interval("2012-01-08T00Z/P1D"),
            new Interval("2012-01-09T00Z/P1D"),
            new Interval("2012-01-10T00Z/P1D")
        ),
        Lists.newArrayList(spec.bucketIntervals())
    );

    Assert.assertEquals(
        "2012-01-03T00Z",
        Optional.of(new Interval("2012-01-03T00Z/2012-01-04T00Z")),
        spec.bucketInterval(new DateTime("2012-01-03T00Z"))
    );

    Assert.assertEquals(
        "2012-01-03T01Z",
        Optional.of(new Interval("2012-01-03T00Z/2012-01-04T00Z")),
        spec.bucketInterval(new DateTime("2012-01-03T01Z"))
    );

    Assert.assertEquals(
        "2012-01-07T23:59:59.999Z",
        Optional.of(new Interval("2012-01-07T00Z/2012-01-08T00Z")),
        spec.bucketInterval(new DateTime("2012-01-07T23:59:59.999Z"))
    );

    Assert.assertEquals(
        "2012-01-08T01Z",
        Optional.of(new Interval("2012-01-08T00Z/2012-01-09T00Z")),
        spec.bucketInterval(new DateTime("2012-01-08T01Z"))
    );
  }

  @Test
  public void testJson()
  {
    final GranularitySpec spec = new UniformGranularitySpec(
        Granularity.DAY,
        Lists.newArrayList(
            new Interval("2012-01-08T00Z/2012-01-11T00Z"),
            new Interval("2012-01-07T00Z/2012-01-08T00Z"),
            new Interval("2012-01-03T00Z/2012-01-04T00Z"),
            new Interval("2012-01-01T00Z/2012-01-03T00Z")
        )
    );

    try {
      final GranularitySpec rtSpec = jsonMapper.readValue(jsonMapper.writeValueAsString(spec), GranularitySpec.class);
      Assert.assertEquals(
          "Round-trip bucketIntervals",
          spec.bucketIntervals(),
          rtSpec.bucketIntervals()
      );
      Assert.assertEquals(
          "Round-trip granularity",
          ((UniformGranularitySpec) spec).getGranularity(),
          ((UniformGranularitySpec) rtSpec).getGranularity()
      );
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
