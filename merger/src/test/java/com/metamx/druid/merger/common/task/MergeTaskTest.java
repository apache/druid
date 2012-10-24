package com.metamx.druid.merger.common.task;

import com.google.common.collect.ImmutableList;
import com.metamx.druid.client.DataSegment;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

public class MergeTaskTest
{
  final List<DataSegment> segments =
      ImmutableList
          .<DataSegment>builder()
          .add(new DataSegment("foo", new Interval("2012-01-04/2012-01-06"), "V1", null, null, null, null, -1))
          .add(new DataSegment("foo", new Interval("2012-01-05/2012-01-07"), "V1", null, null, null, null, -1))
          .add(new DataSegment("foo", new Interval("2012-01-03/2012-01-05"), "V1", null, null, null, null, -1))
          .build();

  final MergeTask testMergeTask = new MergeTask("foo", segments)
  {
    @Override
    protected File merge(Map<DataSegment, File> segments, File outDir) throws Exception
    {
      return null;
    }

    @Override
    public Type getType()
    {
      return Type.TEST;
    }
  };

  @Test
  public void testDataSource()
  {
    Assert.assertEquals("foo", testMergeTask.getDataSource());
  }

  @Test
  public void testInterval()
  {
    Assert.assertEquals(new Interval("2012-01-03/2012-01-07"), testMergeTask.getInterval());
  }

  @Test
  public void testID()
  {
    final String desiredPrefix = "merge_foo_" + DigestUtils.shaHex(
        "2012-01-03T00:00:00.000Z_2012-01-05T00:00:00.000Z_V1_0"
        + "_2012-01-04T00:00:00.000Z_2012-01-06T00:00:00.000Z_V1_0"
        + "_2012-01-05T00:00:00.000Z_2012-01-07T00:00:00.000Z_V1_0"
    ) + "_";
    Assert.assertEquals(
        desiredPrefix,
        testMergeTask.getId().substring(0, desiredPrefix.length())
    );
  }
}
