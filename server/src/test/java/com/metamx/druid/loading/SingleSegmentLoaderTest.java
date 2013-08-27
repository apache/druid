package com.metamx.druid.loading;

import com.google.common.collect.ImmutableMap;
import com.metamx.druid.client.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

/**
 */
public class SingleSegmentLoaderTest
{
  @Test
  public void testStorageLocation() throws Exception
  {
    long expectedAvail = 1000l;
    SingleSegmentLoader.StorageLocation loc = new SingleSegmentLoader.StorageLocation(new File("/tmp"), expectedAvail);

    verifyLoc(expectedAvail, loc);

    final DataSegment secondSegment = makeSegment("2012-01-02/2012-01-03", 23);

    loc.addSegment(makeSegment("2012-01-01/2012-01-02", 10));
    expectedAvail -= 10;
    verifyLoc(expectedAvail, loc);

    loc.addSegment(makeSegment("2012-01-01/2012-01-02", 10));
    verifyLoc(expectedAvail, loc);

    loc.addSegment(secondSegment);
    expectedAvail -= 23;
    verifyLoc(expectedAvail, loc);

    loc.removeSegment(makeSegment("2012-01-01/2012-01-02", 10));
    expectedAvail += 10;
    verifyLoc(expectedAvail, loc);

    loc.removeSegment(makeSegment("2012-01-01/2012-01-02", 10));
    verifyLoc(expectedAvail, loc);

    loc.removeSegment(secondSegment);
    expectedAvail += 23;
    verifyLoc(expectedAvail, loc);
  }

  private void verifyLoc(long maxSize, SingleSegmentLoader.StorageLocation loc)
  {
    Assert.assertEquals(maxSize, loc.available());
    for (int i = 0; i <= maxSize; ++i) {
      Assert.assertTrue(String.valueOf(i), loc.canHandle(i));
    }
  }

  private DataSegment makeSegment(String intervalString, long size)
  {
    return new DataSegment(
        "test",
        new Interval(intervalString),
        "1",
        ImmutableMap.<String, Object>of(),
        Arrays.asList("d"),
        Arrays.asList("m"),
        null,
        null,
        size
    );
  }
}
