/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.loading;

import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.Intervals;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

/**
 */
public class StorageLocationTest
{
  @Test
  public void testStorageLocationFreePercent()
  {
    // free space ignored only maxSize matters
    StorageLocation locationPlain = fakeLocation(100_000, 5_000, 10_000, null);
    Assert.assertTrue(locationPlain.canHandle(makeSegment("2012/2013", 9_000)));
    Assert.assertFalse(locationPlain.canHandle(makeSegment("2012/2013", 11_000)));

    // enough space available maxSize is the limit
    StorageLocation locationFree = fakeLocation(100_000, 25_000, 10_000, 10.0);
    Assert.assertTrue(locationFree.canHandle(makeSegment("2012/2013", 9_000)));
    Assert.assertFalse(locationFree.canHandle(makeSegment("2012/2013", 11_000)));

    // disk almost full percentage is the limit
    StorageLocation locationFull = fakeLocation(100_000, 15_000, 10_000, 10.0);
    Assert.assertTrue(locationFull.canHandle(makeSegment("2012/2013", 4_000)));
    Assert.assertFalse(locationFull.canHandle(makeSegment("2012/2013", 6_000)));
  }

  private StorageLocation fakeLocation(long total, long free, long max, Double percent)
  {
    File file = EasyMock.mock(File.class);
    EasyMock.expect(file.getTotalSpace()).andReturn(total).anyTimes();
    EasyMock.expect(file.getFreeSpace()).andReturn(free).anyTimes();
    EasyMock.replay(file);
    return new StorageLocation(file, max, percent);
  }

  @Test
  public void testStorageLocation() throws Exception
  {
    long expectedAvail = 1000L;
    StorageLocation loc = new StorageLocation(new File("/tmp"), expectedAvail, null);

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

  private void verifyLoc(long maxSize, StorageLocation loc)
  {
    Assert.assertEquals(maxSize, loc.available());
    for (int i = 0; i <= maxSize; ++i) {
      Assert.assertTrue(String.valueOf(i), loc.canHandle(makeSegment("2013/2014", i)));
    }
  }

  private DataSegment makeSegment(String intervalString, long size)
  {
    return new DataSegment(
        "test",
        Intervals.of(intervalString),
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
