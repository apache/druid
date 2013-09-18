/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.loading;

import com.google.common.collect.ImmutableMap;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

/**
 */
public class StorageLocationTest
{
  @Test
  public void testStorageLocation() throws Exception
  {
    long expectedAvail = 1000l;
    StorageLocation loc = new StorageLocation(new File("/tmp"), expectedAvail);

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
