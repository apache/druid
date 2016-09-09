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
    long expectedAvail = 1000L;
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
