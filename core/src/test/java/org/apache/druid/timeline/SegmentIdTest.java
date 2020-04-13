/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.timeline;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class SegmentIdTest
{
  @Test
  public void testBasic()
  {
    String datasource = "datasource";
    SegmentId desc = SegmentId.of(datasource, Intervals.of("2015-01-02/2015-01-03"), "ver_0", 1);
    Assert.assertEquals("datasource_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver_0_1", desc.toString());
    Assert.assertEquals(desc, SegmentId.tryParse(datasource, desc.toString()));

    desc = desc.withInterval(Intervals.of("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals("datasource_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver_0_1", desc.toString());
    Assert.assertEquals(desc, SegmentId.tryParse(datasource, desc.toString()));

    desc = SegmentId.of(datasource, Intervals.of("2015-01-02/2015-01-03"), "ver", 0);
    Assert.assertEquals("datasource_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver", desc.toString());
    Assert.assertEquals(desc, SegmentId.tryParse(datasource, desc.toString()));

    desc = desc.withInterval(Intervals.of("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals("datasource_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver", desc.toString());
    Assert.assertEquals(desc, SegmentId.tryParse(datasource, desc.toString()));
  }

  @Test
  public void testDataSourceWithUnderscore()
  {
    String datasource = "datasource_1";
    SegmentId desc = SegmentId.of(datasource, Intervals.of("2015-01-02/2015-01-03"), "ver_0", 1);
    Assert.assertEquals("datasource_1_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver_0_1", desc.toString());
    Assert.assertEquals(desc, SegmentId.tryParse(datasource, desc.toString()));

    desc = desc.withInterval(Intervals.of("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals("datasource_1_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver_0_1", desc.toString());
    Assert.assertEquals(desc, SegmentId.tryParse(datasource, desc.toString()));

    desc = SegmentId.of(datasource, Intervals.of("2015-01-02/2015-01-03"), "ver", 0);
    Assert.assertEquals("datasource_1_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver", desc.toString());
    Assert.assertEquals(desc, SegmentId.tryParse(datasource, desc.toString()));

    desc = desc.withInterval(Intervals.of("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals("datasource_1_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver", desc.toString());
    Assert.assertEquals(desc, SegmentId.tryParse(datasource, desc.toString()));
  }

  /**
   * Test the ambiguity of a datasource name ending with '_yyyy-mm-dd..' string that could be considered either as the
   * end of the datasource name or the interval start in the segment id's string representation.
   */
  @Test
  public void testDataSourceWithUnderscoreAndTimeStringInDataSourceName()
  {
    String dataSource = "datasource_2015-01-01T00:00:00.000Z";
    SegmentId desc = SegmentId.of(dataSource, Intervals.of("2015-01-02/2015-01-03"), "ver_0", 1);
    Assert.assertEquals(
        "datasource_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver_0_1",
        desc.toString()
    );
    Assert.assertEquals(desc, SegmentId.tryParse(dataSource, desc.toString()));

    desc = desc.withInterval(Intervals.of("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals(
        "datasource_2015-01-01T00:00:00.000Z_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver_0_1",
        desc.toString()
    );
    Assert.assertEquals(desc, SegmentId.tryParse(dataSource, desc.toString()));

    desc = SegmentId.of(dataSource, Intervals.of("2015-01-02/2015-01-03"), "ver", 0);
    Assert.assertEquals(
        "datasource_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver",
        desc.toString()
    );
    Assert.assertEquals(desc, SegmentId.tryParse(dataSource, desc.toString()));

    desc = desc.withInterval(Intervals.of("2014-10-20T00:00:00Z/P1D"));
    Assert.assertEquals(
        "datasource_2015-01-01T00:00:00.000Z_2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z_ver",
        desc.toString()
    );
    Assert.assertEquals(desc, SegmentId.tryParse(dataSource, desc.toString()));
  }

  /**
   * The interval start is later than the end
   */
  @Test
  public void testInvalidFormat0()
  {
    Assert.assertNull(
        SegmentId.tryParse("datasource", "datasource_2015-01-02T00:00:00.000Z_2014-10-20T00:00:00.000Z_version")
    );
  }

  /**
   * No interval dates
   */
  @Test
  public void testInvalidFormat1()
  {
    Assert.assertNull(SegmentId.tryParse("datasource", "datasource_invalid_interval_version"));
  }

  /**
   * Not enough interval dates
   */
  @Test
  public void testInvalidFormat2()
  {
    Assert.assertNull(SegmentId.tryParse("datasource", "datasource_2015-01-02T00:00:00.000Z_version"));
  }

  /**
   * Tests that {@link SegmentId#tryExtractMostProbableDataSource} successfully extracts data sources from some
   * reasonable segment ids.
   */
  @Test
  public void testTryParseHeuristically()
  {
    List<String> segmentIds = Arrays.asList(
        "datasource_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver_0_1",
        "datasource_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver",
        "datasource_1_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver_0_1",
        "datasource_1_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver",
        "datasource_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver_0_1",
        "datasource_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver"
    );
    for (String segmentId : segmentIds) {
      String dataSource = SegmentId.tryExtractMostProbableDataSource(segmentId);
      Assert.assertTrue("datasource".equals(dataSource) || "datasource_1".equals(dataSource));
      Assert.assertTrue(!SegmentId.iteratePossibleParsingsWithDataSource(dataSource, segmentId).isEmpty());
    }
  }

  @Test
  public void testTryParseVersionAmbiguity()
  {
    SegmentId segmentId =
        SegmentId.tryParse("datasource", "datasource_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver_0");
    Assert.assertNotNull(segmentId);
    Assert.assertEquals("ver_0", segmentId.getVersion());
    Assert.assertEquals(0, segmentId.getPartitionNum());
  }

  @Test
  public void testIterateAllPossibleParsings()
  {
    String segmentId = "datasource_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_ver_0_1";
    List<SegmentId> possibleParsings = ImmutableList.copyOf(SegmentId.iterateAllPossibleParsings(segmentId));
    DateTime dt1 = DateTimes.of("2015-01-01T00:00:00.000Z");
    DateTime dt2 = DateTimes.of("2015-01-02T00:00:00.000Z");
    DateTime dt3 = DateTimes.of("2015-01-03T00:00:00.000Z");
    Set<SegmentId> expected = ImmutableSet.of(
        SegmentId.of("datasource", new Interval(dt1, dt2), "2015-01-03T00:00:00.000Z_ver_0", 1),
        SegmentId.of("datasource", new Interval(dt1, dt2), "2015-01-03T00:00:00.000Z_ver_0_1", 0),
        SegmentId.of("datasource_2015-01-01T00:00:00.000Z", new Interval(dt2, dt3), "ver_0", 1),
        SegmentId.of("datasource_2015-01-01T00:00:00.000Z", new Interval(dt2, dt3), "ver_0_1", 0)
    );
    Assert.assertEquals(4, possibleParsings.size());
    Assert.assertEquals(expected, ImmutableSet.copyOf(possibleParsings));
  }

  @Test
  public void testIterateAllPossibleParsingsWithEmptyVersion()
  {
    String segmentId = "datasource_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z__1";
    List<SegmentId> possibleParsings = ImmutableList.copyOf(SegmentId.iterateAllPossibleParsings(segmentId));
    DateTime dt1 = DateTimes.of("2015-01-01T00:00:00.000Z");
    DateTime dt2 = DateTimes.of("2015-01-02T00:00:00.000Z");
    DateTime dt3 = DateTimes.of("2015-01-03T00:00:00.000Z");
    Set<SegmentId> expected = ImmutableSet.of(
        SegmentId.of("datasource", new Interval(dt1, dt2), "2015-01-03T00:00:00.000Z_", 1),
        SegmentId.of("datasource", new Interval(dt1, dt2), "2015-01-03T00:00:00.000Z__1", 0),
        SegmentId.of("datasource_2015-01-01T00:00:00.000Z", new Interval(dt2, dt3), "", 1),
        SegmentId.of("datasource_2015-01-01T00:00:00.000Z", new Interval(dt2, dt3), "_1", 0)
    );
    Assert.assertEquals(4, possibleParsings.size());
    Assert.assertEquals(expected, ImmutableSet.copyOf(possibleParsings));
  }

  /**
   * Three DateTime strings included, but not ascending, that makes a pair of parsings impossible, compared to {@link
   * #testIterateAllPossibleParsings}.
   */
  @Test
  public void testIterateAllPossibleParsings2()
  {
    String segmentId = "datasource_2015-01-02T00:00:00.000Z_2015-01-03T00:00:00.000Z_2015-01-02T00:00:00.000Z_ver_1";
    List<SegmentId> possibleParsings = ImmutableList.copyOf(SegmentId.iterateAllPossibleParsings(segmentId));
    DateTime dt1 = DateTimes.of("2015-01-02T00:00:00.000Z");
    DateTime dt2 = DateTimes.of("2015-01-03T00:00:00.000Z");
    Set<SegmentId> expected = ImmutableSet.of(
        SegmentId.of("datasource", new Interval(dt1, dt2), "2015-01-02T00:00:00.000Z_ver", 1),
        SegmentId.of("datasource", new Interval(dt1, dt2), "2015-01-02T00:00:00.000Z_ver_1", 0)
    );
    Assert.assertEquals(2, possibleParsings.size());
    Assert.assertEquals(expected, ImmutableSet.copyOf(possibleParsings));
  }
}
