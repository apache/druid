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

package io.druid.segment.indexing.granularity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

public class ArbitraryGranularityTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSimple()
  {
    final GranularitySpec spec = new ArbitraryGranularitySpec(
        QueryGranularities.NONE,
        Lists.newArrayList(
        new Interval("2012-01-08T00Z/2012-01-11T00Z"),
        new Interval("2012-02-01T00Z/2012-03-01T00Z"),
        new Interval("2012-01-07T00Z/2012-01-08T00Z"),
        new Interval("2012-01-03T00Z/2012-01-04T00Z"),
        new Interval("2012-01-01T00Z/2012-01-03T00Z")
    ));

    Assert.assertTrue(spec.isRollup());

    Assert.assertEquals(
        Lists.newArrayList(
            new Interval("2012-01-01T00Z/2012-01-03T00Z"),
            new Interval("2012-01-03T00Z/2012-01-04T00Z"),
            new Interval("2012-01-07T00Z/2012-01-08T00Z"),
            new Interval("2012-01-08T00Z/2012-01-11T00Z"),
            new Interval("2012-02-01T00Z/2012-03-01T00Z")
        ),
        Lists.newArrayList(spec.bucketIntervals().get())
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
        "2012-01-04T01Z",
        Optional.<Interval>absent(),
        spec.bucketInterval(new DateTime("2012-01-04T01Z"))
    );

    Assert.assertEquals(
        "2012-01-07T23:59:59.999Z",
        Optional.of(new Interval("2012-01-07T00Z/2012-01-08T00Z")),
        spec.bucketInterval(new DateTime("2012-01-07T23:59:59.999Z"))
    );

    Assert.assertEquals(
        "2012-01-08T01Z",
        Optional.of(new Interval("2012-01-08T00Z/2012-01-11T00Z")),
        spec.bucketInterval(new DateTime("2012-01-08T01Z"))
    );

    Assert.assertEquals(
        "2012-01-04T00Z",
        Optional.absent(),
        spec.bucketInterval(new DateTime("2012-01-04T00Z"))
    );

    Assert.assertEquals(
        "2012-01-05T00Z",
        Optional.absent(),
        spec.bucketInterval(new DateTime("2012-01-05T00Z"))
    );
  }

  @Test
  public void testOverlapViolation()
  {
    List<Interval> intervals = Lists.newArrayList(
        new Interval("2012-01-02T00Z/2012-01-04T00Z"),
        new Interval("2012-01-07T00Z/2012-01-08T00Z"),
        new Interval("2012-01-01T00Z/2012-01-03T00Z")
    );

    boolean thrown = false;
    try {
      final GranularitySpec spec = new ArbitraryGranularitySpec(QueryGranularities.NONE, intervals);
    } catch(IllegalArgumentException e) {
      thrown = true;
    }

    Assert.assertTrue("Exception thrown", thrown);
  }

  @Test
  public void testRollupSetting()
  {
    List<Interval> intervals = Lists.newArrayList(
        new Interval("2012-01-08T00Z/2012-01-11T00Z"),
        new Interval("2012-02-01T00Z/2012-03-01T00Z"),
        new Interval("2012-01-07T00Z/2012-01-08T00Z"),
        new Interval("2012-01-03T00Z/2012-01-04T00Z"),
        new Interval("2012-01-01T00Z/2012-01-03T00Z")
    );
    final GranularitySpec spec = new ArbitraryGranularitySpec(QueryGranularities.NONE, false, intervals, null);

    Assert.assertFalse(spec.isRollup());
  }

  @Test
  public void testOverlapViolationSameStartInstant()
  {
    List<Interval> intervals = Lists.newArrayList(
        new Interval("2012-01-03T00Z/2012-01-04T00Z"),
        new Interval("2012-01-03T00Z/2012-01-05T00Z")
    );

    boolean thrown = false;
    try {
      final GranularitySpec spec = new ArbitraryGranularitySpec(QueryGranularities.NONE, intervals);
    } catch(IllegalArgumentException e) {
      thrown = true;
    }

    Assert.assertTrue("Exception thrown", thrown);
  }

  @Test
  public void testJson()
  {
    final GranularitySpec spec = new ArbitraryGranularitySpec(QueryGranularities.NONE, Lists.newArrayList(
        new Interval("2012-01-08T00Z/2012-01-11T00Z"),
        new Interval("2012-02-01T00Z/2012-03-01T00Z"),
        new Interval("2012-01-07T00Z/2012-01-08T00Z"),
        new Interval("2012-01-03T00Z/2012-01-04T00Z"),
        new Interval("2012-01-01T00Z/2012-01-03T00Z")
    ));

    try {
      final GranularitySpec rtSpec = jsonMapper.readValue(jsonMapper.writeValueAsString(spec), GranularitySpec.class);
      Assert.assertEquals("Round-trip", spec.bucketIntervals(), rtSpec.bucketIntervals());
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Test
  public void testTimezone() {
    final GranularitySpec spec = new ArbitraryGranularitySpec(
            null,
            true,
            Lists.newArrayList(
                    new Interval("2012-01-08T00-08:00/2012-01-11T00-08:00"),
                    new Interval("2012-01-07T00-08:00/2012-01-08T00-08:00"),
                    new Interval("2012-01-03T00-08:00/2012-01-04T00-08:00"),
                    new Interval("2012-01-01T00-08:00/2012-01-03T00-08:00"),
                    new Interval("2012-09-01T00-07:00/2012-09-03T00-07:00")
            ),
            "America/Los_Angeles"
    );

    Assert.assertTrue(spec.bucketIntervals().isPresent());

    final Optional<SortedSet<Interval>> sortedSetOptional = spec.bucketIntervals();
    final SortedSet<Interval> intervals = sortedSetOptional.get();

    final ISOChronology chrono = ISOChronology.getInstance(DateTimeZone.forID("America/Los_Angeles"));

    final ArrayList<Interval> expectedIntervals = Lists.newArrayList(
            new Interval("2012-01-01/2012-01-03", chrono),
            new Interval("2012-01-03/2012-01-04", chrono),
            new Interval("2012-01-07/2012-01-08", chrono),
            new Interval("2012-01-08/2012-01-11", chrono),
            new Interval("2012-09-01/2012-09-03", chrono)
    );

    Assert.assertEquals(expectedIntervals, new ArrayList<Interval>(intervals));
  }
}
