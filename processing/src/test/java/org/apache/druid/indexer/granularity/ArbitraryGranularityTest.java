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

package org.apache.druid.indexer.granularity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ArbitraryGranularityTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testDefaultQueryGranularity()
  {
    final GranularitySpec spec = new ArbitraryGranularitySpec(
        null,
        Lists.newArrayList(
            Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
            Intervals.of("2012-02-01T00Z/2012-03-01T00Z"),
            Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
            Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
            Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
        )
    );
    Assertions.assertNotNull(spec.getQueryGranularity());
  }

  @Test
  public void testSimple()
  {
    final GranularitySpec spec = new ArbitraryGranularitySpec(
        Granularities.NONE,
        Lists.newArrayList(
            Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
            Intervals.of("2012-02-01T00Z/2012-03-01T00Z"),
            Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
            Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
            Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
        )
    );

    Assertions.assertTrue(spec.isRollup());

    Assertions.assertEquals(
        Lists.newArrayList(
            Intervals.of("2012-01-01T00Z/2012-01-03T00Z"),
            Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
            Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
            Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
            Intervals.of("2012-02-01T00Z/2012-03-01T00Z")
        ),
        Lists.newArrayList(spec.sortedBucketIntervals())
    );

    Assertions.assertEquals(
        Optional.of(Intervals.of("2012-01-01T00Z/2012-01-03T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-01T00Z"))
    );

    Assertions.assertEquals(
        Optional.of(Intervals.of("2012-01-08T00Z/2012-01-11T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-08T00Z"))
    );

    Assertions.assertEquals(
        Optional.of(Intervals.of("2012-01-03T00Z/2012-01-04T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-03T00Z")),
        "2012-01-03T00Z"
    );

    Assertions.assertEquals(
        Optional.of(Intervals.of("2012-01-03T00Z/2012-01-04T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-03T01Z")),
        "2012-01-03T01Z"
    );

    Assertions.assertEquals(
        Optional.<Interval>absent(),
        spec.bucketInterval(DateTimes.of("2012-01-04T01Z")),
        "2012-01-04T01Z"
    );

    Assertions.assertEquals(
        Optional.of(Intervals.of("2012-01-07T00Z/2012-01-08T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-07T23:59:59.999Z")),
        "2012-01-07T23:59:59.999Z"
    );

    Assertions.assertEquals(
        Optional.of(Intervals.of("2012-01-08T00Z/2012-01-11T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-08T01Z")),
        "2012-01-08T01Z"
    );

    Assertions.assertEquals(
        Optional.absent(),
        spec.bucketInterval(DateTimes.of("2012-01-04T00Z")),
        "2012-01-04T00Z"
    );

    Assertions.assertEquals(
        Optional.absent(),
        spec.bucketInterval(DateTimes.of("2012-01-05T00Z")),
        "2012-01-05T00Z"
    );

  }

  @Test
  public void testOverlapViolation()
  {
    List<Interval> intervals = Lists.newArrayList(
        Intervals.of("2012-01-02T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    );

    boolean thrown = false;
    try {
      final GranularitySpec spec = new ArbitraryGranularitySpec(Granularities.NONE, intervals);
    }
    catch (IllegalArgumentException e) {
      thrown = true;
    }

    Assertions.assertTrue(thrown, "Exception thrown");
  }

  @Test
  public void testRollupSetting()
  {
    List<Interval> intervals = Lists.newArrayList(
        Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
        Intervals.of("2012-02-01T00Z/2012-03-01T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    );
    final GranularitySpec spec = new ArbitraryGranularitySpec(Granularities.NONE, false, intervals);

    Assertions.assertFalse(spec.isRollup());
  }

  @Test
  public void testOverlapViolationSameStartInstant()
  {
    List<Interval> intervals = Lists.newArrayList(
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-05T00Z")
    );

    boolean thrown = false;
    try {
      final GranularitySpec spec = new ArbitraryGranularitySpec(Granularities.NONE, intervals);
    }
    catch (IllegalArgumentException e) {
      thrown = true;
    }

    Assertions.assertTrue(thrown, "Exception thrown");
  }

  @Test
  public void testJson()
  {
    final GranularitySpec spec = new ArbitraryGranularitySpec(Granularities.NONE, Lists.newArrayList(
        Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
        Intervals.of("2012-02-01T00Z/2012-03-01T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    ));

    try {
      final GranularitySpec rtSpec = JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(spec), GranularitySpec.class);
      Assertions.assertEquals(
          ImmutableList.copyOf(spec.sortedBucketIntervals()),
          ImmutableList.copyOf(rtSpec.sortedBucketIntervals()),
          "Round-trip"
      );
      Assertions.assertEquals(
          ImmutableList.copyOf(spec.inputIntervals()),
          ImmutableList.copyOf(rtSpec.inputIntervals()),
          "Round-trip"
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testNullInputIntervals()
  {
    final GranularitySpec spec = new ArbitraryGranularitySpec(Granularities.NONE, null);
    Assertions.assertFalse(spec.sortedBucketIntervals().iterator().hasNext());
  }
}
