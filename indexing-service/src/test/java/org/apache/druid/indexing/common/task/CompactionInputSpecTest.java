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

package org.apache.druid.indexing.common.task;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(Parameterized.class)
public class CompactionInputSpecTest
{
  private static final String DATASOURCE = "datasource";
  private static final List<DataSegment> SEGMENTS = prepareSegments();
  private static Interval INTERVAL = JodaUtils.umbrellaInterval(
      SEGMENTS.stream().map(DataSegment::getInterval).collect(Collectors.toList())
  );

  @Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{
            new CompactionIntervalSpec(
                INTERVAL,
                SegmentUtils.hashIds(SEGMENTS)
            )
        },
        new Object[]{
            new SpecificSegmentsSpec(
                SEGMENTS.stream().map(segment -> segment.getId().toString()).collect(Collectors.toList())
            )
        }
    );
  }

  private static List<DataSegment> prepareSegments()
  {
    return IntStream.range(0, 20)
                    .mapToObj(i -> newSegment(Intervals.of("2019-01-%02d/2019-01-%02d", i + 1, i + 2)))
                    .collect(Collectors.toList());
  }

  private static DataSegment newSegment(Interval interval)
  {
    return new DataSegment(
        DATASOURCE,
        interval,
        "version",
        null,
        null,
        null,
        null,
        9,
        10
    );
  }

  private final CompactionInputSpec inputSpec;

  public CompactionInputSpecTest(CompactionInputSpec inputSpec)
  {
    this.inputSpec = inputSpec;
  }

  @Test
  public void testFindInterval()
  {
    Assert.assertEquals(INTERVAL, inputSpec.findInterval(DATASOURCE));
  }

  @Test
  public void testValidateSegments()
  {
    Assert.assertTrue(inputSpec.validateSegments(SEGMENTS));
  }

  @Test
  public void testValidateWrongSegments()
  {
    final List<DataSegment> someSegmentIsMissing = new ArrayList<>(SEGMENTS);
    someSegmentIsMissing.remove(0);
    Assert.assertFalse(inputSpec.validateSegments(someSegmentIsMissing));

    final List<DataSegment> someSegmentIsUnknown = new ArrayList<>(SEGMENTS);
    someSegmentIsUnknown.add(newSegment(Intervals.of("2018-01-01/2018-01-02")));
    Assert.assertFalse(inputSpec.validateSegments(someSegmentIsUnknown));
  }
}
