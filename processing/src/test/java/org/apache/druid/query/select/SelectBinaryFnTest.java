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

package org.apache.druid.query.select;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Result;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/**
 */
public class SelectBinaryFnTest
{
  private static final String SEGMENT_ID1 = "testSegment1";

  private static final String SEGMENT_ID2 = "testSegment2";

  @Test
  public void testApply()
  {
    SelectBinaryFn binaryFn = new SelectBinaryFn(Granularities.ALL, new PagingSpec(null, 5), false);

    Result<SelectResultValue> res1 = new Result<>(
        DateTimes.of("2013-01-01"),
        new SelectResultValue(
            ImmutableMap.of(),
            Sets.newHashSet("first", "fourth"),
            Sets.newHashSet("sixth"),
            Arrays.asList(
                new EventHolder(
                        SEGMENT_ID1,
                    0,
                    ImmutableMap.of(
                        EventHolder.TIMESTAMP_KEY,
                        DateTimes.of("2013-01-01T00"),
                        "dim",
                        "first"
                    )
                ),
                new EventHolder(
                    SEGMENT_ID1,
                    1,
                    ImmutableMap.of(
                        EventHolder.TIMESTAMP_KEY,
                        DateTimes.of("2013-01-01T03"),
                        "dim",
                        "fourth"
                    )
                ),
                new EventHolder(
                    SEGMENT_ID1,
                    2,
                    ImmutableMap.of(
                        EventHolder.TIMESTAMP_KEY,
                        DateTimes.of("2013-01-01T05"),
                        "dim",
                        "sixth"
                    )
                )
            )
        )
    );


    Result<SelectResultValue> res2 = new Result<>(
        DateTimes.of("2013-01-01"),
        new SelectResultValue(
            ImmutableMap.of(),
            Sets.newHashSet("second", "third"),
            Sets.newHashSet("fifth"),
            Arrays.asList(
                new EventHolder(
                    SEGMENT_ID2,
                    0,
                    ImmutableMap.of(
                        EventHolder.TIMESTAMP_KEY,
                        DateTimes.of("2013-01-01T00"),
                        "dim",
                        "second"
                    )
                ),
                new EventHolder(
                    SEGMENT_ID2,
                    1,
                    ImmutableMap.of(
                        EventHolder.TIMESTAMP_KEY,
                        DateTimes.of("2013-01-01T02"),
                        "dim",
                        "third"
                    )
                ),
                new EventHolder(
                    SEGMENT_ID2,
                    2,
                    ImmutableMap.of(
                        EventHolder.TIMESTAMP_KEY,
                        DateTimes.of("2013-01-01T04"),
                        "dim",
                        "fifth"
                    )
                )
            )
        )
    );

    Result<SelectResultValue> merged = binaryFn.apply(res1, res2);

    Assert.assertEquals(res1.getTimestamp(), merged.getTimestamp());

    LinkedHashMap<String, Integer> expectedPageIds = Maps.newLinkedHashMap();
    expectedPageIds.put(SEGMENT_ID1, 1);
    expectedPageIds.put(SEGMENT_ID2, 2);

    Iterator<String> exSegmentIter = expectedPageIds.keySet().iterator();
    Iterator<String> acSegmentIter = merged.getValue().getPagingIdentifiers().keySet().iterator();

    verifyIters(exSegmentIter, acSegmentIter);

    Iterator<Integer> exOffsetIter = expectedPageIds.values().iterator();
    Iterator<Integer> acOffsetIter = merged.getValue().getPagingIdentifiers().values().iterator();

    verifyIters(exOffsetIter, acOffsetIter);

    List<EventHolder> exEvents = Arrays.asList(
        new EventHolder(
            SEGMENT_ID1,
            0,
            ImmutableMap.of(
                EventHolder.TIMESTAMP_KEY,
                DateTimes.of("2013-01-01T00"), "dim", "first"
            )
        ),
        new EventHolder(
            SEGMENT_ID2,
            0,
            ImmutableMap.of(
                EventHolder.TIMESTAMP_KEY,
                DateTimes.of("2013-01-01T00"),
                "dim",
                "second"
            )
        ),
        new EventHolder(
            SEGMENT_ID2,
            1,
            ImmutableMap.of(
                EventHolder.TIMESTAMP_KEY,
                DateTimes.of("2013-01-01T02"),
                "dim",
                "third"
            )
        ),
        new EventHolder(
            SEGMENT_ID1,
            1,
            ImmutableMap.of(
                EventHolder.TIMESTAMP_KEY,
                DateTimes.of("2013-01-01T03"),
                "dim",
                "fourth"
            )
        ),
        new EventHolder(
            SEGMENT_ID2,
            2,
            ImmutableMap.of(
                EventHolder.TIMESTAMP_KEY,
                DateTimes.of("2013-01-01T04"),
                "dim",
                "fifth"
            )
        )
    );

    List<EventHolder> acEvents = merged.getValue().getEvents();


    verifyEvents(exEvents, acEvents);
  }

  @Test
  public void testColumnMerge()
  {
    SelectBinaryFn binaryFn = new SelectBinaryFn(Granularities.ALL, new PagingSpec(null, 5), false);

    Result<SelectResultValue> res1 = new Result<>(
        DateTimes.of("2013-01-01"),
        new SelectResultValue(
            ImmutableMap.of(),
            Sets.newHashSet("first", "second", "fourth"),
            Sets.newHashSet("eight", "nineth"),
            Collections.singletonList(
                new EventHolder(
                    SEGMENT_ID1,
                    0,
                    ImmutableMap.of(EventHolder.TIMESTAMP_KEY, DateTimes.of("2013-01-01T00"), "dim", "first")
                )
            )
        )
    );

    Result<SelectResultValue> res2 = new Result<>(
        DateTimes.of("2013-01-01"),
        new SelectResultValue(
            ImmutableMap.of(),
            Sets.newHashSet("third", "second", "fifth"),
            Sets.newHashSet("seventh"),
            Collections.singletonList(
                new EventHolder(
                    SEGMENT_ID2,
                    0,
                    ImmutableMap.of(EventHolder.TIMESTAMP_KEY, DateTimes.of("2013-01-01T00"), "dim", "second")
                )
            )
        )
    );

    Result<SelectResultValue> merged = binaryFn.apply(res1, res2);

    Set<String> exDimensions = Sets.newHashSet("first", "second", "fourth", "third", "fifth");
    Set<String> exMetrics = Sets.newHashSet("eight", "nineth", "seventh");

    Set<String> acDimensions = merged.getValue().getDimensions();
    Set<String> acMetrics = merged.getValue().getMetrics();

    Assert.assertEquals(exDimensions, acDimensions);
    Assert.assertEquals(exMetrics, acMetrics);
  }

  private void verifyIters(Iterator iter1, Iterator iter2)
  {
    while (iter1.hasNext()) {
      Assert.assertEquals(iter1.next(), iter2.next());
    }

    if (iter2.hasNext()) {
      throw new ISE("This should be empty!");
    }
  }

  private void verifyEvents(List<EventHolder> events1, List<EventHolder> events2)
  {
    Iterator<EventHolder> ex = events1.iterator();
    Iterator<EventHolder> ac = events2.iterator();

    verifyIters(ex, ac);
  }
}
