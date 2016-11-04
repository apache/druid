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

package io.druid.query.select;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.ISE;
import io.druid.query.Result;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/**
 */
public class SelectBinaryFnTest
{
  private static final String segmentId1 = "testSegment";

  private static final String segmentId2 = "testSegment";

  @Test
  public void testApply() throws Exception
  {
    SelectBinaryFn binaryFn = new SelectBinaryFn(QueryGranularities.ALL, new PagingSpec(null, 5), false);

    Result<SelectResultValue> res1 = new Result<>(
        new DateTime("2013-01-01"),
        new SelectResultValue(
            ImmutableMap.<String, Integer>of(),
            Sets.newHashSet("first", "fourth"),
            Sets.newHashSet("sixth"),
            Arrays.asList(
                new EventHolder(
                    segmentId1,
                    0,
                    ImmutableMap.<String, Object>of(
                        EventHolder.timestampKey,
                        new DateTime("2013-01-01T00"),
                        "dim",
                        "first"
                    )
                ),
                new EventHolder(
                    segmentId1,
                    1,
                    ImmutableMap.<String, Object>of(
                        EventHolder.timestampKey,
                        new DateTime("2013-01-01T03"),
                        "dim",
                        "fourth"
                    )
                ),
                new EventHolder(
                    segmentId1,
                    2,
                    ImmutableMap.<String, Object>of(
                        EventHolder.timestampKey,
                        new DateTime("2013-01-01T05"),
                        "dim",
                        "sixth"
                    )
                )
            )
        )
    );


    Result<SelectResultValue> res2 = new Result<>(
        new DateTime("2013-01-01"),
        new SelectResultValue(
            ImmutableMap.<String, Integer>of(),
            Sets.newHashSet("second", "third"),
            Sets.newHashSet("fifth"),
            Arrays.asList(
                new EventHolder(
                    segmentId2,
                    0,
                    ImmutableMap.<String, Object>of(
                        EventHolder.timestampKey,
                        new DateTime("2013-01-01T00"),
                        "dim",
                        "second"
                    )
                ),
                new EventHolder(
                    segmentId2,
                    1,
                    ImmutableMap.<String, Object>of(
                        EventHolder.timestampKey,
                        new DateTime("2013-01-01T02"),
                        "dim",
                        "third"
                    )
                ),
                new EventHolder(
                    segmentId2,
                    2,
                    ImmutableMap.<String, Object>of(
                        EventHolder.timestampKey,
                        new DateTime("2013-01-01T04"),
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
    expectedPageIds.put(segmentId1, 0);
    expectedPageIds.put(segmentId2, 0);
    expectedPageIds.put(segmentId2, 1);
    expectedPageIds.put(segmentId1, 1);
    expectedPageIds.put(segmentId2, 2);

    Iterator<String> exSegmentIter = expectedPageIds.keySet().iterator();
    Iterator<String> acSegmentIter = merged.getValue().getPagingIdentifiers().keySet().iterator();

    verifyIters(exSegmentIter, acSegmentIter);

    Iterator<Integer> exOffsetIter = expectedPageIds.values().iterator();
    Iterator<Integer> acOffsetIter = merged.getValue().getPagingIdentifiers().values().iterator();

    verifyIters(exOffsetIter, acOffsetIter);

    List<EventHolder> exEvents = Arrays.<EventHolder>asList(
        new EventHolder(
            segmentId1,
            0,
            ImmutableMap.<String, Object>of(
                EventHolder.timestampKey,
                new DateTime("2013-01-01T00"), "dim", "first"
            )
        ),
        new EventHolder(
            segmentId2,
            0,
            ImmutableMap.<String, Object>of(
                EventHolder.timestampKey,
                new DateTime("2013-01-01T00"),
                "dim",
                "second"
            )
        ),
        new EventHolder(
            segmentId2,
            1,
            ImmutableMap.<String, Object>of(
                EventHolder.timestampKey,
                new DateTime("2013-01-01T02"),
                "dim",
                "third"
            )
        ),
        new EventHolder(
            segmentId1,
            1,
            ImmutableMap.<String, Object>of(
                EventHolder.timestampKey,
                new DateTime("2013-01-01T03"),
                "dim",
                "fourth"
            )
        ),
        new EventHolder(
            segmentId2,
            2,
            ImmutableMap.<String, Object>of(
                EventHolder.timestampKey,
                new DateTime("2013-01-01T04"),
                "dim",
                "fifth"
            )
        )
    );

    List<EventHolder> acEvents = merged.getValue().getEvents();


    verifyEvents(exEvents, acEvents);
  }

  @Test
  public void testColumnMerge() throws Exception
  {
    SelectBinaryFn binaryFn = new SelectBinaryFn(QueryGranularities.ALL, new PagingSpec(null, 5), false);

    Result<SelectResultValue> res1 = new Result<>(
        new DateTime("2013-01-01"),
        new SelectResultValue(
            ImmutableMap.<String, Integer>of(),
            Sets.newHashSet("first", "second", "fourth"),
            Sets.newHashSet("eight", "nineth"),
            Lists.<EventHolder>newArrayList(
                new EventHolder(
                    segmentId1,
                    0,
                    ImmutableMap.<String, Object>of(
                        EventHolder.timestampKey,
                        new DateTime("2013-01-01T00"), "dim", "first"
                    )
                ))
        )
    );

    Result<SelectResultValue> res2 = new Result<>(
        new DateTime("2013-01-01"),
        new SelectResultValue(
            ImmutableMap.<String, Integer>of(),
            Sets.newHashSet("third", "second", "fifth"),
            Sets.newHashSet("seventh"),
            Lists.<EventHolder>newArrayList(
                new EventHolder(
                    segmentId2,
                    0,
                    ImmutableMap.<String, Object>of(
                        EventHolder.timestampKey,
                        new DateTime("2013-01-01T00"),
                        "dim",
                        "second"
                    )
                ))
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
