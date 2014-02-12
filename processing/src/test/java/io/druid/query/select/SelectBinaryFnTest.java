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

package io.druid.query.select;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Result;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 */
public class SelectBinaryFnTest
{
  private static final String segmentId1 = "testSegment";

  private static final String segmentId2 = "testSegment";

  @Test
  public void testApply() throws Exception
  {
    SelectBinaryFn binaryFn = new SelectBinaryFn(QueryGranularity.ALL, new PagingSpec(null, 5));

    Result<SelectResultValue> res1 = new Result<>(
        new DateTime("2013-01-01"),
        new SelectResultValue(
            ImmutableMap.<String, Integer>of(),
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
