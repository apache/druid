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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.timeline.partition.OvershadowableInteger;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.apache.druid.timeline.partition.SingleElementPartitionChunk;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ComplementaryNamespacedVersionedIntervalTimelineTest
{
  private static final String DATASOURCE = "ds1";
  private static final String SUPPORT_DATASOURCE = "ds2";
  private static final String NAMESPACE1 = "ns1";
  private static final String NAMESPACE2 = "ns2";
  private static final String NAMESPACE3 = "ns3";
  private static final String SUB_NAMESPACE1 = "ns1_1";

  private NamespacedVersionedIntervalTimeline<String, OvershadowableInteger> supportTimeline;

  private ComplementaryNamespacedVersionedIntervalTimeline<String, OvershadowableInteger> timeline;

  @Before
  public void setUp()
  {
    supportTimeline = new NamespacedVersionedIntervalTimeline();
    timeline = new ComplementaryNamespacedVersionedIntervalTimeline(DATASOURCE, supportTimeline, SUPPORT_DATASOURCE);

    add(timeline, NAMESPACE1, "2019-09-01/2019-09-03", new OvershadowableInteger("0", 0, 1));
    add(timeline, NAMESPACE1, "2019-09-03/2019-09-06", new OvershadowableInteger("0", 0, 2));
    add(timeline, NAMESPACE1, "2019-09-06/2019-09-09", new OvershadowableInteger("0", 0, 3));
    add(timeline, NAMESPACE2, "2019-08-06/2019-08-09", new OvershadowableInteger("0", 0, 4));

    add(supportTimeline, NAMESPACE2, "2019-08-06/2019-08-09", new OvershadowableInteger("0", 0, 5));
    add(supportTimeline, NAMESPACE2, "2019-08-09/2019-08-12", new OvershadowableInteger("0", 0, 6));
    add(supportTimeline, SUB_NAMESPACE1, "2019-09-01/2019-09-03", new OvershadowableInteger("0", 0, 7));
    add(supportTimeline, SUB_NAMESPACE1, "2019-09-03/2019-09-06", new OvershadowableInteger("0", 0, 8));
    add(supportTimeline, SUB_NAMESPACE1, "2019-09-06/2019-09-09", new OvershadowableInteger("0", 0, 9));
    add(supportTimeline, SUB_NAMESPACE1, "2019-09-09/2019-09-12", new OvershadowableInteger("0", 0, 10));
    add(supportTimeline, NAMESPACE3, "2019-07-01/2019-07-03", new OvershadowableInteger("0", 0, 11));
  }

  @Test
  public void testAlreadyCovered()
  {
    assertValues(
        ImmutableMap.of(
            DATASOURCE, Arrays.asList(
                createExpected("2019-09-01/2019-09-02", new OvershadowableInteger("0", 0, 1))
            )
        ),
        timeline.lookupWithComplementary(ImmutableList.of(Intervals.of("2019-09-01/2019-09-02")))
    );
  }

  @Test
  public void testUseMissingIntervalFromSupportDataSource()
  {
    assertValues(
        ImmutableMap.of(
            DATASOURCE, Arrays.asList(
                createExpected("2019-08-06/2019-08-09", new OvershadowableInteger("0", 0, 4))
            ),
            SUPPORT_DATASOURCE, Arrays.asList(
                createExpected("2019-08-09/2019-08-12", new OvershadowableInteger("0", 0, 6))
            )
        ),
        timeline.lookupWithComplementary(ImmutableList.of(Intervals.of("2019-08-06/2019-08-12")))
    );
  }

  @Test
  public void testUsingMissingNamespaceFromSupportDataSource()
  {
    assertValues(
        ImmutableMap.of(
            SUPPORT_DATASOURCE, Arrays.asList(
                createExpected("2019-07-01/2019-07-03", new OvershadowableInteger("0", 0, 11))
            )
        ),
        timeline.lookupWithComplementary(ImmutableList.of(Intervals.of("2019-07-01/2019-07-12")))
    );
  }

  @Test
  public void testNotUsingSegmentsFromSubNamespace()
  {
    assertValues(
        ImmutableMap.of(
            DATASOURCE, Arrays.asList(
                createExpected("2019-09-01/2019-09-03", new OvershadowableInteger("0", 0, 1)),
                createExpected("2019-09-03/2019-09-06", new OvershadowableInteger("0", 0, 2)),
                createExpected("2019-09-06/2019-09-09", new OvershadowableInteger("0", 0, 3))
            ),
            SUPPORT_DATASOURCE, Arrays.asList(
                createExpected("2019-09-09/2019-09-12", new OvershadowableInteger("0", 0, 10))
            )
        ),
        timeline.lookupWithComplementary(ImmutableList.of(Intervals.of("2019-09-01/2019-09-12")))
    );
  }

  private void add(
      NamespacedVersionedIntervalTimeline timelineToAdd,
      String namespace,
      String intervalString,
      OvershadowableInteger value
  )
  {
    Interval interval = Intervals.of(intervalString);
    timelineToAdd.add(namespace, interval, "1.0", new SingleElementPartitionChunk<>(value));
  }

  private Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>> createExpected(
      String intervalString,
      OvershadowableInteger value
  )
  {
    Interval interval = Intervals.of(intervalString);
    return Pair.of(
        interval,
        Pair.of(
            "1.0",
            new PartitionHolder<>(Collections.singletonList(new SingleElementPartitionChunk<>(value)))
        )
    );
  }

  private void assertValues(
      Map<String, List<Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>>>> expected,
      Map<String, List<TimelineObjectHolder<String, OvershadowableInteger>>> actual
  )
  {
    Assert.assertEquals("Map sizes did not match.", expected.size(), actual.size());
    for (String k : expected.keySet()) {
      assertValues(expected.get(k), actual.get(k));
    }
  }

  private void assertValues(
      List<Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>>> expected,
      List<TimelineObjectHolder<String, OvershadowableInteger>> actual
  )
  {
    Assert.assertEquals("Sizes did not match.", expected.size(), actual.size());

    Iterator<Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>>> expectedIter = expected.iterator();
    Iterator<TimelineObjectHolder<String, OvershadowableInteger>> actualIter = actual.iterator();

    while (expectedIter.hasNext()) {
      Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>> pair = expectedIter.next();
      TimelineObjectHolder<String, OvershadowableInteger> holder = actualIter.next();

      Assert.assertEquals(pair.lhs, holder.getInterval());
      Assert.assertEquals(pair.rhs.lhs, holder.getVersion());
      Assert.assertEquals(pair.rhs.rhs, holder.getObject());
    }
  }
}
