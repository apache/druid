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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
  private static final String SECOND_SUPPORT_DATASOURCE = "ds3";
  private static final String NAMESPACE1 = "ns1";
  private static final String NAMESPACE2 = "ns2";
  private static final String NAMESPACE3 = "ns3";
  private static final String SUB_NAMESPACE1 = "ns1_1";
  private static final String SUB_NAMESPACE3 = "ns3_1";

  private NamespacedVersionedIntervalTimeline<String, OvershadowableInteger> supportTimeline;
  private NamespacedVersionedIntervalTimeline<String, OvershadowableInteger> secondSupportTimeline;

  private Map<String, NamespacedVersionedIntervalTimeline> supportTimelinesByDataSource;

  private List<String> supportDataSourceQueryOrder;

  private ComplementaryNamespacedVersionedIntervalTimeline<String, OvershadowableInteger> timeline;


  @Before
  public void setUp()
  {
    supportTimeline = new NamespacedVersionedIntervalTimeline<>();
    secondSupportTimeline = new NamespacedVersionedIntervalTimeline<>();
    supportTimelinesByDataSource = new HashMap<>();
    supportDataSourceQueryOrder = new ArrayList<>();
    supportDataSourceQueryOrder.add(SUPPORT_DATASOURCE);
    supportDataSourceQueryOrder.add(SECOND_SUPPORT_DATASOURCE);

    add(supportTimeline, NAMESPACE2, "2019-08-06/2019-08-09", new OvershadowableInteger("0", 0, 1));
    add(supportTimeline, NAMESPACE2, "2019-08-09/2019-08-12", new OvershadowableInteger("0", 0, 1));
    add(supportTimeline, NAMESPACE2, "2020-03-01/2020-03-08", new OvershadowableInteger("0", 0, 1));
    add(supportTimeline, NAMESPACE2, "2020-05-01/2020-05-08", new OvershadowableInteger("0", 0, 1));
    add(supportTimeline, SUB_NAMESPACE1, "2019-09-01/2019-09-03", new OvershadowableInteger("0", 0, 1));
    add(supportTimeline, SUB_NAMESPACE1, "2019-09-03/2019-09-06", new OvershadowableInteger("0", 0, 1));
    add(supportTimeline, SUB_NAMESPACE1, "2019-09-09/2019-09-12", new OvershadowableInteger("0", 0, 1));
    add(supportTimeline, NAMESPACE3, "2019-07-01/2019-07-03", new OvershadowableInteger("0", 0, 1));
    add(supportTimeline, NAMESPACE3, "2020-04-01/2020-05-01", new OvershadowableInteger("0", 0, 1));

    add(secondSupportTimeline, NAMESPACE1, "2019-09-01/2019-09-03", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE1, "2019-09-03/2019-09-06", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE1, "2019-09-06/2019-09-09", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE1, "2019-09-09/2019-09-12", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE1, "2019-09-12/2019-09-15", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2019-08-06/2019-08-09", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2019-08-09/2019-08-12", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2019-08-12/2019-08-15", new OvershadowableInteger("0", 0, 1));

    add(secondSupportTimeline, NAMESPACE2, "2020-01-01/2020-02-01", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-02-01/2020-03-01", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-03-01/2020-03-02", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-03-02/2020-03-03", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-03-03/2020-03-04", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-03-04/2020-03-05", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-03-05/2020-03-06", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-03-06/2020-03-07", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-03-07/2020-03-08", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-03-08/2020-03-09", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-03-09/2020-03-10", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-03-10/2020-03-11", new OvershadowableInteger("0", 0, 1));

    add(secondSupportTimeline, NAMESPACE1, "2020-04-01/2020-05-08", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, NAMESPACE2, "2020-04-01/2020-05-08", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, SUB_NAMESPACE3, "2020-04-01/2020-05-01", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, SUB_NAMESPACE3, "2020-05-01/2020-05-06", new OvershadowableInteger("0", 0, 1));

    add(secondSupportTimeline, SUB_NAMESPACE3, "2019-07-01/2019-07-03", new OvershadowableInteger("0", 0, 1));
    add(secondSupportTimeline, SUB_NAMESPACE3, "2019-07-03/2019-07-06", new OvershadowableInteger("0", 0, 1));

    supportTimelinesByDataSource.put(SUPPORT_DATASOURCE, supportTimeline);
    supportTimelinesByDataSource.put(SECOND_SUPPORT_DATASOURCE, secondSupportTimeline);

    timeline = new ComplementaryNamespacedVersionedIntervalTimeline(
            DATASOURCE,
            supportTimelinesByDataSource,
            supportDataSourceQueryOrder);

    add(timeline, NAMESPACE1, "2019-09-01/2019-09-03", new OvershadowableInteger("0", 0, 1));
    add(timeline, NAMESPACE1, "2019-09-03/2019-09-06", new OvershadowableInteger("0", 0, 1));
    add(timeline, NAMESPACE1, "2019-09-06/2019-09-09", new OvershadowableInteger("0", 0, 1));
    add(timeline, NAMESPACE2, "2019-08-06/2019-08-09", new OvershadowableInteger("0", 0, 1));
    add(timeline, NAMESPACE2, "2020-01-01/2020-02-01", new OvershadowableInteger("0", 0, 1));
    add(timeline, NAMESPACE2, "2020-02-01/2020-03-01", new OvershadowableInteger("0", 0, 1));
    add(timeline, NAMESPACE2, "2020-03-01/2020-04-01", new OvershadowableInteger("0", 0, 1));
    add(timeline, NAMESPACE2, "2020-04-01/2020-05-01", new OvershadowableInteger("0", 0, 1));
  }

  @Test
  public void testAlreadyCovered()
  {
    assertValues(
        ImmutableMap.of(
            DATASOURCE, Arrays.asList(
                            createExpected("2019-09-01/2019-09-03", new OvershadowableInteger("0", 0, 1))
            )
        ),
            timeline.lookupWithComplementary(ImmutableList.of(Intervals.of("2019-09-01/2019-09-03")))
    );
  }

  @Test
  public void testUseMissingIntervalFromSupportDataSource()
  {
    assertValues(
        ImmutableMap.of(
            DATASOURCE, Arrays.asList(
                            createExpected("2019-08-06/2019-08-09", new OvershadowableInteger("0", 0, 1))
            ),
            SUPPORT_DATASOURCE, Arrays.asList(
                            createExpected("2019-08-09/2019-08-12", new OvershadowableInteger("0", 0, 1))
                    ),
                    SECOND_SUPPORT_DATASOURCE, Arrays.asList(
                            createExpected("2019-08-12/2019-08-15", new OvershadowableInteger("0", 0, 1))
            )
        ),
            timeline.lookupWithComplementary(ImmutableList.of(Intervals.of("2019-08-01/2019-08-30")))
    );
  }

  @Test
  public void testMultipleNamespaces()
  {
    assertValues(
            ImmutableMap.of(
                    DATASOURCE, Arrays.asList(
                            createExpected("2020-04-01/2020-05-01", new OvershadowableInteger("0", 0, 1))
                    ),
                    SUPPORT_DATASOURCE, Arrays.asList(
                            createExpected("2020-05-01/2020-05-08", new OvershadowableInteger("0", 0, 1)),
                            createExpected("2020-04-01/2020-05-01", new OvershadowableInteger("0", 0, 1))
                    ),
                    SECOND_SUPPORT_DATASOURCE, Arrays.asList(
                            createExpected("2020-04-01/2020-05-08", new OvershadowableInteger("0", 0, 1)),
                            createExpected("2020-05-01/2020-05-06", new OvershadowableInteger("0", 0, 1))
                    )
            ),
            timeline.lookupWithComplementary(ImmutableList.of(Intervals.of("2020-04-01/2020-06-01")))
    );
  }

  @Test
  public void testUsingMissingNamespaceFromSupportDataSource()
  {
    assertValues(
        ImmutableMap.of(
            SUPPORT_DATASOURCE, Arrays.asList(
                            createExpected("2019-07-01/2019-07-03", new OvershadowableInteger("0", 0, 1))
                    ),
                    SECOND_SUPPORT_DATASOURCE, Arrays.asList(
                            createExpected("2019-07-03/2019-07-06", new OvershadowableInteger("0", 0, 1))
            )
        ),
        timeline.lookupWithComplementary(ImmutableList.of(Intervals.of("2019-07-01/2019-07-12")))
    );
  }

  @Test
  public void testSegmentsDoNotOvershootEndDate()
  {
    assertValues(
            ImmutableMap.of(
                    DATASOURCE, Arrays.asList(
                            createExpected("2020-01-01/2020-02-01", new OvershadowableInteger("0", 0, 1)),
                            createExpected("2020-02-01/2020-03-01", new OvershadowableInteger("0", 0, 1))
                    ),
                    SUPPORT_DATASOURCE, Arrays.asList(
                            createExpected("2020-03-01/2020-03-08", new OvershadowableInteger("0", 0, 1))
                    ),
                    SECOND_SUPPORT_DATASOURCE, Arrays.asList(
                            createExpected("2020-03-08/2020-03-09", new OvershadowableInteger("0", 0, 1)),
                            createExpected("2020-03-09/2020-03-10", new OvershadowableInteger("0", 0, 1)),
                            createExpected("2020-03-10/2020-03-11", new OvershadowableInteger("0", 0, 1))
                    )
            ),
            timeline.lookupWithComplementary(ImmutableList.of(Intervals.of("2020-01-01/2020-03-11")))
    );
  }

  @Test
  public void testNotUsingSegmentsFromSubNamespace()
  {
    assertValues(
        ImmutableMap.of(
            DATASOURCE, Arrays.asList(
                createExpected("2019-09-01/2019-09-03", new OvershadowableInteger("0", 0, 1)),
                            createExpected("2019-09-03/2019-09-06", new OvershadowableInteger("0", 0, 1)),
                            createExpected("2019-09-06/2019-09-09", new OvershadowableInteger("0", 0, 1))
            ),
            SUPPORT_DATASOURCE, Arrays.asList(
                            createExpected("2019-09-09/2019-09-12", new OvershadowableInteger("0", 0, 1))
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
          OvershadowableInteger value)
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
