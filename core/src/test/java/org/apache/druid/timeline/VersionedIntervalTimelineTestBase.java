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

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.timeline.partition.NumberedOverwritingPartitionChunk;
import org.apache.druid.timeline.partition.NumberedPartitionChunk;
import org.apache.druid.timeline.partition.OvershadowableInteger;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.SingleElementPartitionChunk;
import org.joda.time.Interval;
import org.junit.Assert;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * The purpose of this class is to host the common static methods used in {@link VersionedIntervalTimelineTest} and
 * {@link VersionedIntervalTimelineSpecificDataTest}. When Druid style allows static imports in tests, this
 * functionality should be moved to {@link VersionedIntervalTimelineTest} and then used statically from {@link
 * VersionedIntervalTimelineSpecificDataTest}.
 */
public class VersionedIntervalTimelineTestBase
{
  static void assertValues(
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

      final List<PartitionChunk<OvershadowableInteger>> expectedChunks = Lists.newArrayList(pair.rhs.rhs);
      final List<PartitionChunk<OvershadowableInteger>> actualChunks = Lists.newArrayList(holder.getObject());

      Assert.assertEquals(expectedChunks.size(), actualChunks.size());
      for (int i = 0; i < expectedChunks.size(); i++) {
        // Check partitionNumber first
        Assert.assertEquals(expectedChunks.get(i), actualChunks.get(i));
        final OvershadowableInteger expectedInteger = expectedChunks.get(i).getObject();
        final OvershadowableInteger actualInteger = actualChunks.get(i).getObject();
        Assert.assertEquals(expectedInteger, actualInteger);
      }
    }
  }

  static void assertValues(
      Set<Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>>> expected,
      Set<TimelineObjectHolder<String, OvershadowableInteger>> actual
  )
  {
    Assert.assertEquals("Sizes did not match.", expected.size(), actual.size());

    Set<Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>>> actualSet =
        Sets.newHashSet(
            Iterables.transform(
                actual,
                input -> new Pair<>(input.getInterval(), new Pair<>(input.getVersion(), input.getObject()))
            )
        );

    Assert.assertEquals(expected, actualSet);
  }

  static VersionedIntervalTimeline<String, OvershadowableInteger> makeStringIntegerTimeline()
  {
    return new VersionedIntervalTimeline<>(Ordering.natural());
  }

  VersionedIntervalTimeline<String, OvershadowableInteger> timeline;

  void checkRemove()
  {
    for (TimelineObjectHolder<String, OvershadowableInteger> holder : timeline.findFullyOvershadowed()) {
      // Copy chunks to avoid the ConcurrentModificationException.
      // Note that timeline.remove() modifies the PartitionHolder.
      List<PartitionChunk<OvershadowableInteger>> chunks = FluentIterable.from(holder.getObject()).toList();
      for (PartitionChunk<OvershadowableInteger> chunk : chunks) {
        timeline.remove(holder.getInterval(), holder.getVersion(), chunk);
      }
    }

    Assert.assertTrue(timeline.findFullyOvershadowed().isEmpty());
  }

  TimelineObjectHolder<String, OvershadowableInteger> makeTimelineObjectHolder(
      String interval,
      String version,
      List<PartitionChunk<OvershadowableInteger>> chunks
  )
  {
    return new TimelineObjectHolder<>(
        Intervals.of(interval),
        Intervals.of(interval),
        version,
        new PartitionHolder<>(chunks)
    );
  }

  Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>> createExpected(
      String intervalString,
      String version,
      Integer value
  )
  {
    return createExpected(
        intervalString,
        version,
        Collections.singletonList(makeSingle(version, value))
    );
  }

  Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>> createExpected(
      String intervalString,
      String version,
      List<PartitionChunk<OvershadowableInteger>> values
  )
  {
    return Pair.of(
        Intervals.of(intervalString),
        Pair.of(version, new PartitionHolder<>(values))
    );
  }

  public static PartitionChunk<OvershadowableInteger> makeSingle(String majorVersion, int value)
  {
    return makeSingle(majorVersion, 0, value);
  }

  public static PartitionChunk<OvershadowableInteger> makeSingle(String majorVersion, int partitionNum, int val)
  {
    return new SingleElementPartitionChunk<>(new OvershadowableInteger(majorVersion, partitionNum, val));
  }

  public static PartitionChunk<OvershadowableInteger> makeNumbered(String majorVersion, int partitionNum, int val)
  {
    return makeNumbered(majorVersion, partitionNum, 0, val);
  }

  public static PartitionChunk<OvershadowableInteger> makeNumbered(
      String majorVersion,
      int partitionNum,
      int chunks,
      int val
  )
  {
    return new NumberedPartitionChunk<>(
        partitionNum,
        chunks,
        new OvershadowableInteger(majorVersion, partitionNum, val)
    );
  }

  public static PartitionChunk<OvershadowableInteger> makeNumberedOverwriting(
      String majorVersion,
      int partitionNumOrdinal,
      int val,
      int startRootPartitionId,
      int endRootPartitionId,
      int minorVersion,
      int atomicUpdateGroupSize
  )
  {
    final int partitionNum = PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + partitionNumOrdinal;
    return new NumberedOverwritingPartitionChunk<>(
        partitionNum,
        new OvershadowableInteger(
            majorVersion,
            partitionNum,
            val,
            startRootPartitionId,
            endRootPartitionId,
            minorVersion,
            atomicUpdateGroupSize
        )
    );
  }

  void add(String interval, String version, Integer value)
  {
    add(Intervals.of(interval), version, value);
  }

  void add(Interval interval, String version, Integer value)
  {
    add(interval, version, makeSingle(version, value));
  }

  void add(String interval, String version, PartitionChunk<OvershadowableInteger> value)
  {
    add(Intervals.of(interval), version, value);
  }

  protected void add(Interval interval, String version, PartitionChunk<OvershadowableInteger> value)
  {
    timeline.add(interval, version, value);
  }
}
