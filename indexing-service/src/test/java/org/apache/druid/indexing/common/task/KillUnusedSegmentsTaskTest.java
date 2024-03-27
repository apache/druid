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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.KillTaskReport;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.timeline.DataSegment;
import org.assertj.core.api.Assertions;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KillUnusedSegmentsTaskTest extends IngestionTestBase
{
  private static final String DATA_SOURCE = "dataSource";

  private TestTaskRunner taskRunner;

  private DataSegment segment1;
  private DataSegment segment2;
  private DataSegment segment3;
  private DataSegment segment4;

  @Before
  public void setup()
  {
    taskRunner = new TestTaskRunner();

    final String version = DateTimes.nowUtc().toString();
    segment1 = newSegment(Intervals.of("2019-01-01/2019-02-01"), version);
    segment2 = newSegment(Intervals.of("2019-02-01/2019-03-01"), version);
    segment3 = newSegment(Intervals.of("2019-03-01/2019-04-01"), version);
    segment4 = newSegment(Intervals.of("2019-04-01/2019-05-01"), version);
  }

  @Test
  public void testKill() throws Exception
  {
    final Set<DataSegment> segments = ImmutableSet.of(segment1, segment2, segment3, segment4);
    final Set<DataSegment> announced = getMetadataStorageCoordinator().commitSegments(segments);
    Assert.assertEquals(segments, announced);

    Assert.assertTrue(
        getSegmentsMetadataManager().markSegmentAsUnused(
            segment2.getId()
        )
    );
    Assert.assertTrue(
        getSegmentsMetadataManager().markSegmentAsUnused(
            segment3.getId()
        )
    );

    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2019-03-01/2019-04-01"))
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
          DATA_SOURCE,
          Intervals.of("2019/2020"),
          null,
          null,
          null
        );

    Assert.assertEquals(ImmutableList.of(segment2), observedUnusedSegments);
    Assertions.assertThat(
        getMetadataStorageCoordinator().retrieveUsedSegmentsForInterval(
            DATA_SOURCE,
            Intervals.of("2019/2020"),
            Segments.ONLY_VISIBLE
        )
    ).containsExactlyInAnyOrder(segment1, segment4);

    Assert.assertEquals(
        new KillTaskReport.Stats(1, 2, 0),
        getReportedStats()
    );
  }

  @Test
  public void testKillWithMarkUnused() throws Exception
  {
    final Set<DataSegment> segments = ImmutableSet.of(segment1, segment2, segment3, segment4);
    final Set<DataSegment> announced = getMetadataStorageCoordinator().commitSegments(segments);
    Assert.assertEquals(segments, announced);

    Assert.assertTrue(
        getSegmentsMetadataManager().markSegmentAsUnused(
            segment2.getId()
        )
    );

    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2019-03-01/2019-04-01"))
        .markAsUnused(true)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
            DATA_SOURCE,
            Intervals.of("2019/2020"),
            null,
            null,
            null
        );

    Assert.assertEquals(ImmutableList.of(segment2), observedUnusedSegments);
    Assertions.assertThat(
        getMetadataStorageCoordinator().retrieveUsedSegmentsForInterval(
            DATA_SOURCE,
            Intervals.of("2019/2020"),
            Segments.ONLY_VISIBLE
        )
    ).containsExactlyInAnyOrder(segment1, segment4);

    Assert.assertEquals(
        new KillTaskReport.Stats(1, 2, 1),
        getReportedStats()
    );
  }

  @Test
  public void testKillSegmentsWithVersions() throws Exception
  {
    final DateTime now = DateTimes.nowUtc();
    final String v1 = now.toString();
    final String v2 = now.minusHours(2).toString();
    final String v3 = now.minusHours(3).toString();

    final DataSegment segment1V1 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v1);
    final DataSegment segment2V1 = newSegment(Intervals.of("2019-02-01/2019-03-01"), v1);
    final DataSegment segment3V1 = newSegment(Intervals.of("2019-03-01/2019-04-01"), v1);
    final DataSegment segment4V2 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v2);
    final DataSegment segment5V3 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v3);

    final Set<DataSegment> segments = ImmutableSet.of(segment1V1, segment2V1, segment3V1, segment4V2, segment5V3);

    Assert.assertEquals(segments, getMetadataStorageCoordinator().commitSegments(segments));
    Assert.assertEquals(
        segments.size(),
        getSegmentsMetadataManager().markSegmentsAsUnused(
            segments.stream().map(DataSegment::getId).collect(Collectors.toSet())
        )
    );

    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018/2020"))
        .versions(ImmutableList.of(v1, v2))
        .batchSize(3)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());
    Assert.assertEquals(
        new KillTaskReport.Stats(4, 3, 0),
        getReportedStats()
    );

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
          DATA_SOURCE,
          Intervals.of("2018/2020"),
          null,
          null
        );

    Assert.assertEquals(ImmutableSet.of(segment5V3), new HashSet<>(observedUnusedSegments));
  }

  @Test
  public void testKillSegmentsWithEmptyVersions() throws Exception
  {
    final DateTime now = DateTimes.nowUtc();
    final String v1 = now.toString();
    final String v2 = now.minusHours(2).toString();
    final String v3 = now.minusHours(3).toString();

    final DataSegment segment1V1 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v1);
    final DataSegment segment2V1 = newSegment(Intervals.of("2019-02-01/2019-03-01"), v1);
    final DataSegment segment3V1 = newSegment(Intervals.of("2019-03-01/2019-04-01"), v1);
    final DataSegment segment4V2 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v2);
    final DataSegment segment5V3 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v3);

    final Set<DataSegment> segments = ImmutableSet.of(segment1V1, segment2V1, segment3V1, segment4V2, segment5V3);

    Assert.assertEquals(segments, getMetadataStorageCoordinator().commitSegments(segments));
    Assert.assertEquals(
        segments.size(),
        getSegmentsMetadataManager().markSegmentsAsUnused(
            segments.stream().map(DataSegment::getId).collect(Collectors.toSet())
        )
    );

    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018/2020"))
        .versions(ImmutableList.of())
        .batchSize(3)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());
    Assert.assertEquals(
        new KillTaskReport.Stats(0, 1, 0),
        getReportedStats()
    );

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
            DATA_SOURCE,
            Intervals.of("2018/2020"),
            null,
            null
        );

    Assert.assertEquals(segments, new HashSet<>(observedUnusedSegments));
  }

  @Test
  public void testKillSegmentsWithVersionsAndLimit() throws Exception
  {
    final DateTime now = DateTimes.nowUtc();
    final String v1 = now.toString();
    final String v2 = now.minusHours(2).toString();
    final String v3 = now.minusHours(3).toString();

    final DataSegment segment1V1 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v1);
    final DataSegment segment2V1 = newSegment(Intervals.of("2019-02-01/2019-03-01"), v1);
    final DataSegment segment3V1 = newSegment(Intervals.of("2019-03-01/2019-04-01"), v1);
    final DataSegment segment4V2 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v2);
    final DataSegment segment5V3 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v3);

    final Set<DataSegment> segments = ImmutableSet.of(segment1V1, segment2V1, segment3V1, segment4V2, segment5V3);

    Assert.assertEquals(segments, getMetadataStorageCoordinator().commitSegments(segments));
    Assert.assertEquals(
        segments.size(),
        getSegmentsMetadataManager().markSegmentsAsUnused(
            segments.stream().map(DataSegment::getId).collect(Collectors.toSet())
        )
    );

    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018/2020"))
        .versions(ImmutableList.of(v1))
        .batchSize(3)
        .limit(2)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());
    Assert.assertEquals(
        new KillTaskReport.Stats(2, 1, 0),
        getReportedStats()
    );

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
          DATA_SOURCE,
          Intervals.of("2018/2020"),
          null,
          null
      );

    Assert.assertEquals(ImmutableSet.of(segment3V1, segment4V2, segment5V3), new HashSet<>(observedUnusedSegments));
  }

  @Test
  public void testKillWithNonExistentVersion() throws Exception
  {
    final DateTime now = DateTimes.nowUtc();
    final String v1 = now.toString();
    final String v2 = now.minusHours(2).toString();
    final String v3 = now.minusHours(3).toString();

    final DataSegment segment1V1 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v1);
    final DataSegment segment2V1 = newSegment(Intervals.of("2019-02-01/2019-03-01"), v1);
    final DataSegment segment3V1 = newSegment(Intervals.of("2019-03-01/2019-04-01"), v1);
    final DataSegment segment4V2 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v2);
    final DataSegment segment5V3 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v3);

    final Set<DataSegment> segments = ImmutableSet.of(segment1V1, segment2V1, segment3V1, segment4V2, segment5V3);

    Assert.assertEquals(segments, getMetadataStorageCoordinator().commitSegments(segments));
    Assert.assertEquals(
        segments.size(),
        getSegmentsMetadataManager().markSegmentsAsUnused(
            segments.stream().map(DataSegment::getId).collect(Collectors.toSet())
        )
    );

    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018/2020"))
        .versions(ImmutableList.of(now.plusDays(100).toString()))
        .batchSize(3)
        .limit(2)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());
    Assert.assertEquals(
        new KillTaskReport.Stats(0, 1, 0),
        getReportedStats()
    );

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
          DATA_SOURCE,
          Intervals.of("2018/2020"),
          null,
          null
      );

    Assert.assertEquals(segments, new HashSet<>(observedUnusedSegments));
  }

  /**
   * {@code segment1}, {@code segment2} and {@code segment3} have different versions, but share the same load spec.
   * {@code segment1} and {@code segment2} are unused segments, while {@code segment3} is a used segment.
   * When a kill task is submitted, the unused segments {@code segment1} and {@code segment2} should be deleted from the
   * metadata store, but should be retained in deep storage as the load spec is used by {@code segment3}.
   */
  @Test
  public void testKillUnusedSegmentsWithUsedLoadSpec() throws Exception
  {
    final DateTime now = DateTimes.nowUtc();
    final String v1 = now.toString();
    final String v2 = now.minusHours(2).toString();
    final String v3 = now.minusHours(3).toString();

    final DataSegment segment1V1 = newSegment(Intervals.of("2019-01-01/2019-02-01"), v1, ImmutableMap.of("foo", "1"));
    final DataSegment segment2V2 = newSegment(Intervals.of("2019-02-01/2019-03-01"), v2, ImmutableMap.of("foo", "1"));
    final DataSegment segment3V3 = newSegment(Intervals.of("2019-03-01/2019-04-01"), v3, ImmutableMap.of("foo", "1"));

    final Set<DataSegment> segments = ImmutableSet.of(segment1V1, segment2V2, segment3V3);
    final Set<DataSegment> unusedSegments = ImmutableSet.of(segment1V1, segment2V2);

    Assert.assertEquals(segments, getMetadataStorageCoordinator().commitSegments(segments));
    Assert.assertEquals(
        unusedSegments.size(),
        getSegmentsMetadataManager().markSegmentsAsUnused(
            unusedSegments.stream().map(DataSegment::getId).collect(Collectors.toSet())
        )
    );

    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018/2020"))
        .versions(ImmutableList.of(v1, v2))
        .limit(100)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());
    Assert.assertEquals(
        new KillTaskReport.Stats(0, 1, 0),
        getReportedStats()
    );

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
          DATA_SOURCE,
          Intervals.of("2018/2020"),
          null,
          null
      );

    Assert.assertEquals(ImmutableSet.of(), new HashSet<>(observedUnusedSegments));
  }

  @Test
  public void testGetInputSourceResources()
  {
    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2019-03-01/2019-04-01"))
        .markAsUnused(true)
        .build();
    Assert.assertTrue(task.getInputSourceResources().isEmpty());
  }

  @Test
  public void testKillBatchSizeOneAndLimit4() throws Exception
  {
    final Set<DataSegment> segments = ImmutableSet.of(segment1, segment2, segment3, segment4);
    final Set<DataSegment> announced = getMetadataStorageCoordinator().commitSegments(segments);

    Assert.assertEquals(segments, announced);
    Assert.assertEquals(
        segments.size(),
        getSegmentsMetadataManager().markAsUnusedSegmentsInInterval(
            DATA_SOURCE,
            Intervals.of("2018-01-01/2020-01-01"),
            null
        )
    );

    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018-01-01/2020-01-01"))
        .batchSize(1)
        .limit(4)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());

    // we expect ALL tasks to be deleted

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
            DATA_SOURCE,
            Intervals.of("2019/2020"),
            null,
            null
        );

    Assert.assertEquals(Collections.emptyList(), observedUnusedSegments);
    Assert.assertEquals(
        new KillTaskReport.Stats(4, 4, 0),
        getReportedStats()
    );
  }

  /**
   * Test kill functionality of multiple unused segments in a wide interval with different {@code used_status_last_updated}
   * timestamps. A kill task submitted with null {@code maxUsedStatusLastUpdatedTime} will kill all the unused segments in the kill
   * interval.
   */
  @Test
  public void testKillMultipleUnusedSegmentsWithNullMaxUsedStatusLastUpdatedTime() throws Exception
  {
    final Set<DataSegment> segments = ImmutableSet.of(segment1, segment2, segment3, segment4);
    final Set<DataSegment> announced = getMetadataStorageCoordinator().commitSegments(segments);

    Assert.assertEquals(segments, announced);

    Assert.assertEquals(
        1,
        getSegmentsMetadataManager().markAsUnusedSegmentsInInterval(
            DATA_SOURCE,
            segment1.getInterval(),
            null
        )
    );

    Assert.assertEquals(
        1,
        getSegmentsMetadataManager().markAsUnusedSegmentsInInterval(
            DATA_SOURCE,
            segment4.getInterval(),
            null
        )
    );

    Assert.assertEquals(
        1,
        getSegmentsMetadataManager().markAsUnusedSegmentsInInterval(
            DATA_SOURCE,
            segment3.getInterval(),
            null
        )
    );

    final List<Interval> segmentIntervals = segments.stream()
                                                    .map(DataSegment::getInterval)
                                                    .collect(Collectors.toList());

    final Interval umbrellaInterval = JodaUtils.umbrellaInterval(segmentIntervals);

    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(umbrellaInterval)
        .batchSize(1)
        .limit(10)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
            DATA_SOURCE,
            umbrellaInterval,
            null,
            null
        );

    Assert.assertEquals(ImmutableList.of(), observedUnusedSegments);
    Assert.assertEquals(
        new KillTaskReport.Stats(3, 4, 0),
        getReportedStats()
    );
  }

  /**
   * Test kill functionality of multiple unused segments in a wide interval with different {@code used_status_last_updated}
   * timestamps. Consider:
   * <li> {@code segment1}, {@code segment2} and {@code segment3} have t1, t2 and t3 {@code used_status_last_updated} timestamps
   * respectively, where  t1 < t2 < t3 </li>
   * <li> {@code segment4} is a used segment and therefore shouldn't be killed </li>
   *
   * <p>
   * A kill task submitted with t2 as the {@code maxUsedStatusLastUpdatedTime} should only kill {@code segment1} and {@code segment2}
   * After that, a kill task submitted with t3 as the {@code maxUsedStatusLastUpdatedTime} should kill {@code segment3}.
   * </p>
   */
  @Test
  public void testKillMultipleUnusedSegmentsWithDifferentMaxUsedStatusLastUpdatedTime() throws Exception
  {
    final Set<DataSegment> segments = ImmutableSet.of(segment1, segment2, segment3, segment4);
    final Set<DataSegment> announced = getMetadataStorageCoordinator().commitSegments(segments);

    Assert.assertEquals(segments, announced);

    Assert.assertEquals(
        1,
        getSegmentsMetadataManager().markAsUnusedSegmentsInInterval(
            DATA_SOURCE,
            segment1.getInterval(),
            null
        )
    );

    Assert.assertEquals(
        1,
        getSegmentsMetadataManager().markAsUnusedSegmentsInInterval(
            DATA_SOURCE,
            segment4.getInterval(),
            null
        )
    );

    final DateTime lastUpdatedTime1 = DateTimes.nowUtc();
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(segment1.getId().toString(), lastUpdatedTime1);
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(segment4.getId().toString(), lastUpdatedTime1);

    // Now mark the third segment as unused
    Assert.assertEquals(
        1,
        getSegmentsMetadataManager().markAsUnusedSegmentsInInterval(
            DATA_SOURCE,
            segment3.getInterval(),
            null
        )
    );

    final DateTime lastUpdatedTime2 = DateTimes.nowUtc();
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(segment3.getId().toString(), lastUpdatedTime2);

    final List<Interval> segmentIntervals = segments.stream()
                                                    .map(DataSegment::getInterval)
                                                    .collect(Collectors.toList());

    final Interval umbrellaInterval = JodaUtils.umbrellaInterval(segmentIntervals);

    final KillUnusedSegmentsTask task1 = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(umbrellaInterval)
        .batchSize(1)
        .limit(10)
        .maxUsedStatusLastUpdatedTime(lastUpdatedTime1)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task1).get().getStatusCode());

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
            DATA_SOURCE,
            umbrellaInterval,
            null,
            null
        );

    Assert.assertEquals(ImmutableList.of(segment3), observedUnusedSegments);
    Assert.assertEquals(
        new KillTaskReport.Stats(2, 3, 0),
        getReportedStats()
    );

    final KillUnusedSegmentsTask task2 = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(umbrellaInterval)
        .batchSize(1)
        .limit(10)
        .maxUsedStatusLastUpdatedTime(lastUpdatedTime2)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task2).get().getStatusCode());

    final List<DataSegment> observedUnusedSegments2 =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
            DATA_SOURCE,
            umbrellaInterval,
            null,
            null
        );

    Assert.assertEquals(ImmutableList.of(), observedUnusedSegments2);
    Assert.assertEquals(
        new KillTaskReport.Stats(1, 2, 0),
        getReportedStats()
    );
  }

  /**
   * Similar to {@link #testKillMultipleUnusedSegmentsWithDifferentMaxUsedStatusLastUpdatedTime()}}, but with a different setup.
   * <p>
   * Tests kill functionality of multiple unused segments in a wide interval with different {@code used_status_last_updated}
   * timestamps. Consider:
   * <li> {@code segment1} and {@code segment4} have t1 {@code used_status_last_updated} timestamp
   * <li> {@code segment2} and {@code segment3} have t2 {@code used_status_last_updated} timestamp, where t1 < t2 </li>
   *
   * <p>
   * A kill task submitted with t1 as the {@code maxUsedStatusLastUpdatedTime} should only kill {@code segment1} and {@code segment4}
   * After that, a kill task submitted with t2 as the {@code maxUsedStatusLastUpdatedTime} should kill {@code segment2} and {@code segment3}.
   * </p>
   */
  @Test
  public void testKillMultipleUnusedSegmentsWithDifferentMaxUsedStatusLastUpdatedTime2() throws Exception
  {
    final Set<DataSegment> segments = ImmutableSet.of(segment1, segment2, segment3, segment4);
    final Set<DataSegment> announced = getMetadataStorageCoordinator().commitSegments(segments);

    Assert.assertEquals(segments, announced);

    Assert.assertEquals(
        2,
        getSegmentsMetadataManager().markSegmentsAsUnused(
            ImmutableSet.of(
                segment1.getId(),
                segment4.getId()
            )
        )
    );

    final DateTime lastUpdatedTime1 = DateTimes.nowUtc();
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(segment1.getId().toString(), lastUpdatedTime1);
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(segment4.getId().toString(), lastUpdatedTime1);

    Assert.assertEquals(
        2,
        getSegmentsMetadataManager().markSegmentsAsUnused(
            ImmutableSet.of(
                segment2.getId(),
                segment3.getId()
            )
        )
    );

    final DateTime lastUpdatedTime2 = DateTimes.nowUtc();
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(segment2.getId().toString(), lastUpdatedTime2);
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(segment3.getId().toString(), lastUpdatedTime2);

    final List<Interval> segmentIntervals = segments.stream()
                                                    .map(DataSegment::getInterval)
                                                    .collect(Collectors.toList());

    final Interval umbrellaInterval = JodaUtils.umbrellaInterval(segmentIntervals);

    final KillUnusedSegmentsTask task1 = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(umbrellaInterval)
        .batchSize(1)
        .limit(10)
        .maxUsedStatusLastUpdatedTime(lastUpdatedTime1)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task1).get().getStatusCode());

    final List<DataSegment> observedUnusedSegments1 =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
            DATA_SOURCE,
            umbrellaInterval,
            null,
            null
        );

    Assert.assertEquals(ImmutableList.of(segment2, segment3), observedUnusedSegments1);
    Assert.assertEquals(
        new KillTaskReport.Stats(2, 3, 0),
        getReportedStats()
    );

    final KillUnusedSegmentsTask task2 = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(umbrellaInterval)
        .batchSize(1)
        .limit(10)
        .maxUsedStatusLastUpdatedTime(lastUpdatedTime2)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task2).get().getStatusCode());

    final List<DataSegment> observedUnusedSegments2 =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
            DATA_SOURCE,
            umbrellaInterval,
            null,
            null
        );

    Assert.assertEquals(ImmutableList.of(), observedUnusedSegments2);
    Assert.assertEquals(
        new KillTaskReport.Stats(2, 3, 0),
        getReportedStats()
    );
  }

  @Test
  public void testKillMultipleUnusedSegmentsWithVersionAndDifferentLastUpdatedTime() throws Exception
  {
    final DateTime version = DateTimes.nowUtc();
    final DataSegment segment1 = newSegment(Intervals.of("2019-01-01/2019-02-01"), version.toString());
    final DataSegment segment2 = newSegment(Intervals.of("2019-02-01/2019-03-01"), version.toString());
    final DataSegment segment3 = newSegment(Intervals.of("2019-03-01/2019-04-01"), version.toString());
    final DataSegment segment4 = newSegment(Intervals.of("2019-04-01/2019-05-01"), version.minusHours(2).toString());
    final DataSegment segment5 = newSegment(Intervals.of("2019-04-01/2019-05-01"), version.minusHours(3).toString());

    final Set<DataSegment> segments = ImmutableSet.of(segment1, segment2, segment3, segment4, segment5);
    Assert.assertEquals(segments, getMetadataStorageCoordinator().commitSegments(segments));

    Assert.assertEquals(
        3,
        getSegmentsMetadataManager().markSegmentsAsUnused(
            ImmutableSet.of(segment1.getId(), segment2.getId(), segment4.getId())
        )
    );

    final DateTime lastUpdatedTime1 = DateTimes.nowUtc();
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(segment1.getId().toString(), lastUpdatedTime1);
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(segment2.getId().toString(), lastUpdatedTime1);
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(segment4.getId().toString(), lastUpdatedTime1);

    Assert.assertEquals(
        2,
        getSegmentsMetadataManager().markSegmentsAsUnused(
            ImmutableSet.of(segment3.getId(), segment5.getId())
        )
    );

    final DateTime lastUpdatedTime2 = DateTimes.nowUtc();
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(segment4.getId().toString(), lastUpdatedTime2);

    final List<Interval> segmentIntervals = segments.stream()
                                                    .map(DataSegment::getInterval)
                                                    .collect(Collectors.toList());

    final Interval umbrellaInterval = JodaUtils.umbrellaInterval(segmentIntervals);

    final KillUnusedSegmentsTask task1 = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(umbrellaInterval)
        .versions(ImmutableList.of(version.toString()))
        .batchSize(1)
        .limit(10)
        .maxUsedStatusLastUpdatedTime(lastUpdatedTime1)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task1).get().getStatusCode());
    Assert.assertEquals(
        new KillTaskReport.Stats(2, 3, 0),
        getReportedStats()
    );

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
            DATA_SOURCE,
            umbrellaInterval,
            null,
            null
        );

    Assert.assertEquals(ImmutableSet.of(segment3, segment4, segment5), new HashSet<>(observedUnusedSegments));

    final KillUnusedSegmentsTask task2 = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(umbrellaInterval)
        .versions(ImmutableList.of(version.toString()))
        .batchSize(1)
        .limit(10)
        .maxUsedStatusLastUpdatedTime(lastUpdatedTime2)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task2).get().getStatusCode());
    Assert.assertEquals(
        new KillTaskReport.Stats(1, 2, 0),
        getReportedStats()
    );

    final List<DataSegment> observedUnusedSegments2 =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
            DATA_SOURCE,
            umbrellaInterval,
            null,
            null
        );

    Assert.assertEquals(ImmutableSet.of(segment4, segment5), new HashSet<>(observedUnusedSegments2));
  }

  @Test
  public void testKillBatchSizeThree() throws Exception
  {
    final Set<DataSegment> segments = ImmutableSet.of(segment1, segment2, segment3, segment4);
    final Set<DataSegment> announced = getMetadataStorageCoordinator().commitSegments(segments);
    Assert.assertEquals(segments, announced);

    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018-01-01/2020-01-01"))
        .markAsUnused(true)
        .batchSize(3)
        .build();

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());

    final List<DataSegment> observedUnusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(
          DATA_SOURCE,
          Intervals.of("2019/2020"),
          null,
          null
        );

    Assert.assertEquals(Collections.emptyList(), observedUnusedSegments);
    Assert.assertEquals(
        new KillTaskReport.Stats(4, 3, 4),
        getReportedStats()
    );
  }

  @Test
  public void testComputeNextBatchSizeDefault()
  {
    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018-01-01/2020-01-01"))
        .build();
    Assert.assertEquals(100, task.computeNextBatchSize(50));
  }

  @Test
  public void testComputeNextBatchSizeWithBatchSizeLargerThanLimit()
  {
    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018-01-01/2020-01-01"))
        .batchSize(10)
        .limit(5)
        .build();
    Assert.assertEquals(5, task.computeNextBatchSize(0));
  }

  @Test
  public void testComputeNextBatchSizeWithBatchSizeSmallerThanLimit()
  {
    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018-01-01/2020-01-01"))
        .batchSize(5)
        .limit(10)
        .build();
    Assert.assertEquals(5, task.computeNextBatchSize(0));
  }

  @Test
  public void testComputeNextBatchSizeWithRemainingLessThanLimit()
  {
    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018-01-01/2020-01-01"))
        .batchSize(5)
        .limit(10)
        .build();
    Assert.assertEquals(3, task.computeNextBatchSize(7));
  }

  @Test
  public void testGetNumTotalBatchesDefault()
  {
    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018-01-01/2020-01-01"))
        .build();
    Assert.assertNull(task.getNumTotalBatches());
  }

  @Test
  public void testGetNumTotalBatchesWithBatchSizeLargerThanLimit()
  {
    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018-01-01/2020-01-01"))
        .batchSize(10)
        .limit(5)
        .build();
    Assert.assertEquals(1, (int) task.getNumTotalBatches());
  }

  @Test
  public void testInvalidLimit()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new KillUnusedSegmentsTaskBuilder()
                .dataSource(DATA_SOURCE)
                .interval(Intervals.of("2018-01-01/2020-01-01"))
                .limit(0)
                .build()
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "limit[0] must be a positive integer."
        )
    );
  }

  @Test
  public void testInvalidBatchSize()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new KillUnusedSegmentsTaskBuilder()
                .dataSource(DATA_SOURCE)
                .interval(Intervals.of("2018-01-01/2020-01-01"))
                .batchSize(0)
                .build()
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "batchSize[0] must be a positive integer."
        )
    );
  }

  @Test
  public void testInvalidLimitWithMarkAsUnused()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new KillUnusedSegmentsTaskBuilder()
                .dataSource(DATA_SOURCE)
                .interval(Intervals.of("2018-01-01/2020-01-01"))
                .markAsUnused(true)
                .batchSize(10)
                .limit(10)
                .build()
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "limit[10] cannot be provided when markAsUnused is enabled."
        )
    );
  }

  @Test
  public void testInvalidVersionsWithMarkAsUnused()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new KillUnusedSegmentsTaskBuilder()
                .dataSource(DATA_SOURCE)
                .interval(Intervals.of("2018-01-01/2020-01-01"))
                .markAsUnused(true)
                .versions(ImmutableList.of("foo"))
                .build()
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "versions[[foo]] cannot be provided when markAsUnused is enabled."
        )
    );
  }

  @Test
  public void testGetNumTotalBatchesWithBatchSizeSmallerThanLimit()
  {
    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTaskBuilder()
        .dataSource(DATA_SOURCE)
        .interval(Intervals.of("2018-01-01/2020-01-01"))
        .versions(ImmutableList.of("foo"))
        .batchSize(5)
        .limit(10)
        .build();
    Assert.assertEquals(2, (int) task.getNumTotalBatches());
  }

  @Test
  public void testKillTaskReportSerde() throws Exception
  {
    final String taskId = "test_serde_task";

    final KillTaskReport.Stats stats = new KillTaskReport.Stats(1, 2, 3);
    KillTaskReport report = new KillTaskReport(taskId, stats);

    String json = getObjectMapper().writeValueAsString(report);
    TaskReport deserializedReport = getObjectMapper().readValue(json, TaskReport.class);
    Assert.assertTrue(deserializedReport instanceof KillTaskReport);

    KillTaskReport deserializedKillReport = (KillTaskReport) deserializedReport;
    Assert.assertEquals(KillTaskReport.REPORT_KEY, deserializedKillReport.getReportKey());
    Assert.assertEquals(taskId, deserializedKillReport.getTaskId());
    Assert.assertEquals(stats, deserializedKillReport.getPayload());
  }

  private static class KillUnusedSegmentsTaskBuilder
  {
    private String id;
    private String dataSource;
    private Interval interval;
    private List<String> versions;
    private Map<String, Object> context;
    private Boolean markAsUnused;
    private Integer batchSize;
    private Integer limit;
    private DateTime maxUsedStatusLastUpdatedTime;

    public KillUnusedSegmentsTaskBuilder id(String id)
    {
      this.id = id;
      return this;
    }

    public KillUnusedSegmentsTaskBuilder dataSource(String dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public KillUnusedSegmentsTaskBuilder interval(Interval interval)
    {
      this.interval = interval;
      return this;
    }

    public KillUnusedSegmentsTaskBuilder versions(List<String> versions)
    {
      this.versions = versions;
      return this;
    }

    public KillUnusedSegmentsTaskBuilder context(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public KillUnusedSegmentsTaskBuilder markAsUnused(Boolean markAsUnused)
    {
      this.markAsUnused = markAsUnused;
      return this;
    }

    public KillUnusedSegmentsTaskBuilder batchSize(Integer batchSize)
    {
      this.batchSize = batchSize;
      return this;
    }

    public KillUnusedSegmentsTaskBuilder limit(Integer limit)
    {
      this.limit = limit;
      return this;
    }

    public KillUnusedSegmentsTaskBuilder maxUsedStatusLastUpdatedTime(DateTime maxUsedStatusLastUpdatedTime)
    {
      this.maxUsedStatusLastUpdatedTime = maxUsedStatusLastUpdatedTime;
      return this;
    }

    public KillUnusedSegmentsTask build()
    {
      return new KillUnusedSegmentsTask(
          id,
          dataSource,
          interval,
          versions,
          context,
          markAsUnused,
          batchSize,
          limit,
          maxUsedStatusLastUpdatedTime
      );
    }
  }

  private KillTaskReport.Stats getReportedStats()
  {
    try {
      Object payload = getObjectMapper().readValue(
          taskRunner.getTaskReportsFile(),
          new TypeReference<Map<String, TaskReport>>()
          {
          }
      ).get(KillTaskReport.REPORT_KEY).getPayload();
      return getObjectMapper().convertValue(payload, KillTaskReport.Stats.class);
    }
    catch (Exception e) {
      throw new ISE(e, "Error while reading task report");
    }
  }

  private static DataSegment newSegment(Interval interval, String version)
  {
    return new DataSegment(
        DATA_SOURCE,
        interval,
        version,
        null,
        null,
        null,
        null,
        9,
        10L
    );
  }

  private static DataSegment newSegment(Interval interval, String version, Map<String, Object> loadSpec)
  {
    return new DataSegment(
        DATA_SOURCE,
        interval,
        version,
        loadSpec,
        null,
        null,
        null,
        9,
        10L
    );
  }
}
