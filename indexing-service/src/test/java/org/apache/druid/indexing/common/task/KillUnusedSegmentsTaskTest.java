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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.assertj.core.api.Assertions;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class KillUnusedSegmentsTaskTest extends IngestionTestBase
{
  private static final String DATA_SOURCE = "dataSource";

  private TaskRunner taskRunner;

  @Before
  public void setup()
  {
    taskRunner = new TestTaskRunner();
  }

  @Test
  public void testKill() throws Exception
  {
    final String version = DateTimes.nowUtc().toString();
    final Set<DataSegment> segments = ImmutableSet.of(
        newSegment(Intervals.of("2019-01-01/2019-02-01"), version),
        newSegment(Intervals.of("2019-02-01/2019-03-01"), version),
        newSegment(Intervals.of("2019-03-01/2019-04-01"), version),
        newSegment(Intervals.of("2019-04-01/2019-05-01"), version)
    );
    final Set<DataSegment> announced = getMetadataStorageCoordinator().announceHistoricalSegments(segments);

    Assert.assertEquals(segments, announced);

    Assert.assertTrue(
        getSegmentsMetadataManager().markSegmentAsUnused(
            newSegment(Intervals.of("2019-02-01/2019-03-01"), version).getId()
        )
    );
    Assert.assertTrue(
        getSegmentsMetadataManager().markSegmentAsUnused(
            newSegment(Intervals.of("2019-03-01/2019-04-01"), version).getId()
        )
    );

    final KillUnusedSegmentsTask task =
        new KillUnusedSegmentsTask(
            null,
            DATA_SOURCE,
            Intervals.of("2019-03-01/2019-04-01"),
            null,
            false,
            null,
            null
        );

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());

    final List<DataSegment> unusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(DATA_SOURCE, Intervals.of("2019/2020"));

    Assert.assertEquals(ImmutableList.of(newSegment(Intervals.of("2019-02-01/2019-03-01"), version)), unusedSegments);
    Assertions.assertThat(
        getMetadataStorageCoordinator()
            .retrieveUsedSegmentsForInterval(DATA_SOURCE, Intervals.of("2019/2020"), Segments.ONLY_VISIBLE)
    ).containsExactlyInAnyOrder(
        newSegment(Intervals.of("2019-01-01/2019-02-01"), version),
        newSegment(Intervals.of("2019-04-01/2019-05-01"), version)
    );

    Assert.assertEquals(2L, task.getNumBatchesProcessed());
    Assert.assertEquals(1, task.getNumSegmentsKilled());
  }


  @Test
  public void testKillWithMarkUnused() throws Exception
  {
    final String version = DateTimes.nowUtc().toString();
    final Set<DataSegment> segments = ImmutableSet.of(
        newSegment(Intervals.of("2019-01-01/2019-02-01"), version),
        newSegment(Intervals.of("2019-02-01/2019-03-01"), version),
        newSegment(Intervals.of("2019-03-01/2019-04-01"), version),
        newSegment(Intervals.of("2019-04-01/2019-05-01"), version)
    );
    final Set<DataSegment> announced = getMetadataStorageCoordinator().announceHistoricalSegments(segments);

    Assert.assertEquals(segments, announced);

    Assert.assertTrue(
        getSegmentsMetadataManager().markSegmentAsUnused(
            newSegment(Intervals.of("2019-02-01/2019-03-01"), version).getId()
        )
    );

    final KillUnusedSegmentsTask task =
        new KillUnusedSegmentsTask(
            null,
            DATA_SOURCE,
            Intervals.of("2019-03-01/2019-04-01"),
            null,
            true,
            null,
            null
        );

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());

    final List<DataSegment> unusedSegments =
        getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(DATA_SOURCE, Intervals.of("2019/2020"));

    Assert.assertEquals(ImmutableList.of(newSegment(Intervals.of("2019-02-01/2019-03-01"), version)), unusedSegments);
    Assertions.assertThat(
        getMetadataStorageCoordinator()
            .retrieveUsedSegmentsForInterval(DATA_SOURCE, Intervals.of("2019/2020"), Segments.ONLY_VISIBLE)
    ).containsExactlyInAnyOrder(
        newSegment(Intervals.of("2019-01-01/2019-02-01"), version),
        newSegment(Intervals.of("2019-04-01/2019-05-01"), version)
    );
    Assert.assertEquals(2L, task.getNumBatchesProcessed());
    Assert.assertEquals(1, task.getNumSegmentsKilled());
  }

  @Test
  public void testGetInputSourceResources()
  {
    final KillUnusedSegmentsTask task =
        new KillUnusedSegmentsTask(
            null,
            DATA_SOURCE,
            Intervals.of("2019-03-01/2019-04-01"),
            null,
            true,
            null,
            null
        );
    Assert.assertTrue(task.getInputSourceResources().isEmpty());
  }

  @Test
  public void testKillBatchSizeOneAndLimit4() throws Exception
  {
    final String version = DateTimes.nowUtc().toString();
    final Set<DataSegment> segments = ImmutableSet.of(
            newSegment(Intervals.of("2019-01-01/2019-02-01"), version),
            newSegment(Intervals.of("2019-02-01/2019-03-01"), version),
            newSegment(Intervals.of("2019-03-01/2019-04-01"), version),
            newSegment(Intervals.of("2019-04-01/2019-05-01"), version)
    );
    final Set<DataSegment> announced = getMetadataStorageCoordinator().announceHistoricalSegments(segments);

    Assert.assertEquals(segments, announced);

    Assert.assertEquals(
        segments.size(),
        getSegmentsMetadataManager().markAsUnusedSegmentsInInterval(
            DATA_SOURCE,
            Intervals.of("2018-01-01/2020-01-01")
        )
    );

    final KillUnusedSegmentsTask task =
            new KillUnusedSegmentsTask(
                    null,
                    DATA_SOURCE,
                    Intervals.of("2018-01-01/2020-01-01"),
                    null,
                    false,
                    1,
                4
            );

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());

    // we expect ALL tasks to be deleted

    final List<DataSegment> unusedSegments =
            getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(DATA_SOURCE, Intervals.of("2019/2020"));

    Assert.assertEquals(Collections.emptyList(), unusedSegments);
    Assert.assertEquals(4L, task.getNumBatchesProcessed());
    Assert.assertEquals(4, task.getNumSegmentsKilled());
  }

  @Test
  public void testKillBatchSizeThree() throws Exception
  {
    final String version = DateTimes.nowUtc().toString();
    final Set<DataSegment> segments = ImmutableSet.of(
            newSegment(Intervals.of("2019-01-01/2019-02-01"), version),
            newSegment(Intervals.of("2019-02-01/2019-03-01"), version),
            newSegment(Intervals.of("2019-03-01/2019-04-01"), version),
            newSegment(Intervals.of("2019-04-01/2019-05-01"), version)
    );
    final Set<DataSegment> announced = getMetadataStorageCoordinator().announceHistoricalSegments(segments);

    Assert.assertEquals(segments, announced);

    final KillUnusedSegmentsTask task =
            new KillUnusedSegmentsTask(
                    null,
                    DATA_SOURCE,
                    Intervals.of("2018-01-01/2020-01-01"),
                    null,
                    true,
                    3,
                null
            );

    Assert.assertEquals(TaskState.SUCCESS, taskRunner.run(task).get().getStatusCode());

    // we expect ALL tasks to be deleted

    final List<DataSegment> unusedSegments =
            getMetadataStorageCoordinator().retrieveUnusedSegmentsForInterval(DATA_SOURCE, Intervals.of("2019/2020"));

    Assert.assertEquals(Collections.emptyList(), unusedSegments);
    Assert.assertEquals(3L, task.getNumBatchesProcessed());
    Assert.assertEquals(4, task.getNumSegmentsKilled());
  }

  @Test
  public void testComputeNextBatchSizeDefault()
  {
    final KillUnusedSegmentsTask task =
        new KillUnusedSegmentsTask(
            null,
            DATA_SOURCE,
            Intervals.of("2018-01-01/2020-01-01"),
            null,
            false,
            null,
            null
        );
    Assert.assertEquals(100, task.computeNextBatchSize(50));
  }

  @Test
  public void testComputeNextBatchSizeWithBatchSizeLargerThanLimit()
  {
    final KillUnusedSegmentsTask task =
        new KillUnusedSegmentsTask(
            null,
            DATA_SOURCE,
            Intervals.of("2018-01-01/2020-01-01"),
            null,
            false,
            10,
            5
        );
    Assert.assertEquals(5, task.computeNextBatchSize(0));
  }

  @Test
  public void testComputeNextBatchSizeWithBatchSizeSmallerThanLimit()
  {
    final KillUnusedSegmentsTask task =
        new KillUnusedSegmentsTask(
            null,
            DATA_SOURCE,
            Intervals.of("2018-01-01/2020-01-01"),
            null,
            false,
            5,
            10
        );
    Assert.assertEquals(5, task.computeNextBatchSize(0));
  }

  @Test
  public void testComputeNextBatchSizeWithRemainingLessThanLimit()
  {
    final KillUnusedSegmentsTask task =
        new KillUnusedSegmentsTask(
            null,
            DATA_SOURCE,
            Intervals.of("2018-01-01/2020-01-01"),
            null,
            false,
            5,
            10
        );
    Assert.assertEquals(3, task.computeNextBatchSize(7));
  }

  @Test
  public void testGetNumTotalBatchesDefault()
  {
    final KillUnusedSegmentsTask task =
        new KillUnusedSegmentsTask(
            null,
            DATA_SOURCE,
            Intervals.of("2018-01-01/2020-01-01"),
            null,
            false,
            null,
            null
        );
    Assert.assertNull(task.getNumTotalBatches());
  }

  @Test
  public void testGetNumTotalBatchesWithBatchSizeLargerThanLimit()
  {
    final KillUnusedSegmentsTask task =
        new KillUnusedSegmentsTask(
            null,
            DATA_SOURCE,
            Intervals.of("2018-01-01/2020-01-01"),
            null,
            false,
            10,
            5
        );
    Assert.assertEquals(1, (int) task.getNumTotalBatches());
  }

  @Test
  public void testGetNumTotalBatchesWithBatchSizeSmallerThanLimit()
  {
    final KillUnusedSegmentsTask task =
        new KillUnusedSegmentsTask(
            null,
            DATA_SOURCE,
            Intervals.of("2018-01-01/2020-01-01"),
            null,
            false,
            5,
            10
        );
    Assert.assertEquals(2, (int) task.getNumTotalBatches());
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
}
