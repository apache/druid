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

package org.apache.druid.indexing.common.task.batch.parallel;

import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;

public class TombstoneHelperTest
{

  private final TaskActionClient taskActionClient = Mockito.mock(TaskActionClient.class);

  @Test
  public void noTombstonesWhenNoDataInInputIntervalAndNoExistingSegments() throws Exception
  {
    Interval interval = Intervals.of("2020-04-01/2020-04-04");
    GranularitySpec granularitySpec = new UniformGranularitySpec(Granularities.DAY, null, false,
                                                                 Collections.singletonList(interval)
    );
    DataSchema dataSchema =
        new DataSchema("test", null, null, null, granularitySpec, null);
    // no segments will be pushed when all rows are thrown away, assume that:
    List<DataSegment> pushedSegments = Collections.emptyList();

    // Assume no used segments :
    Mockito.when(taskActionClient.submit(any(TaskAction.class))).thenReturn(Collections.emptyList());

    TombstoneHelper tombstoneHelper = new TombstoneHelper(
        pushedSegments,
        dataSchema,
        taskActionClient
    );
    List<Interval> tombstoneIntervals = tombstoneHelper.computeTombstoneIntervals();
    Assert.assertTrue(tombstoneIntervals.isEmpty());

    Map<Interval, SegmentIdWithShardSpec> intervalToLockVersion = Collections.emptyMap();
    Set<DataSegment> tombstones = tombstoneHelper.computeTombstones(intervalToLockVersion);

    Assert.assertEquals(0, tombstones.size());

  }

  @Test
  public void tombstonesCreatedWhenNoDataInInputIntervalAndExistingSegments() throws Exception
  {
    Interval interval = Intervals.of("2020-04-01/2020-04-04");
    GranularitySpec granularitySpec = new UniformGranularitySpec(Granularities.DAY, null, false,
                                                                 Collections.singletonList(interval)
    );
    DataSchema dataSchema =
        new DataSchema("test", null, null, null, granularitySpec, null);
    // no segments will be pushed when all rows are thrown away, assume that:
    List<DataSegment> pushedSegments = Collections.emptyList();

    // Assume used segments exist:
    DataSegment existingUsedSegment =
        DataSegment.builder()
                   .dataSource("test")
                   .interval(interval)
                   .version("oldVersion")
                   .size(100)
                   .build();
    Assert.assertFalse(existingUsedSegment.isTombstone());
    Mockito.when(taskActionClient.submit(any(TaskAction.class)))
           .thenReturn(Collections.singletonList(existingUsedSegment));
    TombstoneHelper tombstoneHelper = new TombstoneHelper(
        pushedSegments,
        dataSchema,
        taskActionClient
    );

    List<Interval> tombstoneIntervals = tombstoneHelper.computeTombstoneIntervals();
    Assert.assertEquals(3, tombstoneIntervals.size());
    Map<Interval, SegmentIdWithShardSpec> intervalToVersion = new HashMap<>();
    for (Interval ti : tombstoneIntervals) {
      intervalToVersion.put(
          ti,
          new SegmentIdWithShardSpec("test", ti, "newVersion", new TombstoneShardSpec())
      );
    }
    Set<DataSegment> tombstones = tombstoneHelper.computeTombstones(intervalToVersion);
    Assert.assertEquals(3, tombstones.size());
    tombstones.forEach(ts -> Assert.assertTrue(ts.isTombstone()));
  }

}
