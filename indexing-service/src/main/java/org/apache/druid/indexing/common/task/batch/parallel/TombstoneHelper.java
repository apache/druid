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

import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TombstoneHelper
{

  private final DataSchema dataSchema;
  private final TaskActionClient taskActionClient;
  private final Collection<DataSegment> pushedSegments;

  public TombstoneHelper(
      Collection<DataSegment> pushedSegments,
      DataSchema dataSchema,
      TaskActionClient taskActionClient
  )
  {

    Preconditions.checkNotNull(pushedSegments, "pushedSegments");
    Preconditions.checkNotNull(dataSchema, "dataSchema");
    Preconditions.checkNotNull(taskActionClient, "taskActionClient");

    this.dataSchema = dataSchema;
    this.taskActionClient = taskActionClient;
    this.pushedSegments = pushedSegments;
  }


  private List<Interval> getCondensedPushedSegmentsIntervals()
  {
    List<Interval> pushedSegmentsIntervals = new ArrayList<>();
    for (DataSegment pushedSegment : pushedSegments) {
      pushedSegmentsIntervals.add(pushedSegment.getInterval());
    }
    return JodaUtils.condenseIntervals(pushedSegmentsIntervals);
  }

  public Set<DataSegment> computeTombstones(Map<Interval, SegmentIdWithShardSpec> tombstoneIntervalsAndVersions)
  {
    Set<DataSegment> retVal = new HashSet<>();
    String dataSource = dataSchema.getDataSource();
    for (Map.Entry<Interval, SegmentIdWithShardSpec> tombstoneIntervalAndVersion : tombstoneIntervalsAndVersions.entrySet()) {
      // now we have all the metadata to create the tombstone:
      DataSegment tombstone =
          createTombstoneForTimeChunkInterval(
              dataSource,
              tombstoneIntervalAndVersion.getValue().getVersion(),
              tombstoneIntervalAndVersion.getValue().getShardSpec(),
              tombstoneIntervalAndVersion.getKey()
          );
      retVal.add(tombstone);
    }
    return retVal;
  }

  public List<Interval> computeTombstoneIntervals() throws IOException
  {
    List<Interval> retVal = new ArrayList<>();
    GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    List<Interval> pushedSegmentsIntervals = getCondensedPushedSegmentsIntervals();
    List<Interval> intervalsForUsedSegments = getCondensedUsedIntervals();
    for (Interval timeChunkInterval : granularitySpec.sortedBucketIntervals()) {
      // is it an empty time chunk?
      boolean isEmpty = true;
      for (Interval pushedSegmentCondensedInterval : pushedSegmentsIntervals) {
        if (timeChunkInterval.overlaps(pushedSegmentCondensedInterval)) {
          isEmpty = false;
          break;
        }
      }
      if (isEmpty) {
        // this timeChunkInterval has no data, it is empty, thus it is a candidate for tombstone
        // now check if it actually might overshadow a used segment
        for (Interval usedSegmentInterval : intervalsForUsedSegments) {
          if (timeChunkInterval.overlaps(usedSegmentInterval)) {
            // yes it does overshadow...
            retVal.add(timeChunkInterval);
            break;
          }

        }
      }
    }
    return retVal;
  }

  private DataSegment createTombstoneForTimeChunkInterval(String dataSource, String version, ShardSpec shardSpec, Interval timeChunkInterval)
  {


    // and the loadSpec is different too:
    Map<String, Object> tombstoneLoadSpec = new HashMap<>();
    // since loadspec comes from prototype it is guaranteed to be non-null

    tombstoneLoadSpec.put("type", DataSegment.TOMBSTONE_LOADSPEC_TYPE);
    tombstoneLoadSpec.put("path", null); // tombstones do not have any backing file

    // Several fields do not apply to tombstones...
    DataSegment.Builder dataSegmentBuilder =
        DataSegment.builder()
                   .dataSource(dataSource)
                   .interval(timeChunkInterval) // interval is different
                   .version(version)
                   .shardSpec(shardSpec)
                   .loadSpec(tombstoneLoadSpec) // load spec is special for tombstone
                   .size(1); // in case coordinator segment balancing chokes with zero size

    return dataSegmentBuilder.build();

  }


  /**
   * Helper method to prune required tombstones. Only tombstones that cover used intervals will be created
   * since those that not cover used intervals will be redundant.
   * @return Intervals corresponding to used segments that overlap with any of the spec's input intervals
   * @throws IOException If used segments cannot be retrieved
   */
  public List<Interval> getCondensedUsedIntervals() throws IOException
  {
    List<Interval> retVal = new ArrayList<>();

    List<Interval> condensedInputIntervals = JodaUtils.condenseIntervals(dataSchema.getGranularitySpec().inputIntervals());
    if (!condensedInputIntervals.isEmpty()) {
      Collection<DataSegment> usedSegmentsInInputInterval =
          taskActionClient.submit(new RetrieveUsedSegmentsAction(
              dataSchema.getDataSource(), null,
              condensedInputIntervals,
              Segments.ONLY_VISIBLE
          ));
      for (DataSegment usedSegment : usedSegmentsInInputInterval) {
        for (Interval condensedInputInterval : condensedInputIntervals) {
          if (condensedInputInterval.overlaps(usedSegment.getInterval())) {
            retVal.add(usedSegment.getInterval());
            break;
          }
        }
      }
    }

    return JodaUtils.condenseIntervals(retVal);
  }

}
