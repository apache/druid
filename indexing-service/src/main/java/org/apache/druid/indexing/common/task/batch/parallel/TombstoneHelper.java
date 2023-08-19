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
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.DateTime;
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

  private final TaskActionClient taskActionClient;

  public TombstoneHelper(TaskActionClient taskActionClient)
  {

    this.taskActionClient = Preconditions.checkNotNull(taskActionClient, "taskActionClient");
  }

  private List<Interval> getCondensedPushedSegmentsIntervals(Collection<DataSegment> pushedSegments)
  {
    List<Interval> pushedSegmentsIntervals = new ArrayList<>();
    for (DataSegment pushedSegment : pushedSegments) {
      pushedSegmentsIntervals.add(pushedSegment.getInterval());
    }
    return JodaUtils.condenseIntervals(pushedSegmentsIntervals);
  }

  public Set<DataSegment> computeTombstones(
      DataSchema dataSchema,
      Map<Interval, SegmentIdWithShardSpec> tombstoneIntervalsAndVersions
  )
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

  public List<Interval> computeTombstoneIntervals(Collection<DataSegment> pushedSegments, DataSchema dataSchema)
      throws IOException
  {
    List<Interval> retVal = new ArrayList<>();
    GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    List<Interval> pushedSegmentsIntervals = getCondensedPushedSegmentsIntervals(pushedSegments);
    List<Interval> intervalsForUsedSegments = getExistingNonEmptyIntervalsOfDatasource(
        dataSchema.getGranularitySpec().inputIntervals(),
        dataSchema.getDataSource()
    );
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

  public Set<DataSegment> computeTombstoneSegmentsForReplace(
      List<Interval> intervalsToDrop,
      List<Interval> intervalsToReplace,
      String dataSource,
      Granularity replaceGranularity
  ) throws IOException
  {
    Set<Interval> tombstoneIntervals = computeTombstoneIntervalsForReplace(
        intervalsToDrop,
        intervalsToReplace,
        dataSource,
        replaceGranularity
    );

    final List<TaskLock> locks = taskActionClient.submit(new LockListAction());

    Set<DataSegment> tombstones = new HashSet<>();
    for (Interval tombstoneInterval : tombstoneIntervals) {
      String version = null;
      for (final TaskLock lock : locks) {
        if (lock.getInterval().contains(tombstoneInterval)) {
          version = lock.getVersion();
        }
      }

      if (version == null) {
        // Unable to fetch the version number of the segment
        throw new ISE("Unable to fetch the version of the segments in use. The lock for the task might "
                      + "have been revoked");
      }

      DataSegment tombstone = createTombstoneForTimeChunkInterval(
          dataSource,
          version,
          new TombstoneShardSpec(),
          tombstoneInterval
      );
      tombstones.add(tombstone);
    }
    return tombstones;
  }

  /**
   * See the method body for an example and an indepth explanation as to how the replace interval is created
   * @param intervalsToDrop    Empty intervals in the query that need to be dropped. They should be aligned with the
   *                           replaceGranularity
   * @param intervalsToReplace Intervals in the query which are eligible for replacement with new data.
   *                           They should be aligned with the replaceGranularity
   * @param dataSource         Datasource on which the replace is to be performed
   * @param replaceGranularity Granularity of the replace query
   * @return Intervals computed for the tombstones
   * @throws IOException
   */
  public Set<Interval> computeTombstoneIntervalsForReplace(
      List<Interval> intervalsToDrop,
      List<Interval> intervalsToReplace,
      String dataSource,
      Granularity replaceGranularity
  ) throws IOException
  {
    Set<Interval> retVal = new HashSet<>();
    List<Interval> usedIntervals = getExistingNonEmptyIntervalsOfDatasource(intervalsToReplace, dataSource);

    for (Interval intervalToDrop : intervalsToDrop) {
      for (Interval usedInterval : usedIntervals) {

        Interval overlap = intervalToDrop.overlap(usedInterval);

        // No overlap of the dropped segment with the used interval due to which we donot need to generate any tombstone
        if (overlap == null) {
          continue;
        }

        // "overlap" might not be aligned with the if the used interval is not aligned with the granularity of
        // the REPLACE i.e. datasource's original granularity and replace's granularity are different

        // However, we align the boundaries of the overlap with the replaceGranularity manually, in the following code.

        DateTime alignedIntervalStart = replaceGranularity.bucketStart(overlap.getStart());
        long alignedIntervalStartMillis = Math.max(alignedIntervalStart.getMillis(), JodaUtils.MIN_INSTANT);
        // If the start is aligned, then 'bucketStart()' is unchanged.
        // Else 'bucketStart()' will return the latest timestamp less than overlap.getStart() which aligns with the REPLACE granularity.

        // That extra interval that we are adding before the overlap should be contained in 'intervalToDrop' because
        // intervalToDrop is aligned by the replaceGranularity.
        // If the drop's interval is n, then the extra interval would start from n + 1 (where 1 denotes the replaceGranularity)
        // The overlap's beginning would always be later than intervalToDrop (trivially,
        // because it is the overlap) and if bucketStart floors the overlap beginning, it cannot floor it before
        // the intervalToDrop's start

        // For example, if the replace granularity is DAY, intervalsToReplace are 20/02/2023 - 24/02/2023 (always
        // aligned with the replaceGranularity), intervalsToDrop are 22/02/2023 - 24/02/2023 (they must also be aligned with the replaceGranularity)
        // If the relevant usedIntervals for the datasource are from 22/02/2023 01:00:00 - 23/02/2023 02:00:00, then
        // the overlap would be 22/02/2023 01:00:00 - 23/02/2023 02:00:00. When iterating over the overlap we will get
        // the intervals from 22/02/2023 01:00:00 - 23/02/2023 02:00:00. After aligning it would become
        // 22/02/2023T00:00:00Z - 23/02/2023T23:59:59Z

        // If the end is aligned, then we do not alter it, else we align the end by geting the earliest time later
        // than the overlap's end which aligns with the replace granularity. Using the above-mentioned logic for the
        // start time, we can also argue that the rounded up end would be contained in the intervalToDrop
        DateTime alignedIntervalEnd;
        if (replaceGranularity.bucketStart(overlap.getEnd()).equals(overlap.getEnd())) { // Check if the end is aligned
          alignedIntervalEnd = overlap.getEnd();
        } else {
          alignedIntervalEnd = replaceGranularity.bucketEnd(overlap.getEnd());
        }
        long alignedIntervalEndMillis = Math.min(alignedIntervalEnd.getMillis(), JodaUtils.MAX_INSTANT);
        Interval alignedTombstoneInterval = Intervals.utc(alignedIntervalStartMillis, alignedIntervalEndMillis);

        retVal.add(alignedTombstoneInterval);
      }
    }
    return retVal;
  }

  public DataSegment createTombstoneForTimeChunkInterval(
      String dataSource,
      String version,
      ShardSpec shardSpec,
      Interval timeChunkInterval
  )
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
   * Example:
   * For a datasource having segments for 2020-01-01/2020-12-31 and 2022-01-01/2022-12-31, this method would return
   * the segment 2020-01-01/2020-12-31 if the input intervals asked for the segment between 2019 and 2021.
   *
   * @param inputIntervals   Intervals corresponding to the task
   * @param dataSource       Datasource corresponding to the task
   * @return Intervals corresponding to used segments that overlap with any of the spec's input intervals
   * @throws IOException If used segments cannot be retrieved
   */
  private List<Interval> getExistingNonEmptyIntervalsOfDatasource(
      List<Interval> inputIntervals,
      String dataSource
  ) throws IOException
  {
    List<Interval> retVal = new ArrayList<>();

    List<Interval> condensedInputIntervals = JodaUtils.condenseIntervals(inputIntervals);
    if (!condensedInputIntervals.isEmpty()) {
      Collection<DataSegment> usedSegmentsInInputInterval =
          taskActionClient.submit(new RetrieveUsedSegmentsAction(
              dataSource,
              null,
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
