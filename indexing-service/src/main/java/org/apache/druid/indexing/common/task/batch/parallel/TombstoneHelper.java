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
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.batch.TooManyBucketsException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.IntervalsByGranularity;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

  /**
   * This method is used by the native batch engine and should be reconciled with
   * {@link TombstoneHelper#computeTombstoneSegmentsForReplace(List, List, String, Granularity, int)} since they are
   * functionally similar, so both the native and MSQ engines use the same logic. It will require some refactoring
   * such that we can pass in meaningful arguments that works with both systems.
   */
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

  /**
   * This method is used by the MSQ engine and should be reconciled with
   * {@link TombstoneHelper#computeTombstones(DataSchema, Map)} since they are functionally similar, so both the native
   * and MSQ engines use the same logic. It will require some refactoring such that we can pass in meaningful arguments
   * that works with both systems.
   */
  public Set<DataSegment> computeTombstoneSegmentsForReplace(
      List<Interval> intervalsToDrop,
      List<Interval> intervalsToReplace,
      String dataSource,
      Granularity replaceGranularity,
      int maxBuckets
  ) throws IOException
  {
    Set<Interval> tombstoneIntervals = computeTombstoneIntervalsForReplace(
        intervalsToDrop,
        intervalsToReplace,
        dataSource,
        replaceGranularity,
        maxBuckets
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
   * @param maxBuckets         Maximum number of partition buckets. If the number of computed tombstone buckets
   *                           exceeds this threshold, the method will throw an error.
   * @return Intervals computed for the tombstones
   * @throws IOException
   */
  public Set<Interval> computeTombstoneIntervalsForReplace(
      List<Interval> intervalsToDrop,
      List<Interval> intervalsToReplace,
      String dataSource,
      Granularity replaceGranularity,
      int maxBuckets
  ) throws IOException
  {
    Set<Interval> retVal = new HashSet<>();
    List<Interval> usedIntervals = getExistingNonEmptyIntervalsOfDatasource(intervalsToReplace, dataSource);
    int buckets = 0;

    for (Interval intervalToDrop : intervalsToDrop) {
      for (Interval usedInterval : usedIntervals) {

        Interval overlap = intervalToDrop.overlap(usedInterval);

        // No overlap of the dropped segment with the used interval due to which we do not need to generate any tombstone
        if (overlap == null) {
          continue;
        }

        if (Intervals.isEternity(overlap)) {
          // Generate a tombstone interval covering eternity.
          buckets = validateAndIncrementBuckets(buckets, maxBuckets);
          retVal.add(overlap);
        } else if (Intervals.ETERNITY.getStart().equals(overlap.getStart())) {
          // Generate a tombstone interval covering the negative eternity interval.
          buckets = validateAndIncrementBuckets(buckets, maxBuckets);
          retVal.add(new Interval(overlap.getStart(), replaceGranularity.bucketStart(overlap.getEnd())));
        } else if ((Intervals.ETERNITY.getEnd()).equals(overlap.getEnd())) {
          // Generate a tombstone interval covering the positive eternity interval.
          buckets = validateAndIncrementBuckets(buckets, maxBuckets);
          retVal.add(new Interval(replaceGranularity.bucketStart(overlap.getStart()), overlap.getEnd()));
        } else {
          // Overlap might not be aligned with the granularity if the used interval is not aligned with the granularity
          // However when fetching from the iterator, the first interval is found using the bucketStart, which
          // ensures that the interval is "rounded down" to align with the granularity.
          // Also, the interval would always be contained inside the "intervalToDrop" because the original REPLACE
          // is aligned by the granularity, and by extension all the elements inside the intervals to drop would
          // also be aligned by the same granularity (since intervalsToDrop = replaceIntervals - publishIntervals, and
          // the right-hand side is always aligned)
          //
          // For example, if replaceGranularity is DAY, intervalsToReplace is [2023-02-20, 2023-02-24) (always
          // aligned with the replaceGranularity), intervalsToDrop is [2023-02-22, 2023-02-24) (they must also be aligned with the replaceGranularity)
          // If the usedIntervals for the datasource is from [2023-02-22T01:00:00Z, 2023-02-23T02:00:00Z), then
          // the overlap would be [2023-02-22T01:00:00Z, 2023-02-23T02:00:00Z). When iterating over the overlap we will get
          // the intervals from [2023-02-22, 2023-02-23) and [2023-02-23, 2023-02-24).
          IntervalsByGranularity intervalsToDropByGranularity = new IntervalsByGranularity(
              ImmutableList.of(overlap),
              replaceGranularity
          );

          Iterator<Interval> intervalIterator = intervalsToDropByGranularity.granularityIntervalsIterator();
          while (intervalIterator.hasNext()) {
            buckets = validateAndIncrementBuckets(buckets, maxBuckets);
            retVal.add(intervalIterator.next());
          }
        }
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
   * since those that do not cover used intervals will be redundant.
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
              condensedInputIntervals
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

  private int validateAndIncrementBuckets(final int buckets, final int maxBuckets)
  {
    if (buckets >= maxBuckets) {
      throw new TooManyBucketsException(maxBuckets);
    }
    return buckets + 1;
  }

}
