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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-XX:+UseG1GC"})
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode({Mode.Throughput})
public class VersionedIntervalTimelineBenchmark
{
  private static final String DATA_SOURCE = "dataSource";
  private static final Interval TOTAL_INTERVAL = Intervals.of("2018/2019");
  private static final double NEW_ROOT_GEN_SEGMENTS_RATIO_AFTER_COMPACTION = 0.1;
  private static final double COMPACTED_SEGMENTS_RATIO_TO_INITIAL_SEGMENTS = 0.5;

  @Param({"10", "100", "1000"})
  private int numInitialRootGenSegmentsPerInterval;

  @Param({"1", "2"})
  private int numNonRootGenerations;

  @Param({"false", "true"})
  private boolean useSegmentLock;

  @Param({"MONTH", "DAY"})
  private GranularityType segmentGranularity;

  private List<Interval> intervals;
  private List<DataSegment> segments;
  private VersionedIntervalTimeline<String, DataSegment> timeline;
  private List<DataSegment> newSegments;

  @Setup
  public void setup()
  {
    final int numNewRootGenSegmentsAfterCompaction =
        (int) (numInitialRootGenSegmentsPerInterval * NEW_ROOT_GEN_SEGMENTS_RATIO_AFTER_COMPACTION);
    final int numCompactedSegments =
        (int) (numInitialRootGenSegmentsPerInterval * COMPACTED_SEGMENTS_RATIO_TO_INITIAL_SEGMENTS);

    intervals = Lists.newArrayList(segmentGranularity.getDefaultGranularity().getIterable(TOTAL_INTERVAL));
    segments = new ArrayList<>(intervals.size() * numInitialRootGenSegmentsPerInterval);
    Map<Interval, Integer> nextRootGenPartitionIds = Maps.newHashMapWithExpectedSize(intervals.size());
    Map<Interval, Integer> nextNonRootGenPartitionIds = Maps.newHashMapWithExpectedSize(intervals.size());
    Map<Interval, Short> nextMinorVersions = Maps.newHashMapWithExpectedSize(intervals.size());

    DateTime majorVersion = DateTimes.nowUtc();

    for (Interval interval : intervals) {
      majorVersion = majorVersion.plus(1);
      int nextRootGenPartitionId = 0;
      int nextNonRootGenPartitionId = PartitionIds.NON_ROOT_GEN_START_PARTITION_ID;

      // Generate root generation segments
      for (int i = 0; i < numInitialRootGenSegmentsPerInterval; i++) {
        segments.add(newSegment(interval, majorVersion.toString(), new NumberedShardSpec(nextRootGenPartitionId++, 0)));
      }

      for (int i = 0; i < numNonRootGenerations; i++) {
        if (!useSegmentLock) {
          majorVersion = majorVersion.plus(1);
          nextRootGenPartitionId = 0;
        }
        // Compacted segments
        for (int j = 0; j < numCompactedSegments; j++) {
          if (useSegmentLock) {
            segments.add(
                newSegment(
                    interval,
                    majorVersion.toString(),
                    new NumberedOverwriteShardSpec(
                        nextNonRootGenPartitionId++,
                        0,
                        nextRootGenPartitionId,
                        (short) (i + 1),
                        (short) numCompactedSegments
                    )
                )
            );
          } else {
            segments.add(newSegment(interval, majorVersion.toString(), new NumberedShardSpec(nextRootGenPartitionId++, 0)));
          }
        }

        // New segments
        for (int j = 0; j < numNewRootGenSegmentsAfterCompaction; j++) {
          segments.add(newSegment(interval, majorVersion.toString(), new NumberedShardSpec(nextRootGenPartitionId++, 0)));
        }
      }
      nextRootGenPartitionIds.put(interval, nextRootGenPartitionId);
      nextNonRootGenPartitionIds.put(interval, nextNonRootGenPartitionId);
      nextMinorVersions.put(interval, (short) (numNonRootGenerations + 1));
    }

    timeline = VersionedIntervalTimeline.forSegments(segments);

    newSegments = new ArrayList<>(200);

    // Generate new appending segments
    for (int i = 0; i < 100; i++) {
      final Interval interval = intervals.get(ThreadLocalRandom.current().nextInt(intervals.size()));
      final int rootPartitionId = nextRootGenPartitionIds.get(interval);

      newSegments.add(
          newSegment(
              interval,
              majorVersion.toString(),
              new NumberedShardSpec(rootPartitionId, 0)
          )
      );
      nextRootGenPartitionIds.put(interval, rootPartitionId + 1);
    }

    // Generate overwriting segments
    if (!useSegmentLock) {
      majorVersion = majorVersion.plus(1);
      nextRootGenPartitionIds.keySet().forEach(interval -> nextRootGenPartitionIds.put(interval, 0));
    }

    final List<Interval> intervalCopies = new ArrayList<>(intervals);
    for (int i = 0; i < 100 && !intervalCopies.isEmpty(); i++) {
      final Interval interval = intervalCopies.remove(ThreadLocalRandom.current().nextInt(intervalCopies.size()));
      int rootPartitionId = nextRootGenPartitionIds.remove(interval);
      int nonRootPartitionId = nextNonRootGenPartitionIds.remove(interval);
      final short minorVersion = nextMinorVersions.remove(interval);

      for (int j = 0; j < numCompactedSegments; j++) {
        if (useSegmentLock) {
          newSegments.add(
              newSegment(
                  interval,
                  majorVersion.toString(),
                  new NumberedOverwriteShardSpec(
                      nonRootPartitionId++,
                      0,
                      rootPartitionId,
                      minorVersion,
                      (short) numCompactedSegments
                  )
              )
          );
        } else {
          newSegments.add(
              newSegment(
                  interval,
                  majorVersion.toString(),
                  new NumberedShardSpec(rootPartitionId++, 0)
              )
          );
        }
      }
    }
  }

  @Benchmark
  public void benchAdd(Blackhole blackhole)
  {
    final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(segments);
    for (DataSegment newSegment : newSegments) {
      timeline.add(
          newSegment.getInterval(),
          newSegment.getVersion(),
          newSegment.getShardSpec().createChunk(newSegment)
      );
    }
  }

  @Benchmark
  public void benchRemove(Blackhole blackhole)
  {
    final List<DataSegment> segmentsCopy = new ArrayList<>(segments);
    final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(segmentsCopy);
    final int numTests = (int) (segmentsCopy.size() * 0.1);
    for (int i = 0; i < numTests; i++) {
      final DataSegment segment = segmentsCopy.remove(ThreadLocalRandom.current().nextInt(segmentsCopy.size()));
      blackhole.consume(
          timeline.remove(
              segment.getInterval(),
              segment.getVersion(),
              segment.getShardSpec().createChunk(segment)
          )
      );
    }
  }

  @Benchmark
  public void benchLookup(Blackhole blackhole)
  {
    final int intervalIndex = ThreadLocalRandom.current().nextInt(intervals.size() - 2);
    final Interval queryInterval = new Interval(
        intervals.get(intervalIndex).getStart(),
        intervals.get(intervalIndex + 2).getEnd()
    );
    blackhole.consume(timeline.lookup(queryInterval));
  }

  @Benchmark
  public void benchIsOvershadowed(Blackhole blackhole)
  {
    final DataSegment segment = segments.get(ThreadLocalRandom.current().nextInt(segments.size()));
    blackhole.consume(timeline.isOvershadowed(segment.getInterval(), segment.getVersion(), segment));
  }

  @Benchmark
  public void benchFindFullyOvershadowed(Blackhole blackhole)
  {
    blackhole.consume(timeline.findFullyOvershadowed());
  }

  private static DataSegment newSegment(Interval interval, String version, ShardSpec shardSpec)
  {
    return new DataSegment(
        DATA_SOURCE,
        interval,
        version,
        null,
        null,
        null,
        shardSpec,
        9,
        10
    );
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(VersionedIntervalTimelineBenchmark.class.getSimpleName())
        .forks(1)
        .syncIterations(true)
        .resultFormat(ResultFormatType.CSV)
        .result("versioned-interval-timeline.csv")
        .build();

    new Runner(opt).run();
  }
}
