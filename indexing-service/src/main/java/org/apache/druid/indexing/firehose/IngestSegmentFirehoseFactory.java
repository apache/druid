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

package org.apache.druid.indexing.firehose;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SegmentsSplitHintSpec;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexing.common.RetryPolicy;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.firehose.IngestSegmentFirehose;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class IngestSegmentFirehoseFactory implements FiniteFirehoseFactory<InputRowParser, List<WindowedSegmentId>>
{
  private static final EmittingLogger log = new EmittingLogger(IngestSegmentFirehoseFactory.class);
  private final String dataSource;
  // Exactly one of interval and segmentIds should be non-null. Typically 'interval' is specified directly
  // by the user creating this firehose and 'segmentIds' is used for sub-tasks if it is split for parallel
  // batch ingestion.
  @Nullable
  private final Interval interval;
  @Nullable
  private final List<WindowedSegmentId> segmentIds;
  private final DimFilter dimFilter;
  private final List<String> dimensions;
  private final List<String> metrics;
  @Nullable
  private final Long maxInputSegmentBytesPerTask;
  private final IndexIO indexIO;
  private final CoordinatorClient coordinatorClient;
  private final SegmentLoaderFactory segmentLoaderFactory;
  private final RetryPolicyFactory retryPolicyFactory;

  private List<InputSplit<List<WindowedSegmentId>>> splits;

  @JsonCreator
  public IngestSegmentFirehoseFactory(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") @Nullable Interval interval,
      // Specifying "segments" is intended only for when this FirehoseFactory has split itself,
      // not for direct end user use.
      @JsonProperty("segments") @Nullable List<WindowedSegmentId> segmentIds,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("maxInputSegmentBytesPerTask") @Deprecated @Nullable Long maxInputSegmentBytesPerTask,
      @JacksonInject IndexIO indexIO,
      @JacksonInject CoordinatorClient coordinatorClient,
      @JacksonInject SegmentLoaderFactory segmentLoaderFactory,
      @JacksonInject RetryPolicyFactory retryPolicyFactory
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    if ((interval == null && segmentIds == null) || (interval != null && segmentIds != null)) {
      throw new IAE("Specify exactly one of 'interval' and 'segments'");
    }
    this.dataSource = dataSource;
    this.interval = interval;
    this.segmentIds = segmentIds;
    this.dimFilter = dimFilter;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.maxInputSegmentBytesPerTask = maxInputSegmentBytesPerTask;
    this.indexIO = Preconditions.checkNotNull(indexIO, "null IndexIO");
    this.coordinatorClient = Preconditions.checkNotNull(coordinatorClient, "null CoordinatorClient");
    this.segmentLoaderFactory = Preconditions.checkNotNull(segmentLoaderFactory, "null SegmentLoaderFactory");
    this.retryPolicyFactory = Preconditions.checkNotNull(retryPolicyFactory, "null RetryPolicyFactory");
  }

  @Override
  public FiniteFirehoseFactory<InputRowParser, List<WindowedSegmentId>> withSplit(InputSplit<List<WindowedSegmentId>> split)
  {
    return new IngestSegmentFirehoseFactory(
        dataSource,
        null,
        split.get(),
        dimFilter,
        dimensions,
        metrics,
        maxInputSegmentBytesPerTask,
        indexIO,
        coordinatorClient,
        segmentLoaderFactory,
        retryPolicyFactory
    );
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Nullable
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  @Nullable
  public List<WindowedSegmentId> getSegments()
  {
    return segmentIds;
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public List<String> getMetrics()
  {
    return metrics;
  }

  @Nullable
  @JsonProperty
  public Long getMaxInputSegmentBytesPerTask()
  {
    return maxInputSegmentBytesPerTask;
  }

  @Override
  public Firehose connect(InputRowParser inputRowParser, File temporaryDirectory) throws ParseException
  {
    log.info(
        "Connecting firehose: dataSource[%s], interval[%s], segmentIds[%s]",
        dataSource,
        interval,
        segmentIds
    );

    final List<TimelineObjectHolder<String, DataSegment>> timeLineSegments = getTimeline();

    // Download all segments locally.
    // Note: this requires enough local storage space to fit all of the segments, even though
    // IngestSegmentFirehose iterates over the segments in series. We may want to change this
    // to download files lazily, perhaps sharing code with PrefetchableTextFilesFirehoseFactory.
    final SegmentLoader segmentLoader = segmentLoaderFactory.manufacturate(temporaryDirectory);
    Map<DataSegment, File> segmentFileMap = Maps.newLinkedHashMap();
    for (TimelineObjectHolder<String, DataSegment> holder : timeLineSegments) {
      for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
        final DataSegment segment = chunk.getObject();

        segmentFileMap.computeIfAbsent(segment, k -> {
          try {
            return segmentLoader.getSegmentFiles(segment);
          }
          catch (SegmentLoadingException e) {
            throw new RuntimeException(e);
          }
        });

      }
    }

    final List<String> dims;
    if (dimensions != null) {
      dims = dimensions;
    } else if (inputRowParser.getParseSpec().getDimensionsSpec().hasCustomDimensions()) {
      dims = inputRowParser.getParseSpec().getDimensionsSpec().getDimensionNames();
    } else {
      dims = getUniqueDimensions(
        timeLineSegments,
        inputRowParser.getParseSpec().getDimensionsSpec().getDimensionExclusions()
      );
    }

    final List<String> metricsList = metrics == null ? getUniqueMetrics(timeLineSegments) : metrics;

    final List<WindowedStorageAdapter> adapters = Lists.newArrayList(
        Iterables.concat(
        Iterables.transform(
          timeLineSegments,
          new Function<TimelineObjectHolder<String, DataSegment>, Iterable<WindowedStorageAdapter>>() {
            @Override
            public Iterable<WindowedStorageAdapter> apply(final TimelineObjectHolder<String, DataSegment> holder)
            {
              return
                Iterables.transform(
                  holder.getObject(),
                  new Function<PartitionChunk<DataSegment>, WindowedStorageAdapter>() {
                    @Override
                    public WindowedStorageAdapter apply(final PartitionChunk<DataSegment> input)
                    {
                      final DataSegment segment = input.getObject();
                      try {
                        return new WindowedStorageAdapter(
                          new QueryableIndexStorageAdapter(
                            indexIO.loadIndex(
                              Preconditions.checkNotNull(
                                segmentFileMap.get(segment),
                                "File for segment %s", segment.getId()
                              )
                            )
                          ),
                          holder.getInterval()
                        );
                      }
                      catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    }
                  }
                );
            }
          }
        )
      )
    );

    final TransformSpec transformSpec = TransformSpec.fromInputRowParser(inputRowParser);
    return new IngestSegmentFirehose(adapters, transformSpec, dims, metricsList, dimFilter);
  }

  private long jitter(long input)
  {
    final double jitter = ThreadLocalRandom.current().nextGaussian() * input / 4.0;
    long retval = input + (long) jitter;
    return retval < 0 ? 0 : retval;
  }

  private List<TimelineObjectHolder<String, DataSegment>> getTimeline()
  {
    if (interval == null) {
      return getTimelineForSegmentIds();
    } else {
      return getTimelineForInterval();
    }
  }

  private List<TimelineObjectHolder<String, DataSegment>> getTimelineForInterval()
  {
    Preconditions.checkNotNull(interval);

    // This call used to use the TaskActionClient, so for compatibility we use the same retry configuration
    // as TaskActionClient.
    final RetryPolicy retryPolicy = retryPolicyFactory.makeRetryPolicy();
    List<DataSegment> usedSegments;
    while (true) {
      try {
        usedSegments =
            coordinatorClient.getDatabaseSegmentDataSourceSegments(dataSource, Collections.singletonList(interval));
        break;
      }
      catch (Throwable e) {
        log.warn(e, "Exception getting database segments");
        final Duration delay = retryPolicy.getAndIncrementRetryDelay();
        if (delay == null) {
          throw e;
        } else {
          final long sleepTime = jitter(delay.getMillis());
          log.info("Will try again in [%s].", new Duration(sleepTime).toString());
          try {
            Thread.sleep(sleepTime);
          }
          catch (InterruptedException e2) {
            throw new RuntimeException(e2);
          }
        }
      }
    }

    return VersionedIntervalTimeline.forSegments(usedSegments).lookup(interval);
  }

  private List<TimelineObjectHolder<String, DataSegment>> getTimelineForSegmentIds()
  {
    final SortedMap<Interval, TimelineObjectHolder<String, DataSegment>> timeline = new TreeMap<>(
        Comparators.intervalsByStartThenEnd()
    );
    for (WindowedSegmentId windowedSegmentId : Preconditions.checkNotNull(segmentIds)) {
      final DataSegment segment = coordinatorClient.getDatabaseSegmentDataSourceSegment(
          dataSource,
          windowedSegmentId.getSegmentId()
      );
      for (Interval interval : windowedSegmentId.getIntervals()) {
        final TimelineObjectHolder<String, DataSegment> existingHolder = timeline.get(interval);
        if (existingHolder != null) {
          if (!existingHolder.getVersion().equals(segment.getVersion())) {
            throw new ISE("Timeline segments with the same interval should have the same version: " +
                          "existing version[%s] vs new segment[%s]", existingHolder.getVersion(), segment);
          }
          existingHolder.getObject().add(segment.getShardSpec().createChunk(segment));
        } else {
          timeline.put(interval, new TimelineObjectHolder<>(
              interval,
              segment.getInterval(),
              segment.getVersion(),
              new PartitionHolder<DataSegment>(segment.getShardSpec().createChunk(segment))
          ));
        }
      }
    }

    // Validate that none of the given windows overlaps (except for when multiple segments share exactly the
    // same interval).
    Interval lastInterval = null;
    for (Interval interval : timeline.keySet()) {
      if (lastInterval != null) {
        if (interval.overlaps(lastInterval)) {
          throw new IAE(
              "Distinct intervals in input segments may not overlap: [%s] vs [%s]",
              lastInterval,
              interval
          );
        }
      }
      lastInterval = interval;
    }

    return new ArrayList<>(timeline.values());
  }

  private void initializeSplitsIfNeeded(@Nullable SplitHintSpec splitHintSpec)
  {
    if (splits != null) {
      return;
    }

    final SegmentsSplitHintSpec nonNullSplitHintSpec;
    if (!(splitHintSpec instanceof SegmentsSplitHintSpec)) {
      if (splitHintSpec != null) {
        log.warn("Given splitHintSpec[%s] is not a SegmentsSplitHintSpec. Ingoring it.", splitHintSpec);
      }
      nonNullSplitHintSpec = new SegmentsSplitHintSpec(null);
    } else {
      nonNullSplitHintSpec = (SegmentsSplitHintSpec) splitHintSpec;
    }

    final long maxInputSegmentBytesPerTask = this.maxInputSegmentBytesPerTask == null
                                             ? nonNullSplitHintSpec.getMaxInputSegmentBytesPerTask()
                                             : this.maxInputSegmentBytesPerTask;

    // isSplittable() ensures this is only called when we have an interval.
    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = getTimelineForInterval();

    // We do the simplest possible greedy algorithm here instead of anything cleverer. The general bin packing
    // problem is NP-hard, and we'd like to get segments from the same interval into the same split so that their
    // data can combine with each other anyway.

    List<InputSplit<List<WindowedSegmentId>>> newSplits = new ArrayList<>();
    List<WindowedSegmentId> currentSplit = new ArrayList<>();
    Map<DataSegment, WindowedSegmentId> windowedSegmentIds = new HashMap<>();
    long bytesInCurrentSplit = 0;
    for (TimelineObjectHolder<String, DataSegment> timelineHolder : timelineSegments) {
      for (PartitionChunk<DataSegment> chunk : timelineHolder.getObject()) {
        final DataSegment segment = chunk.getObject();
        final WindowedSegmentId existingWindowedSegmentId = windowedSegmentIds.get(segment);
        if (existingWindowedSegmentId != null) {
          // We've already seen this segment in the timeline, so just add this interval to it. It has already
          // been placed into a split.
          existingWindowedSegmentId.getIntervals().add(timelineHolder.getInterval());
        } else {
          // It's the first time we've seen this segment, so create a new WindowedSegmentId.
          List<Interval> intervals = new ArrayList<>();
          // Use the interval that contributes to the timeline, not the entire segment's true interval.
          intervals.add(timelineHolder.getInterval());
          final WindowedSegmentId newWindowedSegmentId = new WindowedSegmentId(segment.getId().toString(), intervals);
          windowedSegmentIds.put(segment, newWindowedSegmentId);

          // Now figure out if it goes in the current split or not.
          final long segmentBytes = segment.getSize();
          if (bytesInCurrentSplit + segmentBytes > maxInputSegmentBytesPerTask && !currentSplit.isEmpty()) {
            // This segment won't fit in the current non-empty split, so this split is done.
            newSplits.add(new InputSplit<>(currentSplit));
            currentSplit = new ArrayList<>();
            bytesInCurrentSplit = 0;
          }
          if (segmentBytes > maxInputSegmentBytesPerTask) {
            // If this segment is itself bigger than our max, just put it in its own split.
            Preconditions.checkState(currentSplit.isEmpty() && bytesInCurrentSplit == 0);
            newSplits.add(new InputSplit<>(Collections.singletonList(newWindowedSegmentId)));
          } else {
            currentSplit.add(newWindowedSegmentId);
            bytesInCurrentSplit += segmentBytes;
          }
        }
      }
    }
    if (!currentSplit.isEmpty()) {
      newSplits.add(new InputSplit<>(currentSplit));
    }

    splits = newSplits;
  }

  @Override
  public boolean isSplittable()
  {
    // Specifying 'segments' to this factory instead of 'interval' is intended primarily for internal use by
    // parallel batch injection: we don't need to support splitting a list of segments.
    return interval != null;
  }

  @Override
  public Stream<InputSplit<List<WindowedSegmentId>>> getSplits(@Nullable SplitHintSpec splitHintSpec)
  {
    initializeSplitsIfNeeded(splitHintSpec);
    return splits.stream();
  }

  @Override
  public int getNumSplits(@Nullable SplitHintSpec splitHintSpec)
  {
    initializeSplitsIfNeeded(splitHintSpec);
    return splits.size();
  }

  @VisibleForTesting
  static List<String> getUniqueDimensions(
      List<TimelineObjectHolder<String, DataSegment>> timelineSegments,
      @Nullable Set<String> excludeDimensions
  )
  {
    final BiMap<String, Integer> uniqueDims = HashBiMap.create();

    // Here, we try to retain the order of dimensions as they were specified since the order of dimensions may be
    // optimized for performance.
    // Dimensions are extracted from the recent segments to olders because recent segments are likely to be queried more
    // frequently, and thus the performance should be optimized for recent ones rather than old ones.

    // timelineSegments are sorted in order of interval
    int index = 0;
    for (TimelineObjectHolder<String, DataSegment> timelineHolder : Lists.reverse(timelineSegments)) {
      for (PartitionChunk<DataSegment> chunk : timelineHolder.getObject()) {
        for (String dimension : chunk.getObject().getDimensions()) {
          if (!uniqueDims.containsKey(dimension) &&
              (excludeDimensions == null || !excludeDimensions.contains(dimension))) {
            uniqueDims.put(dimension, index++);
          }
        }
      }
    }

    final BiMap<Integer, String> orderedDims = uniqueDims.inverse();
    return IntStream.range(0, orderedDims.size())
                    .mapToObj(orderedDims::get)
                    .collect(Collectors.toList());
  }

  @VisibleForTesting
  static List<String> getUniqueMetrics(List<TimelineObjectHolder<String, DataSegment>> timelineSegments)
  {
    final BiMap<String, Integer> uniqueMetrics = HashBiMap.create();

    // Here, we try to retain the order of metrics as they were specified. Metrics are extracted from the recent
    // segments to olders.

    // timelineSegments are sorted in order of interval
    int[] index = {0};
    for (TimelineObjectHolder<String, DataSegment> timelineHolder : Lists.reverse(timelineSegments)) {
      for (PartitionChunk<DataSegment> chunk : timelineHolder.getObject()) {
        for (String metric : chunk.getObject().getMetrics()) {
          uniqueMetrics.computeIfAbsent(metric, k -> {
            return index[0]++;
          }
          );
        }
      }
    }

    final BiMap<Integer, String> orderedMetrics = uniqueMetrics.inverse();
    return IntStream.range(0, orderedMetrics.size())
                    .mapToObj(orderedMetrics::get)
                    .collect(Collectors.toList());
  }
}
