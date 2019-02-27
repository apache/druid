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
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexing.common.RetryPolicy;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class IngestSegmentFirehoseFactory implements FiniteFirehoseFactory<InputRowParser, List<WindowedSegmentId>>
{
  private static final EmittingLogger log = new EmittingLogger(IngestSegmentFirehoseFactory.class);
  private static final long DEFAULT_MAX_INPUT_SEGMENT_BYTES_PER_TASK = 150 * 1024 * 1024;
  private final String dataSource;
  private final Interval interval;
  private final List<WindowedSegmentId> segmentIds;
  private final DimFilter dimFilter;
  private final List<String> dimensions;
  private final List<String> metrics;
  private final long maxInputSegmentBytesPerTask;
  private List<InputSplit<List<WindowedSegmentId>>> splits;
  private final IndexIO indexIO;
  private final CoordinatorClient coordinatorClient;
  private final SegmentLoaderFactory segmentLoaderFactory;
  private final RetryPolicyFactory retryPolicyFactory;

  @JsonCreator
  public IngestSegmentFirehoseFactory(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("segments") List<WindowedSegmentId> segmentIds,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("maxInputSegmentBytesPerTask") Long maxInputSegmentBytesPerTask,
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
    this.maxInputSegmentBytesPerTask = maxInputSegmentBytesPerTask == null
                                       ? DEFAULT_MAX_INPUT_SEGMENT_BYTES_PER_TASK
                                       : maxInputSegmentBytesPerTask;
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
  public Interval getInterval()
  {
    return interval;
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

  @JsonProperty
  public List<WindowedSegmentId> getSegments()
  {
    return segmentIds;
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

    try {
      final List<WindowedSegment> windowedSegments = getWindowedSegments();

      final SegmentLoader segmentLoader = segmentLoaderFactory.manufacturate(temporaryDirectory);
      Map<DataSegment, File> segmentFileMap = Maps.newLinkedHashMap();
      for (WindowedSegment windowedSegment : windowedSegments) {
        final DataSegment segment = windowedSegment.getSegment();
        segmentFileMap.put(segment, segmentLoader.getSegmentFiles(segment));
      }

      final List<String> dims;
      if (dimensions != null) {
        dims = dimensions;
      } else if (inputRowParser.getParseSpec().getDimensionsSpec().hasCustomDimensions()) {
        dims = inputRowParser.getParseSpec().getDimensionsSpec().getDimensionNames();
      } else {
        dims = getUniqueDimensions(
            windowedSegments,
            inputRowParser.getParseSpec().getDimensionsSpec().getDimensionExclusions()
        );
      }

      final List<String> metricsList = metrics == null ? getUniqueMetrics(windowedSegments) : metrics;

      // FIXME If we have a segment A that goes from 1 to 4, and a segment B from 2 to 3 with a later
      // version, then this list of adapters will be: A[1-2], A[3-4], B[2-3]. Before this PR it would
      // be A[1-2], B[2-3], A[3-4]. Is this change OK, or should we endeavor to keep things in order?
      // Can we just do a sort on the list at the end to fix this?
      final List<WindowedStorageAdapter> adapters = Lists.newArrayList(
          Iterables.concat(
              Iterables.transform(
                  windowedSegments,
                  new Function<WindowedSegment, Iterable<WindowedStorageAdapter>>()
                  {
                    @Override
                    public Iterable<WindowedStorageAdapter> apply(final WindowedSegment windowedSegment)
                    {
                      return
                          Iterables.transform(
                              windowedSegment.getIntervals(),
                              new Function<Interval, WindowedStorageAdapter>()
                              {
                                @Override
                                public WindowedStorageAdapter apply(final Interval interval)
                                {
                                  final DataSegment segment = windowedSegment.getSegment();
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
                                        interval
                                    );
                                  }
                                  catch (IOException e) {
                                    throw Throwables.propagate(e);
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
    catch (SegmentLoadingException e) {
      throw Throwables.propagate(e);
    }
  }

  private long jitter(long input)
  {
    final double jitter = ThreadLocalRandom.current().nextGaussian() * input / 4.0;
    long retval = input + (long) jitter;
    return retval < 0 ? 0 : retval;
  }

  // Returns each segment in the interval, along with the sub-intervals from that segment which contribute
  // to the timeline. Segments are sorted by the first instant which contributes to the timeline.
  private List<WindowedSegment> getWindowedSegmentsForInterval(Interval interval)
  {
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

    final List<TimelineObjectHolder<String, DataSegment>> timeLineSegments = VersionedIntervalTimeline
        .forSegments(usedSegments)
        .lookup(interval);

    final List<WindowedSegment> windowedSegments = new ArrayList<>();
    final Map<DataSegment, WindowedSegment> windowedSegmentsBySegment = new HashMap<>();

    for (TimelineObjectHolder<String, DataSegment> timelineHolder : timeLineSegments) {
      for (PartitionChunk<DataSegment> chunk : timelineHolder.getObject()) {
        final DataSegment segment = chunk.getObject();
        if (timelineHolder.getInterval().isEqual(timelineHolder.getTrueInterval())) {
          // This is the simple case: the entirety of the segment is used: no overshadowing or clipping
          // from the beginning or end of the full interval. We aren't going to see it again.
          if (windowedSegmentsBySegment.containsKey(segment)) {
            throw new ISE("unclipped segment[%s] seen more than once on timeline", segment);
          }
          final WindowedSegment windowedSegment = new WindowedSegment(segment, null);
          windowedSegments.add(windowedSegment);
          windowedSegmentsBySegment.put(segment, windowedSegment);
        } else {
          // Some part of the segment is overshadowed or clipped. This WindowedSegment will need
          // to have explicit intervals.
          final WindowedSegment existingWindowedSegment = windowedSegmentsBySegment.get(segment);
          if (existingWindowedSegment == null) {
            final List<Interval> intervals = new ArrayList<>();
            intervals.add(timelineHolder.getInterval());
            final WindowedSegment newWindowedSegment = new WindowedSegment(segment, intervals);
            windowedSegments.add(newWindowedSegment);
            windowedSegmentsBySegment.put(segment, newWindowedSegment);
          } else {
            if (!existingWindowedSegment.hasExplicitIntervals()) {
              throw new ISE("unclipped segment[%s] seen later clipped on timeline", segment);
            }
            existingWindowedSegment.getIntervals().add(timelineHolder.getInterval());
          }
        }
      }
    }

    return windowedSegments;
  }

  private List<WindowedSegment> getWindowedSegmentsForIds(List<WindowedSegmentId> ids)
  {
    // FIXME This is doing a series of single HTTP calls for each segment. It could instead do a single
    // GET /metadata/datasources/DATASOURCE/segments call to download all DataSegments. I'm erring on the
    // side of avoiding single unboundedly-large calls, in favor of more smaller roundtrip --- after all,
    // we're going to later fetch each of these segments in series, so the additional coordinator calls
    // in series here don't feel that bad. And adding a new "get multiple segments by ID" HTTP call seems
    // like overkill.
    //
    // Also: If the only use of WindowedSegments and the "segments" field on this FirehoseFactory is for internal
    // use by parallel ingestion, then it might make sense to get rid of WindowedSegmentId entirely and just put
    // WindowedSegments directly into this class's 'segments' parameter. Then we wouldn't even need to do this fetch
    // at all. But if we think people might actually appreciate being able to write ingestSegment firehoses specifying
    // specific segments by hand, then WindowedSegmentIds still makes sense. Plus would it be safe to trust the
    // DataSegment written in a spec isntead of one fetched from the DB? I note that CompactionTask has an undocumented
    // "segments" parameter which has similar semantics, though I'm not really sure in what context it is used.
    return ids.stream()
              .map(wsi -> new WindowedSegment(
                  coordinatorClient.getDatabaseSegmentDataSourceSegment(dataSource, wsi.getSegmentId()),
                  wsi.getIntervals()
              ))
              .collect(Collectors.toList());
  }

  private List<WindowedSegment> getWindowedSegments()
  {
    if (segmentIds == null) {
      return getWindowedSegmentsForInterval(Preconditions.checkNotNull(interval));
    } else {
      return getWindowedSegmentsForIds(segmentIds);
    }
  }

  private void initializeSplitsIfNeeded()
  {
    if (splits != null) {
      return;
    }

    List<WindowedSegment> windowedSegments = getWindowedSegments();

    // We do the simplest possible greedy algorithm here instead of anything cleverer. The general bin packing
    // problem is NP-hard, and we'd like to get segments from the same interval into the same split so that their
    // data can combine with each other anyway.

    List<InputSplit<List<WindowedSegmentId>>> newSplits = new ArrayList<>();
    List<WindowedSegmentId> currentSplit = new ArrayList<>();
    long bytesInCurrentSplit = 0;
    for (WindowedSegment windowedSegment : windowedSegments) {
      final long segmentBytes = windowedSegment.getSegment().getSize();
      if (bytesInCurrentSplit + segmentBytes > maxInputSegmentBytesPerTask && !currentSplit.isEmpty()) {
        // This segment won't fit in the current (non-empty) split, so this split is done.
        newSplits.add(new InputSplit<>(currentSplit));
        currentSplit = new ArrayList<>();
        bytesInCurrentSplit = 0;
      }
      if (segmentBytes > maxInputSegmentBytesPerTask) {
        // If this segment is itself bigger than our max, just put it in its own split.
        Preconditions.checkState(currentSplit.isEmpty() && bytesInCurrentSplit == 0);
        newSplits.add(new InputSplit<>(Collections.singletonList(windowedSegment.toWindowedSegmentId())));
      } else {
        currentSplit.add(windowedSegment.toWindowedSegmentId());
        bytesInCurrentSplit += segmentBytes;
      }
    }
    if (!currentSplit.isEmpty()) {
      newSplits.add(new InputSplit<>(currentSplit));
    }

    splits = newSplits;
  }

  @Override
  public Stream<InputSplit<List<WindowedSegmentId>>> getSplits()
  {
    initializeSplitsIfNeeded();
    return splits.stream();
  }

  @Override
  public int getNumSplits()
  {
    initializeSplitsIfNeeded();
    return splits.size();
  }

  @VisibleForTesting
  static List<String> getUniqueDimensions(
      List<WindowedSegment> windowedSegments,
      @Nullable Set<String> excludeDimensions
  )
  {
    final BiMap<String, Integer> uniqueDims = HashBiMap.create();

    // Here, we try to retain the order of dimensions as they were specified since the order of dimensions may be
    // optimized for performance.
    // Dimensions are extracted from the recent segments to olders because recent segments are likely to be queried more
    // frequently, and thus the performance should be optimized for recent ones rather than old ones.

    // windowedSegments are sorted in order of their first interval
    int index = 0;
    for (WindowedSegment windowedSegment : Lists.reverse(windowedSegments)) {
      for (String dimension : windowedSegment.getSegment().getDimensions()) {
        if (!uniqueDims.containsKey(dimension) &&
            (excludeDimensions == null || !excludeDimensions.contains(dimension))) {
          uniqueDims.put(dimension, index++);
        }
      }
    }

    final BiMap<Integer, String> orderedDims = uniqueDims.inverse();
    return IntStream.range(0, orderedDims.size())
                    .mapToObj(orderedDims::get)
                    .collect(Collectors.toList());
  }

  @VisibleForTesting
  static List<String> getUniqueMetrics(List<WindowedSegment> windowedSegments)
  {
    final BiMap<String, Integer> uniqueMetrics = HashBiMap.create();

    // Here, we try to retain the order of metrics as they were specified. Metrics are extracted from the recent
    // segments to olders.

    // windowedSegments are sorted in order of their first interval
    int index = 0;
    for (WindowedSegment windowedSegment : Lists.reverse(windowedSegments)) {
      for (String metric : windowedSegment.getSegment().getMetrics()) {
        if (!uniqueMetrics.containsKey(metric)) {
          uniqueMetrics.put(metric, index++);
        }
      }
    }

    final BiMap<Integer, String> orderedMetrics = uniqueMetrics.inverse();
    return IntStream.range(0, orderedMetrics.size())
                    .mapToObj(orderedMetrics::get)
                    .collect(Collectors.toList());
  }
}
