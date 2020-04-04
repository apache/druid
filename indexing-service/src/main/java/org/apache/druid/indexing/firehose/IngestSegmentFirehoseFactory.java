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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
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
import org.apache.druid.indexing.common.ReingestionTimelineUtils;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.IAE;
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
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
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
    log.debug(
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

    final List<String> dims = ReingestionTimelineUtils.getDimensionsToReingest(
        dimensions,
        inputRowParser.getParseSpec().getDimensionsSpec(),
        timeLineSegments
    );
    final List<String> metricsList = metrics == null
                                     ? ReingestionTimelineUtils.getUniqueMetrics(timeLineSegments)
                                     : metrics;

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

  private List<TimelineObjectHolder<String, DataSegment>> getTimeline()
  {
    if (interval == null) {
      return DruidInputSource.getTimelineForSegmentIds(coordinatorClient, dataSource, segmentIds);
    } else {
      return DruidInputSource.getTimelineForInterval(coordinatorClient, retryPolicyFactory, dataSource, interval);
    }
  }

  private void initializeSplitsIfNeeded(@Nullable SplitHintSpec splitHintSpec)
  {
    if (splits != null) {
      return;
    }

    splits = Lists.newArrayList(
        DruidInputSource.createSplits(
            coordinatorClient,
            retryPolicyFactory,
            dataSource,
            interval,
            splitHintSpec == null ? new SegmentsSplitHintSpec(maxInputSegmentBytesPerTask) : splitHintSpec
        )
    );
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
}
