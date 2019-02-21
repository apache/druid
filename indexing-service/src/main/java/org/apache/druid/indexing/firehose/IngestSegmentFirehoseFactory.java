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
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentListUsedAction;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.firehose.IngestSegmentFirehose;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IngestSegmentFirehoseFactory implements FirehoseFactory<InputRowParser>
{
  private static final EmittingLogger log = new EmittingLogger(IngestSegmentFirehoseFactory.class);
  private final String dataSource;
  private final Interval interval;
  private final DimFilter dimFilter;
  private final List<String> dimensions;
  private final List<String> metrics;
  private final IndexIO indexIO;
  private TaskToolbox taskToolbox;

  @JsonCreator
  public IngestSegmentFirehoseFactory(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JacksonInject IndexIO indexIO
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(interval, "interval");
    this.dataSource = dataSource;
    this.interval = interval;
    this.dimFilter = dimFilter;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.indexIO = Preconditions.checkNotNull(indexIO, "null IndexIO");
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

  public void setTaskToolbox(TaskToolbox taskToolbox)
  {
    this.taskToolbox = taskToolbox;
  }

  @Override
  public Firehose connect(InputRowParser inputRowParser, File temporaryDirectory) throws ParseException
  {
    log.info("Connecting firehose: dataSource[%s], interval[%s]", dataSource, interval);

    Preconditions.checkNotNull(taskToolbox, "taskToolbox is not set");

    try {
      final List<DataSegment> usedSegments = taskToolbox
          .getTaskActionClient()
          .submit(new SegmentListUsedAction(dataSource, interval, null));
      final Map<DataSegment, File> segmentFileMap = taskToolbox.fetchSegments(usedSegments);
      final List<TimelineObjectHolder<String, DataSegment>> timeLineSegments = VersionedIntervalTimeline
          .forSegments(usedSegments)
          .lookup(interval);

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
                  new Function<TimelineObjectHolder<String, DataSegment>, Iterable<WindowedStorageAdapter>>()
                  {
                    @Override
                    public Iterable<WindowedStorageAdapter> apply(final TimelineObjectHolder<String, DataSegment> holder)
                    {
                      return
                          Iterables.transform(
                              holder.getObject(),
                              new Function<PartitionChunk<DataSegment>, WindowedStorageAdapter>()
                              {
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
    catch (IOException | SegmentLoadingException e) {
      throw Throwables.propagate(e);
    }
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
    int index = 0;
    for (TimelineObjectHolder<String, DataSegment> timelineHolder : Lists.reverse(timelineSegments)) {
      for (PartitionChunk<DataSegment> chunk : timelineHolder.getObject()) {
        for (String metric : chunk.getObject().getMetrics()) {
          if (!uniqueMetrics.containsKey(metric)) {
            uniqueMetrics.put(metric, index++);
          }
        }
      }
    }

    final BiMap<Integer, String> orderedMetrics = uniqueMetrics.inverse();
    return IntStream.range(0, orderedMetrics.size())
        .mapToObj(orderedMetrics::get)
        .collect(Collectors.toList());
  }
}
