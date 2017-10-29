/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.firehose;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.task.NoopTask;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.filter.DimFilter;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.realtime.firehose.IngestSegmentFirehose;
import io.druid.segment.realtime.firehose.WindowedStorageAdapter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IngestSegmentFirehoseFactory implements FirehoseFactory<InputRowParser>
{
  private static final EmittingLogger log = new EmittingLogger(IngestSegmentFirehoseFactory.class);
  private final String dataSource;
  private final Interval interval;
  private final DimFilter dimFilter;
  private final List<String> dimensions;
  private final List<String> metrics;
  private final Injector injector;
  private final IndexIO indexIO;
  private TaskToolbox taskToolbox;

  @JsonCreator
  public IngestSegmentFirehoseFactory(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JacksonInject Injector injector,
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
    this.injector = injector;
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
  public Firehose connect(InputRowParser inputRowParser, File temporaryDirectory) throws IOException, ParseException
  {
    log.info("Connecting firehose: dataSource[%s], interval[%s]", dataSource, interval);

    if (taskToolbox == null) {
      // Noop Task is just used to create the toolbox and list segments.
      taskToolbox = injector.getInstance(TaskToolboxFactory.class).build(
          new NoopTask("reingest", 0, 0, null, null, null)
      );
    }

    try {
      final List<DataSegment> usedSegments = taskToolbox
          .getTaskActionClient()
          .submit(new SegmentListUsedAction(dataSource, interval, null));
      final Map<DataSegment, File> segmentFileMap = taskToolbox.fetchSegments(usedSegments);
      VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(
          Comparators.naturalNullsFirst()
      );

      for (DataSegment segment : usedSegments) {
        timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
      }
      final List<TimelineObjectHolder<String, DataSegment>> timeLineSegments = timeline.lookup(
          interval
      );

      final List<String> dims;
      if (dimensions != null) {
        dims = dimensions;
      } else if (inputRowParser.getParseSpec().getDimensionsSpec().hasCustomDimensions()) {
        dims = inputRowParser.getParseSpec().getDimensionsSpec().getDimensionNames();
      } else {
        Set<String> dimSet = Sets.newHashSet(
            Iterables.concat(
                Iterables.transform(
                    timeLineSegments,
                    new Function<TimelineObjectHolder<String, DataSegment>, Iterable<String>>()
                    {
                      @Override
                      public Iterable<String> apply(
                          TimelineObjectHolder<String, DataSegment> timelineObjectHolder
                      )
                      {
                        return Iterables.concat(
                            Iterables.transform(
                                timelineObjectHolder.getObject(),
                                new Function<PartitionChunk<DataSegment>, Iterable<String>>()
                                {
                                  @Override
                                  public Iterable<String> apply(PartitionChunk<DataSegment> input)
                                  {
                                    return input.getObject().getDimensions();
                                  }
                                }
                            )
                        );
                      }
                    }

                )
            )
        );
        dims = Lists.newArrayList(
            Sets.difference(
                dimSet,
                inputRowParser
                    .getParseSpec()
                    .getDimensionsSpec()
                    .getDimensionExclusions()
            )
        );
      }

      final List<String> metricsList;
      if (metrics != null) {
        metricsList = metrics;
      } else {
        Set<String> metricsSet = Sets.newHashSet(
            Iterables.concat(
                Iterables.transform(
                    timeLineSegments,
                    new Function<TimelineObjectHolder<String, DataSegment>, Iterable<String>>()
                    {
                      @Override
                      public Iterable<String> apply(
                          TimelineObjectHolder<String, DataSegment> input
                      )
                      {
                        return Iterables.concat(
                            Iterables.transform(
                                input.getObject(),
                                new Function<PartitionChunk<DataSegment>, Iterable<String>>()
                                {
                                  @Override
                                  public Iterable<String> apply(PartitionChunk<DataSegment> input)
                                  {
                                    return input.getObject().getMetrics();
                                  }
                                }
                            )
                        );
                      }
                    }
                )
            )
        );
        metricsList = Lists.newArrayList(metricsSet);
      }


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
                                                    "File for segment %s", segment.getIdentifier()
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

      return new IngestSegmentFirehose(adapters, dims, metricsList, dimFilter);
    }
    catch (IOException | SegmentLoadingException e) {
      throw Throwables.propagate(e);
    }

  }
}
