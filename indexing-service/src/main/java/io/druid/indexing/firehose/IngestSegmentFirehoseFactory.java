/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.parsers.ParseException;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.granularity.QueryGranularity;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.task.NoopTask;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.EventHolder;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.IndexIO;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.utils.Runnables;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
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

  @JsonCreator
  public IngestSegmentFirehoseFactory(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JacksonInject Injector injector
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

  @Override
  public Firehose connect(InputRowParser inputRowParser) throws IOException, ParseException
  {
    log.info("Connecting firehose: dataSource[%s], interval[%s]", dataSource, interval);
    // better way to achieve this is to pass toolbox to Firehose, The instance is initialized Lazily on connect method.
    // Noop Task is just used to create the toolbox and list segments.
    final TaskToolbox toolbox = injector.getInstance(TaskToolboxFactory.class).build(
        new NoopTask("reingest", 0, 0, null, null)
    );

    try {
      final List<DataSegment> usedSegments = toolbox
          .getTaskActionClient()
          .submit(new SegmentListUsedAction(dataSource, interval));
      final Map<DataSegment, File> segmentFileMap = toolbox.fetchSegments(usedSegments);
      VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<String, DataSegment>(
          Ordering.<String>natural().nullsFirst()
      );

      for (DataSegment segment : usedSegments) {
        timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
      }
      final List<TimelineObjectHolder<String, DataSegment>> timeLineSegments = timeline.lookup(
          interval
      );

      List<String> dims;
      if (dimensions != null) {
        dims = dimensions;
      } else if (inputRowParser.getParseSpec().getDimensionsSpec().hasCustomDimensions()) {
        dims = inputRowParser.getParseSpec().getDimensionsSpec().getDimensions();
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

      List<String> metricsList;
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


      final List<StorageAdapter> adapters = Lists.newArrayList(
          Iterables.concat(
              Iterables.transform(
                  timeLineSegments,
                  new Function<TimelineObjectHolder<String, DataSegment>, Iterable<StorageAdapter>>()
                  {
                    @Override
                    public Iterable<StorageAdapter> apply(
                        TimelineObjectHolder<String, DataSegment> input
                    )
                    {
                      return
                          Iterables.transform(
                              input.getObject(),
                              new Function<PartitionChunk<DataSegment>, StorageAdapter>()
                              {
                                @Override
                                public StorageAdapter apply(PartitionChunk<DataSegment> input)
                                {
                                  final DataSegment segment = input.getObject();
                                  try {
                                    return new QueryableIndexStorageAdapter(
                                        IndexIO.loadIndex(
                                            Preconditions.checkNotNull(
                                                segmentFileMap.get(segment),
                                                "File for segment %s", segment.getIdentifier()
                                            )
                                        )
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

      return new IngestSegmentFirehose(adapters, dims, metricsList);

    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    catch (SegmentLoadingException e) {
      throw Throwables.propagate(e);
    }

  }

  public class IngestSegmentFirehose implements Firehose
  {
    private volatile Yielder<InputRow> rowYielder;

    public IngestSegmentFirehose(List<StorageAdapter> adapters, final List<String> dims, final List<String> metrics)
    {
      Sequence<InputRow> rows = Sequences.concat(
          Iterables.transform(
              adapters, new Function<StorageAdapter, Sequence<InputRow>>()
              {
                @Nullable
                @Override
                public Sequence<InputRow> apply(StorageAdapter adapter)
                {
                  return Sequences.concat(
                      Sequences.map(
                          adapter.makeCursors(
                              Filters.convertDimensionFilters(dimFilter),
                              interval,
                              QueryGranularity.ALL
                          ), new Function<Cursor, Sequence<InputRow>>()
                          {
                            @Nullable
                            @Override
                            public Sequence<InputRow> apply(final Cursor cursor)
                            {
                              final LongColumnSelector timestampColumnSelector = cursor.makeLongColumnSelector(Column.TIME_COLUMN_NAME);

                              final Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
                              for (String dim : dims) {
                                final DimensionSelector dimSelector = cursor.makeDimensionSelector(dim, null);
                                // dimSelector is null if the dimension is not present
                                if (dimSelector != null) {
                                  dimSelectors.put(dim, dimSelector);
                                }
                              }

                              final Map<String, ObjectColumnSelector> metSelectors = Maps.newHashMap();
                              for (String metric : metrics) {
                                final ObjectColumnSelector metricSelector = cursor.makeObjectColumnSelector(metric);
                                if (metricSelector != null) {
                                  metSelectors.put(metric, metricSelector);
                                }
                              }

                              return Sequences.simple(
                                  new Iterable<InputRow>()
                                  {
                                    @Override
                                    public Iterator<InputRow> iterator()
                                    {
                                      return new Iterator<InputRow>()
                                      {
                                        @Override
                                        public boolean hasNext()
                                        {
                                          return !cursor.isDone();
                                        }

                                        @Override
                                        public InputRow next()
                                        {
                                          final Map<String, Object> theEvent = Maps.newLinkedHashMap();
                                          final long timestamp = timestampColumnSelector.get();
                                          theEvent.put(EventHolder.timestampKey, new DateTime(timestamp));

                                          for (Map.Entry<String, DimensionSelector> dimSelector : dimSelectors.entrySet()) {
                                            final String dim = dimSelector.getKey();
                                            final DimensionSelector selector = dimSelector.getValue();
                                            final IndexedInts vals = selector.getRow();

                                            if (vals.size() == 1) {
                                              final String dimVal = selector.lookupName(vals.get(0));
                                              theEvent.put(dim, dimVal);
                                            } else {
                                              List<String> dimVals = Lists.newArrayList();
                                              for (int i = 0; i < vals.size(); ++i) {
                                                dimVals.add(selector.lookupName(vals.get(i)));
                                              }
                                              theEvent.put(dim, dimVals);
                                            }
                                          }

                                          for (Map.Entry<String, ObjectColumnSelector> metSelector : metSelectors.entrySet()) {
                                            final String metric = metSelector.getKey();
                                            final ObjectColumnSelector selector = metSelector.getValue();
                                            theEvent.put(metric, selector.get());
                                          }
                                          cursor.advance();
                                          return new MapBasedInputRow(timestamp, dims, theEvent);
                                        }

                                        @Override
                                        public void remove()
                                        {
                                          throw new UnsupportedOperationException("Remove Not Supported");
                                        }
                                      };
                                    }
                                  }
                              );
                            }
                          }
                      )
                  );
                }
              }
          )
      );
      rowYielder = rows.toYielder(
          null,
          new YieldingAccumulator<InputRow, InputRow>()
          {
            @Override
            public InputRow accumulate(InputRow accumulated, InputRow in)
            {
              yield();
              return in;
            }
          }
      );
    }

    @Override
    public boolean hasMore()
    {
      return !rowYielder.isDone();
    }

    @Override
    public InputRow nextRow()
    {
      final InputRow inputRow = rowYielder.get();
      rowYielder = rowYielder.next(null);
      return inputRow;
    }

    @Override
    public Runnable commit()
    {
      return Runnables.getNoopRunnable();
    }

    @Override
    public void close() throws IOException
    {
      rowYielder.close();
    }
  }
}
