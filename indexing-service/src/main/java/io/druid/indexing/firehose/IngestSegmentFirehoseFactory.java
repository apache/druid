/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.firehose;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Injector;
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
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.TimestampColumnSelector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.utils.Runnables;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
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
      } else {
        Set<String> dimSet = new HashSet<>();
        for (TimelineObjectHolder<String, DataSegment> timelineObjectHolder : timeLineSegments) {
          dimSet.addAll(timelineObjectHolder.getObject().getChunk(0).getObject().getDimensions());
        }
        dims = Lists.newArrayList(dimSet);
      }

      List<String> metricsList;
      if (metrics != null) {
        metricsList = metrics;
      } else {
        Set<String> metricsSet = new HashSet<>();
        for (TimelineObjectHolder<String, DataSegment> timelineObjectHolder : timeLineSegments) {
          metricsSet.addAll(timelineObjectHolder.getObject().getChunk(0).getObject().getMetrics());
        }
        metricsList = Lists.newArrayList(metricsSet);
      }


      final List<StorageAdapter> adapters = Lists.transform(
          timeLineSegments,
          new Function<TimelineObjectHolder<String, DataSegment>, StorageAdapter>()
          {
            @Override
            public StorageAdapter apply(TimelineObjectHolder<String, DataSegment> input)
            {
              final DataSegment segment = input.getObject().getChunk(0).getObject();
              final File file = Preconditions.checkNotNull(
                  segmentFileMap.get(segment),
                  "File for segment %s", segment.getIdentifier()
              );

              try {
                return new QueryableIndexStorageAdapter((IndexIO.loadIndex(file)));
              }
              catch (IOException e) {
                throw Throwables.propagate(e);
              }
            }
          }
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

  @Override
  public InputRowParser getParser()
  {
    return null;
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
                      final TimestampColumnSelector timestampColumnSelector = cursor.makeTimestampColumnSelector();

                      final Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
                      for (String dim : dims) {
                        final DimensionSelector dimSelector = cursor.makeDimensionSelector(dim);
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
                                  final long timestamp = timestampColumnSelector.getTimestamp();
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
          new YieldingAccumulator()
          {
            @Override
            public Object accumulate(Object accumulated, Object in)
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
