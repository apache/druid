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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.QueryInterruptedException;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionIndexer;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.Metadata;
import io.druid.segment.NullDimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.SingleScanTimeDimSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.filter.BooleanValueMatcher;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class IncrementalIndexStorageAdapter implements StorageAdapter
{
  private static final NullDimensionSelector NULL_DIMENSION_SELECTOR = new NullDimensionSelector();

  private final IncrementalIndex<?> index;

  public IncrementalIndexStorageAdapter(
      IncrementalIndex<?> index
  )
  {
    this.index = index;
  }

  @Override
  public String getSegmentIdentifier()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Interval getInterval()
  {
    return index.getInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<String>(index.getDimensionNames(), String.class);
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return index.getMetricNames();
  }

  @Override
  public int getDimensionCardinality(String dimension)
  {
    if (dimension.equals(Column.TIME_COLUMN_NAME)) {
      return Integer.MAX_VALUE;
    }

    IncrementalIndex.DimensionDesc desc = index.getDimension(dimension);
    if (desc == null) {
      return 0;
    }

    DimensionIndexer indexer = index.getDimension(dimension).getIndexer();
    return indexer.getCardinality();
  }

  @Override
  public int getNumRows()
  {
    return index.size();
  }

  @Override
  public DateTime getMinTime()
  {
    return index.getMinTime();
  }

  @Override
  public DateTime getMaxTime()
  {
    return index.getMaxTime();
  }

  @Override
  public Comparable getMinValue(String column)
  {
    IncrementalIndex.DimensionDesc desc = index.getDimension(column);
    if (desc == null) {
      return null;
    }

    DimensionIndexer indexer = desc.getIndexer();
    return indexer.getMinValue();
  }

  @Override
  public Comparable getMaxValue(String column)
  {
    IncrementalIndex.DimensionDesc desc = index.getDimension(column);
    if (desc == null) {
      return null;
    }

    DimensionIndexer indexer = desc.getIndexer();
    return indexer.getMaxValue();
  }

  @Override
  public Capabilities getCapabilities()
  {
    return Capabilities.builder().dimensionValuesSorted(false).build();
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return index.getCapabilities(column);
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return index.getDimensionHandlers();
  }

  @Override
  public String getColumnTypeName(String column)
  {
    final String metricType = index.getMetricType(column);
    return metricType != null ? metricType : getColumnCapabilities(column).getType().toString();
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return index.getMaxIngestedEventTime();
  }

  @Override
  public Sequence<Cursor> makeCursors(
      final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final QueryGranularity gran,
      final boolean descending
  )
  {
    if (index.isEmpty()) {
      return Sequences.empty();
    }

    Interval actualIntervalTmp = interval;

    final Interval dataInterval = new Interval(
        getMinTime().getMillis(),
        gran.next(gran.truncate(getMaxTime().getMillis()))
    );

    if (!actualIntervalTmp.overlaps(dataInterval)) {
      return Sequences.empty();
    }

    if (actualIntervalTmp.getStart().isBefore(dataInterval.getStart())) {
      actualIntervalTmp = actualIntervalTmp.withStart(dataInterval.getStart());
    }
    if (actualIntervalTmp.getEnd().isAfter(dataInterval.getEnd())) {
      actualIntervalTmp = actualIntervalTmp.withEnd(dataInterval.getEnd());
    }

    final Interval actualInterval = actualIntervalTmp;

    Iterable<Long> iterable = gran.iterable(actualInterval.getStartMillis(), actualInterval.getEndMillis());
    if (descending) {
      // might be better to be included in granularity#iterable
      iterable = Lists.reverse(ImmutableList.copyOf(iterable));
    }
    return Sequences.map(
        Sequences.simple(iterable),
        new Function<Long, Cursor>()
        {
          EntryHolder currEntry = new EntryHolder();

          @Override
          public Cursor apply(final Long input)
          {
            final long timeStart = Math.max(input, actualInterval.getStartMillis());

            return new Cursor()
            {
              private final ValueMatcher filterMatcher = makeFilterMatcher(filter, this);
              private Iterator<Map.Entry<IncrementalIndex.TimeAndDims, Integer>> baseIter;
              private Iterable<Map.Entry<IncrementalIndex.TimeAndDims, Integer>> cursorIterable;
              private boolean emptyRange;
              final DateTime time;
              int numAdvanced = -1;
              boolean done;

              {
                cursorIterable = index.getFacts().timeRangeIterable(
                    descending,
                    timeStart,
                    Math.min(actualInterval.getEndMillis(), gran.next(input))
                );
                emptyRange = !cursorIterable.iterator().hasNext();
                time = gran.toDateTime(input);

                reset();
              }

              @Override
              public DateTime getTime()
              {
                return time;
              }

              @Override
              public void advance()
              {
                if (!baseIter.hasNext()) {
                  done = true;
                  return;
                }

                while (baseIter.hasNext()) {
                  if (Thread.interrupted()) {
                    throw new QueryInterruptedException(new InterruptedException());
                  }

                  currEntry.set(baseIter.next());

                  if (filterMatcher.matches()) {
                    return;
                  }
                }

                if (!filterMatcher.matches()) {
                  done = true;
                }
              }

              @Override
              public void advanceTo(int offset)
              {
                int count = 0;
                while (count < offset && !isDone()) {
                  advance();
                  count++;
                }
              }

              @Override
              public boolean isDone()
              {
                return done;
              }

              @Override
              public void reset()
              {
                baseIter = cursorIterable.iterator();

                if (numAdvanced == -1) {
                  numAdvanced = 0;
                } else {
                  Iterators.advance(baseIter, numAdvanced);
                }

                if (Thread.interrupted()) {
                  throw new QueryInterruptedException(new InterruptedException());
                }

                boolean foundMatched = false;
                while (baseIter.hasNext()) {
                  currEntry.set(baseIter.next());
                  if (filterMatcher.matches()) {
                    foundMatched = true;
                    break;
                  }

                  numAdvanced++;
                }

                done = !foundMatched && (emptyRange || !baseIter.hasNext());
              }

              @Override
              public DimensionSelector makeDimensionSelector(
                  DimensionSpec dimensionSpec
              )
              {
                final String dimension = dimensionSpec.getDimension();
                final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

                if (dimension.equals(Column.TIME_COLUMN_NAME)) {
                  DimensionSelector selector = new SingleScanTimeDimSelector(
                      makeLongColumnSelector(dimension),
                      extractionFn,
                      descending
                  );
                  return dimensionSpec.decorate(selector);
                }

                final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(dimensionSpec.getDimension());
                if (dimensionDesc == null) {
                  return dimensionSpec.decorate(NULL_DIMENSION_SELECTOR);
                }

                final DimensionIndexer indexer = dimensionDesc.getIndexer();
                return dimensionSpec.decorate((DimensionSelector) indexer.makeColumnValueSelector(dimensionSpec, currEntry, dimensionDesc));
              }

              @Override
              public FloatColumnSelector makeFloatColumnSelector(String columnName)
              {
                final Integer dimIndex = index.getDimensionIndex(columnName);
                if (dimIndex != null) {
                  final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
                  final DimensionIndexer indexer = dimensionDesc.getIndexer();
                  return (FloatColumnSelector) indexer.makeColumnValueSelector(
                      new DefaultDimensionSpec(columnName, null),
                      currEntry,
                      dimensionDesc
                  );
                }

                final Integer metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt == null) {
                  return new FloatColumnSelector()
                  {
                    @Override
                    public float get()
                    {
                      return 0.0f;
                    }
                  };
                }

                final int metricIndex = metricIndexInt;
                return new FloatColumnSelector()
                {
                  @Override
                  public float get()
                  {
                    return index.getMetricFloatValue(currEntry.getValue(), metricIndex);
                  }
                };
              }

              @Override
              public LongColumnSelector makeLongColumnSelector(String columnName)
              {
                if (columnName.equals(Column.TIME_COLUMN_NAME)) {
                  return new LongColumnSelector()
                  {
                    @Override
                    public long get()
                    {
                      return currEntry.getKey().getTimestamp();
                    }
                  };
                }

                final Integer dimIndex = index.getDimensionIndex(columnName);
                if (dimIndex != null) {
                  final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
                  final DimensionIndexer indexer = dimensionDesc.getIndexer();
                  return (LongColumnSelector) indexer.makeColumnValueSelector(
                      new DefaultDimensionSpec(columnName, null),
                      currEntry,
                      dimensionDesc
                  );
                }

                final Integer metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt == null) {
                  return new LongColumnSelector()
                  {
                    @Override
                    public long get()
                    {
                      return 0L;
                    }
                  };
                }

                final int metricIndex = metricIndexInt;

                return new LongColumnSelector()
                {
                  @Override
                  public long get()
                  {
                    return index.getMetricLongValue(
                        currEntry.getValue(),
                        metricIndex
                    );
                  }
                };
              }

              @Override
              public ObjectColumnSelector makeObjectColumnSelector(String column)
              {
                if (column.equals(Column.TIME_COLUMN_NAME)) {
                  return new ObjectColumnSelector<Long>()
                  {
                    @Override
                    public Class classOfObject()
                    {
                      return Long.TYPE;
                    }

                    @Override
                    public Long get()
                    {
                      return currEntry.getKey().getTimestamp();
                    }
                  };
                }

                final Integer metricIndexInt = index.getMetricIndex(column);
                if (metricIndexInt != null) {
                  final int metricIndex = metricIndexInt;
                  final Class classOfObject = index.getMetricClass(column);
                  return new ObjectColumnSelector()
                  {
                    @Override
                    public Class classOfObject()
                    {
                      return classOfObject;
                    }

                    @Override
                    public Object get()
                    {
                      return index.getMetricObjectValue(
                          currEntry.getValue(),
                          metricIndex
                      );
                    }
                  };
                }

                IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(column);

                if (dimensionDesc == null) {
                  VirtualColumn virtualColumn = virtualColumns.getVirtualColumn(column);
                  if (virtualColumn != null) {
                    return virtualColumn.init(column, this);
                  }
                  return null;
                } else {

                  final int dimensionIndex = dimensionDesc.getIndex();
                  final DimensionIndexer indexer = dimensionDesc.getIndexer();

                  return new ObjectColumnSelector<Object>()
                  {
                    @Override
                    public Class classOfObject()
                    {
                      return Object.class;
                    }

                    @Override
                    public Object get()
                    {
                      IncrementalIndex.TimeAndDims key = currEntry.getKey();
                      if (key == null) {
                        return null;
                      }

                      Object[] dims = key.getDims();
                      if (dimensionIndex >= dims.length) {
                        return null;
                      }

                      return indexer.convertUnsortedEncodedArrayToActualArrayOrList(
                          dims[dimensionIndex], DimensionIndexer.ARRAY
                      );
                    }
                  };
                }
              }

              @Nullable
              @Override
              public ColumnCapabilities getColumnCapabilities(String columnName)
              {
                ColumnCapabilities capabilities = index.getCapabilities(columnName);
                if (capabilities == null && !virtualColumns.isEmpty()) {
                  VirtualColumn virtualColumn = virtualColumns.getVirtualColumn(columnName);
                  if (virtualColumn != null) {
                    Class clazz = virtualColumn.init(columnName, this).classOfObject();
                    capabilities = new ColumnCapabilitiesImpl().setType(ValueType.typeFor(clazz));
                  }
                }
                return capabilities;
              }
            };
          }
        }
    );
  }

  private ValueMatcher makeFilterMatcher(final Filter filter, final Cursor cursor)
  {
    return filter == null
           ? new BooleanValueMatcher(true)
           : filter.makeMatcher(cursor);
  }

  public static class EntryHolder
  {
    Map.Entry<IncrementalIndex.TimeAndDims, Integer> currEntry = null;

    public Map.Entry<IncrementalIndex.TimeAndDims, Integer> get()
    {
      return currEntry;
    }

    public void set(Map.Entry<IncrementalIndex.TimeAndDims, Integer> currEntry)
    {
      this.currEntry = currEntry;
    }

    public IncrementalIndex.TimeAndDims getKey()
    {
      return currEntry.getKey();
    }

    public Integer getValue()
    {
      return currEntry.getValue();
    }
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }
}
