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
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.BaseQuery;
import io.druid.query.QueryMetrics;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionIndexer;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.DoubleWrappingDimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.FloatWrappingDimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.LongWrappingDimensionSelector;
import io.druid.segment.Metadata;
import io.druid.segment.NullDimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.SingleScanTimeDimSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.ZeroDoubleColumnSelector;
import io.druid.segment.ZeroFloatColumnSelector;
import io.druid.segment.ZeroLongColumnSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
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
      final Granularity gran,
      final boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    if (index.isEmpty()) {
      return Sequences.empty();
    }

    Interval actualIntervalTmp = interval;

    final Interval dataInterval = new Interval(
        getMinTime().getMillis(),
        gran.bucketEnd(getMaxTime()).getMillis()
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

    Iterable<Interval> iterable = gran.getIterable(actualInterval);
    if (descending) {
      iterable = Lists.reverse(ImmutableList.copyOf(iterable));
    }

    return Sequences.map(
        Sequences.simple(iterable),
        new Function<Interval, Cursor>()
        {
          EntryHolder currEntry = new EntryHolder();

          @Override
          public Cursor apply(@Nullable final Interval interval)
          {
            final long timeStart = Math.max(interval.getStartMillis(), actualInterval.getStartMillis());

            return new Cursor()
            {
              private final ValueMatcher filterMatcher = makeFilterMatcher(filter, this);
              private final int maxRowIndex;
              private Iterator<IncrementalIndex.TimeAndDims> baseIter;
              private Iterable<IncrementalIndex.TimeAndDims> cursorIterable;
              private boolean emptyRange;
              final DateTime time;
              int numAdvanced = -1;
              boolean done;

              {
                maxRowIndex = index.getLastRowIndex();
                cursorIterable = index.getFacts().timeRangeIterable(
                    descending,
                    timeStart,
                    Math.min(actualInterval.getEndMillis(), gran.increment(interval.getStart()).getMillis())
                );
                emptyRange = !cursorIterable.iterator().hasNext();
                time = gran.toDateTime(interval.getStartMillis());

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
                  BaseQuery.checkInterrupted();

                  IncrementalIndex.TimeAndDims entry = baseIter.next();
                  if (beyondMaxRowIndex(entry.getRowIndex())) {
                    continue;
                  }

                  currEntry.set(entry);

                  if (filterMatcher.matches()) {
                    return;
                  }
                }

                done = true;
              }

              @Override
              public void advanceUninterruptibly()
              {
                if (!baseIter.hasNext()) {
                  done = true;
                  return;
                }

                while (baseIter.hasNext()) {
                  if (Thread.currentThread().isInterrupted()) {
                    return;
                  }

                  IncrementalIndex.TimeAndDims entry = baseIter.next();
                  if (beyondMaxRowIndex(entry.getRowIndex())) {
                    continue;
                  }

                  currEntry.set(entry);

                  if (filterMatcher.matches()) {
                    return;
                  }
                }

                done = true;
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
              public boolean isDoneOrInterrupted()
              {
                return isDone() || Thread.currentThread().isInterrupted();
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

                BaseQuery.checkInterrupted();

                boolean foundMatched = false;
                while (baseIter.hasNext()) {
                  IncrementalIndex.TimeAndDims entry = baseIter.next();
                  if (beyondMaxRowIndex(entry.getRowIndex())) {
                    numAdvanced++;
                    continue;
                  }
                  currEntry.set(entry);
                  if (filterMatcher.matches()) {
                    foundMatched = true;
                    break;
                  }

                  numAdvanced++;
                }

                done = !foundMatched && (emptyRange || !baseIter.hasNext());
              }

              private boolean beyondMaxRowIndex(int rowIndex)
              {
                // ignore rows whose rowIndex is beyond the maxRowIndex
                // rows are order by timestamp, not rowIndex,
                // so we still need to go through all rows to skip rows added after cursor created
                return rowIndex > maxRowIndex;
              }

              @Override
              public DimensionSelector makeDimensionSelector(
                  DimensionSpec dimensionSpec
              )
              {
                if (virtualColumns.exists(dimensionSpec.getDimension())) {
                  return virtualColumns.makeDimensionSelector(dimensionSpec, this);
                }

                return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
              }

              private DimensionSelector makeDimensionSelectorUndecorated(
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
                  return selector;
                }

                final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(dimensionSpec.getDimension());
                if (dimensionDesc == null) {
                  // not a dimension, column may be a metric
                  ColumnCapabilities capabilities = getColumnCapabilities(dimension);
                  if (capabilities == null) {
                    return NullDimensionSelector.instance();
                  }
                  if (capabilities.getType() == ValueType.LONG) {
                    return new LongWrappingDimensionSelector(makeLongColumnSelector(dimension), extractionFn);
                  }
                  if (capabilities.getType() == ValueType.FLOAT) {
                    return new FloatWrappingDimensionSelector(makeFloatColumnSelector(dimension), extractionFn);
                  }
                  if (capabilities.getType() == ValueType.DOUBLE) {
                    return new DoubleWrappingDimensionSelector(makeDoubleColumnSelector(dimension), extractionFn);
                  }

                  // if we can't wrap the base column, just return a column of all nulls
                  return NullDimensionSelector.instance();
                } else {
                  final DimensionIndexer indexer = dimensionDesc.getIndexer();
                  return indexer.makeDimensionSelector(dimensionSpec, currEntry, dimensionDesc);
                }
              }

              @Override
              public FloatColumnSelector makeFloatColumnSelector(String columnName)
              {
                if (virtualColumns.exists(columnName)) {
                  return virtualColumns.makeFloatColumnSelector(columnName, this);
                }

                final Integer dimIndex = index.getDimensionIndex(columnName);
                if (dimIndex != null) {
                  final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
                  final DimensionIndexer indexer = dimensionDesc.getIndexer();
                  return indexer.makeFloatColumnSelector(
                      currEntry,
                      dimensionDesc
                  );
                }

                final Integer metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt == null) {
                  return ZeroFloatColumnSelector.instance();
                }

                final int metricIndex = metricIndexInt;
                return new FloatColumnSelector()
                {
                  @Override
                  public float get()
                  {
                    return index.getMetricFloatValue(currEntry.getValue(), metricIndex);
                  }

                  @Override
                  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                  {
                    inspector.visit("index", index);
                  }
                };
              }

              @Override
              public LongColumnSelector makeLongColumnSelector(String columnName)
              {
                if (virtualColumns.exists(columnName)) {
                  return virtualColumns.makeLongColumnSelector(columnName, this);
                }

                if (columnName.equals(Column.TIME_COLUMN_NAME)) {
                  class TimeLongColumnSelector implements LongColumnSelector
                  {
                    @Override
                    public long get()
                    {
                      return currEntry.getKey().getTimestamp();
                    }

                    @Override
                    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                    {
                      // nothing to inspect
                    }
                  }
                  return new TimeLongColumnSelector();
                }

                final Integer dimIndex = index.getDimensionIndex(columnName);
                if (dimIndex != null) {
                  final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
                  final DimensionIndexer indexer = dimensionDesc.getIndexer();
                  return indexer.makeLongColumnSelector(
                      currEntry,
                      dimensionDesc
                  );
                }

                final Integer metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt == null) {
                  return ZeroLongColumnSelector.instance();
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

                  @Override
                  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                  {
                    inspector.visit("index", index);
                  }
                };
              }

              @Override
              public ObjectColumnSelector makeObjectColumnSelector(String column)
              {
                if (virtualColumns.exists(column)) {
                  return virtualColumns.makeObjectColumnSelector(column, this);
                }

                if (column.equals(Column.TIME_COLUMN_NAME)) {
                  return new ObjectColumnSelector<Long>()
                  {
                    @Override
                    public Class classOfObject()
                    {
                      return Long.class;
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

                      return indexer.convertUnsortedEncodedKeyComponentToActualArrayOrList(
                          dims[dimensionIndex], DimensionIndexer.ARRAY
                      );
                    }
                  };
                }
              }

              @Override
              public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
              {
                if (virtualColumns.exists(columnName)) {
                  return virtualColumns.makeDoubleColumnSelector(columnName, this);
                }

                final Integer dimIndex = index.getDimensionIndex(columnName);
                if (dimIndex != null) {
                  final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(columnName);
                  final DimensionIndexer indexer = dimensionDesc.getIndexer();
                  return indexer.makeDoubleColumnSelector(
                      currEntry,
                      dimensionDesc
                  );
                }

                final Integer metricIndexInt = index.getMetricIndex(columnName);
                if (metricIndexInt == null) {
                  return ZeroDoubleColumnSelector.instance();
                }

                final int metricIndex = metricIndexInt;
                return new DoubleColumnSelector()
                {
                  @Override
                  public double get()
                  {
                    return index.getMetricDoubleValue(currEntry.getValue(), metricIndex);
                  }

                  @Override
                  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                  {
                    inspector.visit("index", index);
                  }
                };
              }

              @Nullable
              @Override
              public ColumnCapabilities getColumnCapabilities(String columnName)
              {
                if (virtualColumns.exists(columnName)) {
                  return virtualColumns.getColumnCapabilities(columnName);
                }

                return index.getCapabilities(columnName);
              }
            };
          }
        }
    );
  }

  private ValueMatcher makeFilterMatcher(final Filter filter, final Cursor cursor)
  {
    return filter == null
           ? BooleanValueMatcher.of(true)
           : filter.makeMatcher(cursor);
  }

  public static class EntryHolder
  {
    IncrementalIndex.TimeAndDims currEntry = null;

    public IncrementalIndex.TimeAndDims get()
    {
      return currEntry;
    }

    public void set(IncrementalIndex.TimeAndDims currEntry)
    {
      this.currEntry = currEntry;
    }

    public IncrementalIndex.TimeAndDims getKey()
    {
      return currEntry;
    }

    public int getValue()
    {
      return currEntry.getRowIndex();
    }
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }
}
