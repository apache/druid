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
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.metamx.collections.spatial.search.Bound;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.granularity.QueryGranularity;
import io.druid.query.QueryInterruptedException;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
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
import io.druid.segment.StringDimensionHandler;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedLongs;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 */
public class IncrementalIndexStorageAdapter implements StorageAdapter
{
  private static final Splitter SPLITTER = Splitter.on(",");
  private static final NullDimensionSelector NULL_DIMENSION_SELECTOR = new NullDimensionSelector(ValueType.STRING);

  private final IncrementalIndex index;

  public IncrementalIndexStorageAdapter(
      IncrementalIndex index
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
          private final ValueMatcher filterMatcher;

          {
            filterMatcher = makeFilterMatcher(filter, currEntry);
          }

          @Override
          public Cursor apply(@Nullable final Long input)
          {
            final long timeStart = Math.max(input, actualInterval.getStartMillis());

            return new Cursor()
            {
              private Iterator<Map.Entry<IncrementalIndex.TimeAndDims, Integer>> baseIter;
              private ConcurrentNavigableMap<IncrementalIndex.TimeAndDims, Integer> cursorMap;
              final DateTime time;
              int numAdvanced = -1;
              boolean done;

              {
                cursorMap = index.getSubMap(
                    new IncrementalIndex.TimeAndDims(
                        timeStart, new Comparable[][]{}, null
                    ),
                    new IncrementalIndex.TimeAndDims(
                        Math.min(actualInterval.getEndMillis(), gran.next(input)), new Comparable[][]{}, null
                    )
                );
                if (descending) {
                  cursorMap = cursorMap.descendingMap();
                }
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
                baseIter = cursorMap.entrySet().iterator();

                if (numAdvanced == -1) {
                  numAdvanced = 0;
                } else {
                  Iterators.advance(baseIter, numAdvanced);
                }

                if (Thread.interrupted()) {
                  throw new QueryInterruptedException( new InterruptedException());
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

                done = !foundMatched && (cursorMap.size() == 0 || !baseIter.hasNext());
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
                DimensionHandler handler = dimensionDesc.getHandler();
                return dimensionSpec.decorate(handler.getDimensionSelector(
                    this,
                    dimensionSpec,
                    index.getCapabilities(dimensionSpec.getDimension())
                ));
              }

              @Override
              public DimensionSelector makeDictEncodedStringDimensionSelector(
                  DimensionSpec dimensionSpec
              )
              {
                final String dimension = dimensionSpec.getDimension();
                final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();
                final ColumnCapabilities capabilities = index.getCapabilities(dimension);
                final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(dimension);

                final int dimIndex = dimensionDesc.getIndex();
                final StringDimensionHandler.StringDimensionIndexer indexer = (StringDimensionHandler.StringDimensionIndexer) dimensionDesc.getIndexer();
                final int maxId = indexer.getCardinality();

                return new DimensionSelector()
                {
                  @Override
                  public IndexedInts getRow()
                  {
                    final Comparable[][] dims = currEntry.getKey().getDims();

                    int[] indices;
                    if (dimIndex < dims.length) {
                      indices = IncrementalIndex.castComparablesAsInts(dims[dimIndex]);
                    } else {
                      indices = null;
                    }

                    int nullId = indexer.getEncodedValue(null, false);
                    List<Integer> valsTmp = null;
                    if ((indices == null || indices.length == 0) && nullId > -1) {
                      if (nullId < maxId) {
                        valsTmp = new ArrayList<>(1);
                        valsTmp.add(nullId);
                      }
                    } else if (indices != null && indices.length > 0) {
                      valsTmp = new ArrayList<>(indices.length);
                      for (int i = 0; i < indices.length; i++) {
                        int id = indices[i];
                        if (id < maxId) {
                          valsTmp.add(id);
                        }
                      }
                    }

                    final List<Integer> vals = valsTmp == null ? Collections.EMPTY_LIST : valsTmp;
                    return new IndexedInts()
                    {
                      @Override
                      public int size()
                      {
                        return vals.size();
                      }

                      @Override
                      public int get(int index)
                      {
                        return vals.get(index);
                      }

                      @Override
                      public Iterator<Integer> iterator()
                      {
                        return vals.iterator();
                      }

                      @Override
                      public void fill(int index, int[] toFill)
                      {
                        throw new UnsupportedOperationException("fill not supported");
                      }

                      @Override
                      public void close() throws IOException
                      {

                      }
                    };
                  }

                  @Override
                  public int getValueCardinality()
                  {
                    return maxId;
                  }

                  @Override
                  public String lookupName(int id)
                  {
                    final Comparable value = indexer.getActualValue(id, false);
                    final String strValue = value == null ? null : value.toString();
                    return extractionFn == null ? strValue : extractionFn.apply(strValue);

                  }

                  @Override
                  public int lookupId(String name)
                  {
                    if (extractionFn != null) {
                      throw new UnsupportedOperationException(
                          "cannot perform lookup when applying an extraction function"
                      );
                    }
                    return indexer.getEncodedValue(name, false);
                  }

                  @Override
                  public IndexedLongs getLongRow()
                  {
                    throw new UnsupportedOperationException("getLongRow() is not supported.");
                  }

                  @Override
                  public Comparable getExtractedValueLong(long val)
                  {
                    throw new UnsupportedOperationException("getExtractedValueLong() is not supported.");
                  }

                  @Override
                  public IndexedFloats getFloatRow()
                  {
                    throw new UnsupportedOperationException("getFloatRow() is not supported.");
                  }

                  @Override
                  public Comparable getExtractedValueFloat(float val)
                  {
                    throw new UnsupportedOperationException("getExtractedValueFloat() is not supported.");
                  }

                  @Override
                  public Comparable getComparableRow()
                  {
                    throw new UnsupportedOperationException("getComparableRow() is not supported.");
                  }

                  @Override
                  public Comparable getExtractedValueComparable(Comparable val)
                  {
                    throw new UnsupportedOperationException("getExtractedValueComparable() is not supported.");

                  }

                  @Override
                  public ColumnCapabilities getDimCapabilities()
                  {
                    return capabilities;
                  }
                };
              }

              @Override
              public FloatColumnSelector makeFloatColumnSelector(String columnName)
              {
                final Integer dimIndex = index.getDimensionIndex(columnName);
                if (dimIndex != null) {
                  return new FloatColumnSelector()
                  {
                    @Override
                    public float get()
                    {
                      Comparable[][] rowVals = currEntry.getKey().getDims();
                      Comparable[] dimVals = rowVals[dimIndex];
                      return dimVals == null ? 0L : (Float) dimVals[0];
                    }
                  };
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
                  return new LongColumnSelector()
                  {
                    @Override
                    public long get()
                    {
                      Comparable[][] rowVals = currEntry.getKey().getDims();
                      Comparable[] dimVals = rowVals[dimIndex];
                      return dimVals == null ? 0L : (Long) dimVals[0];
                    }
                  };
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

                  final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(index.getMetricType(column));
                  return new ObjectColumnSelector()
                  {
                    @Override
                    public Class classOfObject()
                    {
                      return serde.getObjectStrategy().getClazz();
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

                if (dimensionDesc != null) {

                  final int dimensionIndex = dimensionDesc.getIndex();
                  final StringDimensionHandler.StringDimensionIndexer indexer = (StringDimensionHandler.StringDimensionIndexer) dimensionDesc.getIndexer();
                  final ColumnCapabilities capabilities = dimensionDesc.getCapabilities();

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

                      Comparable[][] dims = key.getDims();
                      if (dimensionIndex >= dims.length) {
                        return null;
                      }

                      final Comparable[] dimIdx = dims[dimensionIndex];
                      if (dimIdx == null || dimIdx.length == 0) {
                        return null;
                      }
                      if (dimIdx.length == 1) {
                        return indexer.getActualValue((Integer) dimIdx[0], false);
                      }
                      Comparable[] dimVals = new String[dimIdx.length];
                      for (int i = 0; i < dimIdx.length; i++) {
                        dimVals[i] = indexer.getActualValue((Integer) dimIdx[i], false);
                      }
                      return dimVals;
                    }
                  };
                }

                return null;
              }
            };
          }
        }
    );
  }

  private boolean isComparableNullOrEmpty(final Comparable value)
  {
    if (value instanceof String) {
      return Strings.isNullOrEmpty((String) value);
    }
    return value == null;
  }

  private ValueMatcher makeFilterMatcher(final Filter filter, final EntryHolder holder)
  {
    return filter == null
           ? new BooleanValueMatcher(true)
           : filter.makeMatcher(new EntryHolderValueMatcherFactory(holder));
  }

  private static class EntryHolder
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

  private class EntryHolderValueMatcherFactory implements ValueMatcherFactory
  {
    private final EntryHolder holder;

    public EntryHolderValueMatcherFactory(
        EntryHolder holder
    )
    {
      this.holder = holder;
    }

    @Override
    public ValueMatcher makeValueMatcher(String dimension, final Comparable originalValue)
    {
      IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(dimension);
      if (dimensionDesc == null) {
        return new BooleanValueMatcher(isComparableNullOrEmpty(originalValue));
      }

      final DimensionHandler handler = dimensionDesc.getHandler();
      final DimensionIndexer indexer = dimensionDesc.getIndexer();

      final int dimIndex = dimensionDesc.getIndex();
      final ColumnCapabilities capabilities = index.getCapabilities(dimension);
      final Comparable value = (Comparable) handler.getValueTypeTransformer().apply(originalValue);
      final Comparator encodedComparator = handler.getEncodedComparator();

      if (!capabilities.isDictionaryEncoded()) {
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            Comparable[][] dims = holder.getKey().getDims();
            Comparable[] dimVals = dims[dimIndex];
            if (dimIndex >= dims.length || dimVals == null) {
              return value == null;
            }

            for (Comparable dimVal : dimVals) {
              if (encodedComparator.compare(dimVal, value) == 0) {
                return true;
              }
            }

            return false;
          }
        };
      }

      final Comparable encodedVal = indexer.getEncodedValue(value, false);
      if (encodedVal == null) {
        if (isComparableNullOrEmpty(value)) {
          return new ValueMatcher()
          {
            @Override
            public boolean matches()
            {
              Comparable[][] dims = holder.getKey().getDims();
              if (dimIndex >= dims.length || dims[dimIndex] == null) {
                return true;
              }
              return false;
            }
          };
        }
        return new BooleanValueMatcher(false);
      }

      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          Comparable[][] dims = holder.getKey().getDims();
          if (dimIndex >= dims.length || dims[dimIndex] == null) {
            return isComparableNullOrEmpty(value);
          }
          for (Comparable dimVal : dims[dimIndex]) {
            if (encodedComparator.compare(dimVal, encodedVal) == 0) {
              return true;
            }
          }
          return false;
        }
      };
    }

    @Override
    public ValueMatcher makeValueMatcher(String dimension, final Predicate predicate)
    {
      IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(dimension);
      if (dimensionDesc == null) {
        return new BooleanValueMatcher(predicate.apply(null));
      }

      final DimensionHandler handler = dimensionDesc.getHandler();
      final DimensionIndexer indexer = dimensionDesc.getIndexer();

      final int dimIndex = dimensionDesc.getIndex();
      final ColumnCapabilities capabilities = dimensionDesc.getCapabilities();

      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          Comparable[][] dims = holder.getKey().getDims();
          if (dimIndex >= dims.length || dims[dimIndex] == null) {
            return predicate.apply(null);
          }

          for (Comparable dimVal : dims[dimIndex]) {
            Comparable finalDimVal = indexer.getActualValue(dimVal, false);
            if (predicate.apply(finalDimVal)) {
              return true;
            }
          }
          return false;
        }
      };
    }
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }
}
