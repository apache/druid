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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.metamx.collections.spatial.search.Bound;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.granularity.QueryGranularity;
import io.druid.query.QueryInterruptedException;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.NullDimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.SingleScanTimeDimSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 */
public class IncrementalIndexStorageAdapter implements StorageAdapter
{
  private static final Splitter SPLITTER = Splitter.on(",");
  private static final NullDimensionSelector NULL_DIMENSION_SELECTOR = new NullDimensionSelector();

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
    return new ListIndexed<String>(index.getDimensions(), String.class);
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
    IncrementalIndex.DimDim dimDim = index.getDimension(dimension);
    if (dimDim == null) {
      return 0;
    }
    return dimDim.size();
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
  public DateTime getMaxIngestedEventTime()
  {
    return index.getMaxIngestedEventTime();
  }

  @Override
  public Sequence<Cursor> makeCursors(final Filter filter, final Interval interval, final QueryGranularity gran)
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

    return Sequences.map(
        Sequences.simple(gran.iterable(actualInterval.getStartMillis(), actualInterval.getEndMillis())),
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
                        timeStart, new String[][]{}
                    ),
                    new IncrementalIndex.TimeAndDims(
                        Math.min(actualInterval.getEndMillis(), gran.next(input)), new String[][]{}
                    )
                );
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
                    throw new QueryInterruptedException();
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
                  throw new QueryInterruptedException();
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
                  final String dimension,
                  @Nullable final ExtractionFn extractionFn
              )
              {
                if (dimension.equals(Column.TIME_COLUMN_NAME)) {
                  return new SingleScanTimeDimSelector(makeLongColumnSelector(dimension), extractionFn);
                }

                final IncrementalIndex.DimDim dimValLookup = index.getDimension(dimension);
                if (dimValLookup == null) {
                  return NULL_DIMENSION_SELECTOR;
                }

                final int maxId = dimValLookup.size();
                final int dimIndex = index.getDimensionIndex(dimension);

                return new DimensionSelector()
                {
                  @Override
                  public IndexedInts getRow()
                  {
                    final ArrayList<Integer> vals = Lists.newArrayList();
                    if (dimIndex < currEntry.getKey().getDims().length) {
                      final String[] dimVals = currEntry.getKey().getDims()[dimIndex];
                      if (dimVals != null) {
                        for (String dimVal : dimVals) {
                          int id = dimValLookup.getId(dimVal);
                          if (id < maxId) {
                            vals.add(id);
                          }
                        }
                      }
                    }
                    // check for null entry
                    if (vals.isEmpty() && dimValLookup.contains(null)) {
                      int id = dimValLookup.getId(null);
                      if (id < maxId) {
                        vals.add(id);
                      }
                    }

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
                    final String value = dimValLookup.getValue(id);
                    return extractionFn == null ? value : extractionFn.apply(value);

                  }

                  @Override
                  public int lookupId(String name)
                  {
                    if (extractionFn != null) {
                      throw new UnsupportedOperationException(
                          "cannot perform lookup when applying an extraction function"
                      );
                    }
                    return dimValLookup.getId(name);
                  }
                };
              }

              @Override
              public FloatColumnSelector makeFloatColumnSelector(String columnName)
              {
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

                final Integer dimensionIndexInt = index.getDimensionIndex(column);

                if (dimensionIndexInt != null) {
                  final int dimensionIndex = dimensionIndexInt;
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

                      String[][] dims = key.getDims();
                      if (dimensionIndex >= dims.length) {
                        return null;
                      }

                      final String[] dimVals = dims[dimensionIndex];
                      if (dimVals == null || dimVals.length == 0) {
                        return null;
                      }
                      if (dimVals.length == 1) {
                        return dimVals[0];
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
    public ValueMatcher makeValueMatcher(String dimension, final String value)
    {
      Integer dimIndexObject = index.getDimensionIndex(dimension);
      if (dimIndexObject == null) {
        return new BooleanValueMatcher(Strings.isNullOrEmpty(value));
      }
      final IncrementalIndex.DimDim dimDim = index.getDimension(dimension);
      if (!dimDim.contains(value)) {
        if (Strings.isNullOrEmpty(value)) {
          final int dimIndex = dimIndexObject;

          return new ValueMatcher()
          {
            @Override
            public boolean matches()
            {
              String[][] dims = holder.getKey().getDims();
              if (dimIndex >= dims.length || dims[dimIndex] == null) {
                return true;
              }
              return false;
            }
          };
        }
        return new BooleanValueMatcher(false);
      }

      final int dimIndex = dimIndexObject;
      final String id = dimDim.get(value);

      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          String[][] dims = holder.getKey().getDims();
          if (dimIndex >= dims.length || dims[dimIndex] == null) {
            return Strings.isNullOrEmpty(value);
          }

          for (String dimVal : dims[dimIndex]) {
            if (dimDim.compareCannonicalValues(id, dimVal)) {
              return true;
            }
          }
          return false;
        }
      };
    }

    @Override
    public ValueMatcher makeValueMatcher(String dimension, final Predicate<String> predicate)
    {
      Integer dimIndexObject = index.getDimensionIndex(dimension);
      if (dimIndexObject == null) {
        return new BooleanValueMatcher(false);
      }
      final int dimIndex = dimIndexObject;

      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          String[][] dims = holder.getKey().getDims();
          if (dimIndex >= dims.length || dims[dimIndex] == null) {
            return predicate.apply(null);
          }

          for (String dimVal : dims[dimIndex]) {
            if (predicate.apply(dimVal)) {
              return true;
            }
          }
          return false;
        }
      };
    }

    @Override
    public ValueMatcher makeValueMatcher(final String dimension, final Bound bound)
    {
      Integer dimIndexObject = index.getDimensionIndex(dimension);
      if (dimIndexObject == null) {
        return new BooleanValueMatcher(false);
      }
      final int dimIndex = dimIndexObject;

      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          String[][] dims = holder.getKey().getDims();
          if (dimIndex >= dims.length || dims[dimIndex] == null) {
            return false;
          }

          for (String dimVal : dims[dimIndex]) {
            List<String> stringCoords = Lists.newArrayList(SPLITTER.split(dimVal));
            float[] coords = new float[stringCoords.size()];
            for (int j = 0; j < coords.length; j++) {
              coords[j] = Float.valueOf(stringCoords.get(j));
            }
            if (bound.contains(coords)) {
              return true;
            }
          }
          return false;
        }
      };
    }
  }
}
