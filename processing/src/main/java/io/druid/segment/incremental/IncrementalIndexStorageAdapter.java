
/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.metamx.collections.spatial.search.Bound;
import com.metamx.common.guava.FunctionalIterator;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.TimestampColumnSelector;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
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
    IncrementalIndex.DimDim dimDim = index.getDimension(dimension.toLowerCase());
    if (dimDim == null) {
      return 0;
    }
    return dimDim.size();
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
  public Iterable<Cursor> makeCursors(final Filter filter, final Interval interval, final QueryGranularity gran)
  {
    if (index.isEmpty()) {
      return ImmutableList.of();
    }

    Interval actualIntervalTmp = interval;


    final Interval dataInterval = new Interval(getMinTime().getMillis(), gran.next(getMaxTime().getMillis()));
    if (!actualIntervalTmp.overlaps(dataInterval)) {
      return ImmutableList.of();
    }

    if (actualIntervalTmp.getStart().isBefore(dataInterval.getStart())) {
      actualIntervalTmp = actualIntervalTmp.withStart(dataInterval.getStart());
    }
    if (actualIntervalTmp.getEnd().isAfter(dataInterval.getEnd())) {
      actualIntervalTmp = actualIntervalTmp.withEnd(dataInterval.getEnd());
    }

    final Interval actualInterval = actualIntervalTmp;

    return new Iterable<Cursor>()
    {
      @Override
      public Iterator<Cursor> iterator()
      {
        return FunctionalIterator
            .create(gran.iterable(actualInterval.getStartMillis(), actualInterval.getEndMillis()).iterator())
            .transform(
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
                      private Iterator<Map.Entry<IncrementalIndex.TimeAndDims, Aggregator[]>> baseIter;
                      private ConcurrentNavigableMap<IncrementalIndex.TimeAndDims, Aggregator[]> cursorMap;
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
                          while (baseIter.hasNext()) {
                            currEntry.set(baseIter.next());
                            if (filterMatcher.matches()) {
                              return;
                            }

                            numAdvanced++;
                          }
                        } else {
                          Iterators.advance(baseIter, numAdvanced);
                          if (baseIter.hasNext()) {
                            currEntry.set(baseIter.next());
                          }
                        }

                        done = cursorMap.size() == 0 || !baseIter.hasNext();

                      }

                      @Override
                      public TimestampColumnSelector makeTimestampColumnSelector()
                      {
                        return new TimestampColumnSelector()
                        {
                          @Override
                          public long getTimestamp()
                          {
                            return currEntry.getKey().getTimestamp();
                          }
                        };
                      }

                      @Override
                      public DimensionSelector makeDimensionSelector(String dimension)
                      {
                        final String dimensionName = dimension.toLowerCase();
                        final IncrementalIndex.DimDim dimValLookup = index.getDimension(dimensionName);
                        if (dimValLookup == null) {
                          return null;
                        }

                        final int maxId = dimValLookup.size();
                        final int dimIndex = index.getDimensionIndex(dimensionName);

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
                            return dimValLookup.getValue(id);
                          }

                          @Override
                          public int lookupId(String name)
                          {
                            return dimValLookup.getId(name);
                          }
                        };
                      }

                      @Override
                      public FloatColumnSelector makeFloatColumnSelector(String columnName)
                      {
                        final String metricName = columnName.toLowerCase();
                        final Integer metricIndexInt = index.getMetricIndex(metricName);
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
                            return currEntry.getValue()[metricIndex].getFloat();
                          }
                        };
                      }

                      @Override
                      public ObjectColumnSelector makeObjectColumnSelector(String column)
                      {
                        final String columnName = column.toLowerCase();
                        final Integer metricIndexInt = index.getMetricIndex(columnName);

                        if (metricIndexInt != null) {
                          final int metricIndex = metricIndexInt;

                          final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(index.getMetricType(columnName));

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
                              return currEntry.getValue()[metricIndex].get();
                            }
                          };
                        }

                        final Integer dimensionIndexInt = index.getDimensionIndex(columnName);

                        if (dimensionIndexInt != null) {
                          final int dimensionIndex = dimensionIndexInt;
                          return new ObjectColumnSelector<String>()
                          {
                            @Override
                            public Class classOfObject()
                            {
                              return String.class;
                            }

                            @Override
                            public String get()
                            {
                              final String[] dimVals = currEntry.getKey().getDims()[dimensionIndex];
                              if (dimVals.length == 1) {
                                return dimVals[0];
                              }
                              if (dimVals.length == 0) {
                                return null;
                              }
                              throw new UnsupportedOperationException(
                                  "makeObjectColumnSelector does not support multivalued columns"
                              );
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
    };
  }

  private ValueMatcher makeFilterMatcher(final Filter filter, final EntryHolder holder)
  {
    return filter == null
           ? new BooleanValueMatcher(true)
           : filter.makeMatcher(new EntryHolderValueMatcherFactory(holder));
  }

  private static class EntryHolder
  {
    Map.Entry<IncrementalIndex.TimeAndDims, Aggregator[]> currEntry = null;

    public Map.Entry<IncrementalIndex.TimeAndDims, Aggregator[]> get()
    {
      return currEntry;
    }

    public void set(Map.Entry<IncrementalIndex.TimeAndDims, Aggregator[]> currEntry)
    {
      this.currEntry = currEntry;
      this.currEntry = currEntry;
    }

    public IncrementalIndex.TimeAndDims getKey()
    {
      return currEntry.getKey();
    }

    public Aggregator[] getValue()
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
    public ValueMatcher makeValueMatcher(String dimension, String value)
    {
      Integer dimIndexObject = index.getDimensionIndex(dimension.toLowerCase());
      if (dimIndexObject == null) {
        return new BooleanValueMatcher(false);
      }
      String idObject = index.getDimension(dimension.toLowerCase()).get(value);
      if (idObject == null) {
        if (value == null || "".equals(value)) {
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
      final String id = idObject;

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
            if (id == dimVal) {
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
      Integer dimIndexObject = index.getDimensionIndex(dimension.toLowerCase());
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
      if (!dimension.endsWith(".geo")) {
        return new BooleanValueMatcher(false);
      }

      Integer dimIndexObject = index.getDimensionIndex(dimension.toLowerCase());
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
