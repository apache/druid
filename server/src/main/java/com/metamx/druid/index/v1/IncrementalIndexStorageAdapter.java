/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.v1;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.FunctionalIterator;
import com.metamx.druid.Capabilities;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.aggregation.Aggregator;
import com.metamx.druid.index.brita.BooleanValueMatcher;
import com.metamx.druid.index.brita.Filter;
import com.metamx.druid.index.brita.ValueMatcher;
import com.metamx.druid.index.brita.ValueMatcherFactory;
import com.metamx.druid.index.v1.processing.Cursor;
import com.metamx.druid.index.v1.processing.DimensionSelector;
import com.metamx.druid.index.v1.serde.ComplexMetricSerde;
import com.metamx.druid.index.v1.serde.ComplexMetrics;
import com.metamx.druid.kv.IndexedInts;
import com.metamx.druid.processing.ComplexMetricSelector;
import com.metamx.druid.processing.FloatMetricSelector;
import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.query.search.SearchQuery;
import com.metamx.druid.query.search.SearchQuerySpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 */
public class IncrementalIndexStorageAdapter implements StorageAdapter
{
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
    Interval actualIntervalTmp = interval;
    Interval dataInterval = getInterval();
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
                                Math.min(actualInterval.getEndMillis(), gran.next(timeStart)), new String[][]{}
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
                          Iterators.skip(baseIter, numAdvanced);
                          if (baseIter.hasNext()) {
                            currEntry.set(baseIter.next());
                          }
                        }

                        done = cursorMap.size() == 0 || !baseIter.hasNext();

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
                            return dimValLookup.size();
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
                      public FloatMetricSelector makeFloatMetricSelector(String metric)
                      {
                        final String metricName = metric.toLowerCase();
                        final Integer metricIndexInt = index.getMetricIndex(metricName);
                        if (metricIndexInt == null) {
                          return new FloatMetricSelector()
                          {
                            @Override
                            public float get()
                            {
                              return 0.0f;
                            }
                          };
                        }

                        final int metricIndex = metricIndexInt;

                        return new FloatMetricSelector()
                        {
                          @Override
                          public float get()
                          {
                            return currEntry.getValue()[metricIndex].getFloat();
                          }
                        };
                      }

                      @Override
                      public ComplexMetricSelector makeComplexMetricSelector(String metric)
                      {
                        final String metricName = metric.toLowerCase();
                        final Integer metricIndexInt = index.getMetricIndex(metricName);
                        if (metricIndexInt == null) {
                          return null;
                        }

                        final int metricIndex = metricIndexInt;

                        final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(index.getMetricType(metricName));

                        return new ComplexMetricSelector()
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
                    };
                  }
                }
            );
      }
    };
  }

  @Override
  public Iterable<SearchHit> searchDimensions(final SearchQuery query, final Filter filter)
  {
    final List<String> dimensions = query.getDimensions();
    final int[] dimensionIndexes;
    final String[] dimensionNames;
    final List<String> dimensionOrder = index.getDimensions();
    if (dimensions == null || dimensions.isEmpty()) {
      dimensionIndexes = new int[dimensionOrder.size()];
      dimensionNames = new String[dimensionIndexes.length];

      Iterator<String> dimensionOrderIter = dimensionOrder.iterator();
      for (int i = 0; i < dimensionIndexes.length; ++i) {
        dimensionNames[i] = dimensionOrderIter.next();
        dimensionIndexes[i] = index.getDimensionIndex(dimensionNames[i]);
      }
    } else {
      int[] tmpDimensionIndexes = new int[dimensions.size()];
      String[] tmpDimensionNames = new String[dimensions.size()];
      int i = 0;
      for (String dimension : dimensions) {
        Integer dimIndex = index.getDimensionIndex(dimension.toLowerCase());
        if (dimIndex != null) {
          tmpDimensionNames[i] = dimension;
          tmpDimensionIndexes[i] = dimIndex;
          ++i;
        }
      }

      if (i != tmpDimensionIndexes.length) {
        dimensionIndexes = new int[i];
        dimensionNames = new String[i];
        System.arraycopy(tmpDimensionIndexes, 0, dimensionIndexes, 0, i);
        System.arraycopy(tmpDimensionNames, 0, dimensionNames, 0, i);
      } else {
        dimensionIndexes = tmpDimensionIndexes;
        dimensionNames = tmpDimensionNames;
      }
    }

    final List<Interval> queryIntervals = query.getIntervals();
    if (queryIntervals.size() != 1) {
      throw new IAE("Can only handle one interval, got query[%s]", query);
    }

    final Interval queryInterval = queryIntervals.get(0);
    final long intervalStart = queryInterval.getStartMillis();
    final long intervalEnd = queryInterval.getEndMillis();

    final EntryHolder holder = new EntryHolder();
    final ValueMatcher theMatcher = makeFilterMatcher(filter, holder);
    final SearchQuerySpec searchQuerySpec = query.getQuery();
    final TreeSet<SearchHit> retVal = Sets.newTreeSet(query.getSort().getComparator());

    ConcurrentNavigableMap<IncrementalIndex.TimeAndDims, Aggregator[]> facts = index.getSubMap(
        new IncrementalIndex.TimeAndDims(intervalStart, new String[][]{}),
        new IncrementalIndex.TimeAndDims(intervalEnd, new String[][]{})
    );

    for (Map.Entry<IncrementalIndex.TimeAndDims, Aggregator[]> entry : facts.entrySet()) {
      holder.set(entry);
      final IncrementalIndex.TimeAndDims key = holder.getKey();
      final long timestamp = key.getTimestamp();

      if (timestamp >= intervalStart && timestamp < intervalEnd && theMatcher.matches()) {
        final String[][] dims = key.getDims();

        for (int i = 0; i < dimensionIndexes.length; ++i) {
          if (dimensionIndexes[i] < dims.length) {
            final String[] dimVals = dims[dimensionIndexes[i]];
            if (dimVals != null) {
              for (int j = 0; j < dimVals.length; ++j) {
                if (searchQuerySpec.accept(dimVals[j])) {
                  retVal.add(new SearchHit(dimensionNames[i], dimVals[j]));
                }
              }
            }
          }
        }
      }
    }

    return new FunctionalIterable<SearchHit>(retVal).limit(query.getLimit());
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
  }
}
