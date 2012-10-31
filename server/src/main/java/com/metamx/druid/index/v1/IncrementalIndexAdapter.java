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

import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.IntSet;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import com.metamx.druid.aggregation.Aggregator;
import com.metamx.druid.kv.EmptyIndexedInts;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.kv.IndexedInts;
import com.metamx.druid.kv.IndexedIterable;
import com.metamx.druid.kv.ListIndexed;

/**
 */
public class IncrementalIndexAdapter implements IndexableAdapter
{
  private static final Logger log = new Logger(IncrementalIndexAdapter.class);

  private final Interval dataInterval;
  private final IncrementalIndex index;
  private final List<String> dimensions;
  private final List<String> metrics;

  private final Map<String, Map<String, ConciseSet>> invertedIndexes;

  public IncrementalIndexAdapter(
      Interval dataInterval, IncrementalIndex index, List<String> dimensions, List<String> metrics
  )
  {
    this.dataInterval = dataInterval;
    this.index = index;
    this.dimensions = dimensions;
    this.metrics = metrics;

    this.invertedIndexes = Maps.newHashMap();

    for (String dimension : dimensions) {
      invertedIndexes.put(dimension, Maps.<String, ConciseSet>newHashMap());
    }

    int rowNum = 0;
    for (IncrementalIndex.TimeAndDims timeAndDims : index.facts.keySet()) {
      final String[][] dims = timeAndDims.getDims();

      for (String dimension : dimensions) {
        if (index.dimensionOrder == null || invertedIndexes == null) {
          log.error("wtf, dimensionOrder and indvertedIndexes are null");
        }
        int dimIndex = index.dimensionOrder.get(dimension);
        Map<String, ConciseSet> conciseSets = invertedIndexes.get(dimension);

        if (conciseSets == null || dims == null) {
          log.error("conciseSets and dims are null!");
        }
        if (dimIndex >= dims.length || dims[dimIndex] == null) {
          continue;
        }

        for (String dimValue : dims[dimIndex]) {
          ConciseSet conciseSet = conciseSets.get(dimValue);

          if (conciseSet == null) {
            conciseSet = new ConciseSet();
            conciseSets.put(dimValue, conciseSet);
          }

          try {
            conciseSet.add(rowNum);
          }
          catch (Exception e) {
            log.info(e.toString());
          }
        }
      }

      ++rowNum;
    }
  }

  @Override
  public Interval getDataInterval()
  {
    return dataInterval;
  }

  @Override
  public int getNumRows()
  {
    return index.size();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<String>(dimensions, String.class);
  }

  @Override
  public Indexed<String> getAvailableMetrics()
  {
    return new ListIndexed<String>(metrics, String.class);
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    final IncrementalIndex.DimDim dimDim = index.dimValues.get(dimension);
    dimDim.sort();

    return new Indexed<String>()
    {
      @Override
      public Class<? extends String> getClazz()
      {
        return String.class;
      }

      @Override
      public int size()
      {
        return dimDim.size();
      }

      @Override
      public String get(int index)
      {
        return dimDim.getSortedValue(index);
      }

      @Override
      public int indexOf(String value)
      {
        return dimDim.getSortedId(value);
      }

      @Override
      public Iterator<String> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }
    };
  }

  @Override
  public Iterable<Rowboat> getRows()
  {
    return FunctionalIterable
        .create(index.facts.entrySet())
        .transform(
            new Function<Map.Entry<IncrementalIndex.TimeAndDims, Aggregator[]>, Rowboat>()
            {
              int count = 0;

              @Override
              public Rowboat apply(
                  @Nullable Map.Entry<IncrementalIndex.TimeAndDims, Aggregator[]> input
              )
              {
                final IncrementalIndex.TimeAndDims timeAndDims = input.getKey();
                final String[][] dimValues = timeAndDims.getDims();
                final Aggregator[] aggs = input.getValue();

                int[][] dims = new int[dimValues.length][];
                for (String dimension : dimensions) {
                  int dimIndex = index.dimensionOrder.get(dimension);
                  final IncrementalIndex.DimDim dimDim = index.dimValues.get(dimension);
                  dimDim.sort();

                  if (dimIndex >= dimValues.length || dimValues[dimIndex] == null) {
                    continue;
                  }

                  dims[dimIndex] = new int[dimValues[dimIndex].length];

                  if (dimIndex >= dims.length || dims[dimIndex] == null) {
                    continue;
                  }

                  for (int i = 0; i < dimValues[dimIndex].length; ++i) {
                    dims[dimIndex][i] = dimDim.getSortedId(dimValues[dimIndex][i]);
                  }
                }

                Object[] metrics = new Object[aggs.length];
                for (int i = 0; i < aggs.length; i++) {
                  metrics[i] = aggs[i].get();
                }

                return new Rowboat(
                    timeAndDims.getTimestamp(),
                    dims,
                    metrics,
                    count++
                );
              }
            }
        );
  }

  @Override
  public IndexedInts getInverteds(String dimension, String value)
  {
    Map<String, ConciseSet> dimInverted = invertedIndexes.get(dimension);

    if (dimInverted == null) {
      return new EmptyIndexedInts();
    }

    final ConciseSet conciseSet = dimInverted.get(value);

    if (conciseSet == null) {
      return new EmptyIndexedInts();
    }

    return new IndexedInts()
    {
      @Override
      public int size()
      {
        return conciseSet.size();
      }

      @Override
      public int get(int index)
      {
        throw new UnsupportedOperationException("This is really slow, so it's just not supported.");
      }

      @Override
      public Iterator<Integer> iterator()
      {
        return new Iterator<Integer>()
        {
          IntSet.IntIterator baseIter = conciseSet.iterator();

          @Override
          public boolean hasNext()
          {
            return baseIter.hasNext();
          }

          @Override
          public Integer next()
          {
            return baseIter.next();
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public String getMetricType(String metric)
  {
    return index.getMetricType(metric);
  }
}