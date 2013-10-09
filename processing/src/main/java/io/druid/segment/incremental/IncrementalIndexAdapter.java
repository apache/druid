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
import com.google.common.collect.Maps;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.IndexableAdapter;
import io.druid.segment.Rowboat;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.ListIndexed;
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.IntSet;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class IncrementalIndexAdapter implements IndexableAdapter
{
  private static final Logger log = new Logger(IncrementalIndexAdapter.class);

  private final Interval dataInterval;
  private final IncrementalIndex index;

  private final Map<String, Map<String, ConciseSet>> invertedIndexes;

  public IncrementalIndexAdapter(
      Interval dataInterval, IncrementalIndex index
  )
  {
    this.dataInterval = dataInterval;
    this.index = index;

    this.invertedIndexes = Maps.newHashMap();

    for (String dimension : index.getDimensions()) {
      invertedIndexes.put(dimension, Maps.<String, ConciseSet>newHashMap());
    }

    int rowNum = 0;
    for (IncrementalIndex.TimeAndDims timeAndDims : index.getFacts().keySet()) {
      final String[][] dims = timeAndDims.getDims();

      for (String dimension : index.getDimensions()) {
        int dimIndex = index.getDimensionIndex(dimension);
        Map<String, ConciseSet> conciseSets = invertedIndexes.get(dimension);

        if (conciseSets == null || dims == null) {
          log.error("conciseSets and dims are null!");
          continue;
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
    return new ListIndexed<String>(index.getDimensions(), String.class);
  }

  @Override
  public Indexed<String> getAvailableMetrics()
  {
    return new ListIndexed<String>(index.getMetricNames(), String.class);
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    final IncrementalIndex.DimDim dimDim = index.getDimension(dimension);
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
        .create(index.getFacts().entrySet())
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
                for (String dimension : index.getDimensions()) {
                  int dimIndex = index.getDimensionIndex(dimension);
                  final IncrementalIndex.DimDim dimDim = index.getDimension(dimension);
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

                Map<String, String> description = Maps.newHashMap();
                for (SpatialDimensionSchema spatialDimensionSchema : index.getSpatialDimensions()) {
                  description.put(spatialDimensionSchema.getDimName(), "spatial");
                }
                return new Rowboat(
                    timeAndDims.getTimestamp(),
                    dims,
                    metrics,
                    count++,
                    description
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
