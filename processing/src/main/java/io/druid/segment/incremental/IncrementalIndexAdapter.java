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
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;

import io.druid.segment.IndexableAdapter;
import io.druid.segment.Rowboat;
import io.druid.segment.column.BitmapIndexSeeker;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.EmptyBitmapIndexSeeker;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.ListIndexed;

import org.joda.time.Interval;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class IncrementalIndexAdapter implements IndexableAdapter
{
  private static final Logger log = new Logger(IncrementalIndexAdapter.class);
  private final Interval dataInterval;
  private final IncrementalIndex<?> index;
  private final Map<String, Map<String, MutableBitmap>> invertedIndexes;

  public IncrementalIndexAdapter(
      Interval dataInterval, IncrementalIndex<?> index, BitmapFactory bitmapFactory
  )
  {
    this.dataInterval = dataInterval;
    this.index = index;

    this.invertedIndexes = Maps.newHashMap();

    for (String dimension : index.getDimensions()) {
      invertedIndexes.put(dimension, Maps.<String, MutableBitmap>newHashMap());
    }

    int rowNum = 0;
    for (IncrementalIndex.TimeAndDims timeAndDims : index.getFacts().keySet()) {
      final String[][] dims = timeAndDims.getDims();

      for (String dimension : index.getDimensions()) {
        int dimIndex = index.getDimensionIndex(dimension);
        Map<String, MutableBitmap> bitmapIndexes = invertedIndexes.get(dimension);

        if (bitmapIndexes == null || dims == null) {
          log.error("bitmapIndexes and dims are null!");
          continue;
        }
        if (dimIndex >= dims.length || dims[dimIndex] == null) {
          continue;
        }

        for (String dimValue : dims[dimIndex]) {
          MutableBitmap mutableBitmap = bitmapIndexes.get(dimValue);

          if (mutableBitmap == null) {
            mutableBitmap = bitmapFactory.makeEmptyMutableBitmap();
            bitmapIndexes.put(dimValue, mutableBitmap);
          }

          try {
            mutableBitmap.add(rowNum);
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
  public Indexed<String> getDimensionNames()
  {
    return new ListIndexed<String>(index.getDimensions(), String.class);
  }

  @Override
  public Indexed<String> getMetricNames()
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
            new Function<Map.Entry<IncrementalIndex.TimeAndDims, Integer>, Rowboat>()
            {
              int count = 0;

              @Override
              public Rowboat apply(
                  @Nullable Map.Entry<IncrementalIndex.TimeAndDims, Integer> input
              )
              {
                final IncrementalIndex.TimeAndDims timeAndDims = input.getKey();
                final String[][] dimValues = timeAndDims.getDims();
                final int rowOffset = input.getValue();

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

                Object[] metrics = new Object[index.getMetricAggs().length];
                for (int i = 0; i < metrics.length; i++) {
                  metrics[i] = index.getMetricObjectValue(rowOffset, i);
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
  public IndexedInts getBitmapIndex(String dimension, String value)
  {
    Map<String, MutableBitmap> dimInverted = invertedIndexes.get(dimension);

    if (dimInverted == null) {
      return new EmptyIndexedInts();
    }

    final MutableBitmap bitmapIndex = dimInverted.get(value);

    if (bitmapIndex == null) {
      return new EmptyIndexedInts();
    }

    return new BitmapIndexedInts(bitmapIndex);
  }

  @Override
  public String getMetricType(String metric)
  {
    return index.getMetricType(metric);
  }

  @Override
  public ColumnCapabilities getCapabilities(String column)
  {
    return index.getCapabilities(column);
  }

  @Override
  public BitmapIndexSeeker getBitmapIndexSeeker(String dimension)
  {
    final Map<String, MutableBitmap> dimInverted = invertedIndexes.get(dimension);
    if (dimInverted == null) {
      return new EmptyBitmapIndexSeeker();
    }

    return new BitmapIndexSeeker()
    {
      private String lastVal = null;

      @Override
      public IndexedInts seek(String value)
      {
        if (value != null && GenericIndexed.STRING_STRATEGY.compare(value, lastVal) <= 0)  {
          throw new ISE("Value[%s] is less than the last value[%s] I have, cannot be.",
              value, lastVal);
        }
        lastVal = value;
        final MutableBitmap bitmapIndex = dimInverted.get(value);
        if (bitmapIndex == null) {
          return new EmptyIndexedInts();
        }
        return new BitmapIndexedInts(bitmapIndex);
      }
    };
  }

  static class BitmapIndexedInts implements IndexedInts {

    private final MutableBitmap bitmapIndex;

    BitmapIndexedInts(MutableBitmap bitmapIndex)
    {
      this.bitmapIndex = bitmapIndex;
    }

    @Override
    public int size()
    {
      return bitmapIndex.size();
    }

    @Override
    public int get(int index)
    {
      // Slow for concise bitmaps, but is fast with roaring bitmaps, so it's just not supported.
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Iterator<Integer> iterator()
    {
      return new Iterator<Integer>()
      {
        final IntIterator baseIter = bitmapIndex.iterator();

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

    @Override
    public void fill(int index, int[] toFill)
    {
      throw new UnsupportedOperationException("fill not supported");
    }

    @Override
    public void close() throws IOException
    {

    }
  }
}
