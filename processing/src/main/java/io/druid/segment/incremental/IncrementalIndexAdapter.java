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
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionIndexer;
import io.druid.segment.IndexableAdapter;
import io.druid.segment.IntIteratorUtils;
import io.druid.segment.Metadata;
import io.druid.segment.Rowboat;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ListIndexed;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class IncrementalIndexAdapter implements IndexableAdapter
{
  private static final Logger log = new Logger(IncrementalIndexAdapter.class);
  private final Interval dataInterval;
  private final IncrementalIndex<?> index;
  private final Map<String, DimensionAccessor> accessors;

  private static class DimensionAccessor
  {
    private final IncrementalIndex.DimensionDesc dimensionDesc;
    private final MutableBitmap[] invertedIndexes;
    private final DimensionIndexer indexer;

    public DimensionAccessor(IncrementalIndex.DimensionDesc dimensionDesc)
    {
      this.dimensionDesc = dimensionDesc;
      this.indexer = dimensionDesc.getIndexer();
      if(dimensionDesc.getCapabilities().hasBitmapIndexes()) {
        this.invertedIndexes = new MutableBitmap[indexer.getCardinality() + 1];
      } else {
        this.invertedIndexes = null;
      }
    }
  }

  public IncrementalIndexAdapter(
      Interval dataInterval, IncrementalIndex<?> index, BitmapFactory bitmapFactory
  )
  {
    this.dataInterval = dataInterval;
    this.index = index;

    /* Sometimes it's hard to tell whether one dimension contains a null value or not.
     * If one dimension had show a null or empty value explicitly, then yes, it contains
     * null value. But if one dimension's values are all non-null, it still early to say
     * this dimension does not contain null value. Consider a two row case, first row had
     * "dimA=1" and "dimB=2", the second row only had "dimA=3". To dimB, its value are "2" and
     * never showed a null or empty value. But when we combines these two rows, dimB is null
     * in row 2. So we should iterate all rows to determine whether one dimension contains
     * a null value.
     */
    final List<IncrementalIndex.DimensionDesc> dimensions = index.getDimensions();

    accessors = Maps.newHashMapWithExpectedSize(dimensions.size());
    for (IncrementalIndex.DimensionDesc dimension : dimensions) {
      accessors.put(dimension.getName(), new DimensionAccessor(dimension));
    }

    int rowNum = 0;
    for (IncrementalIndex.TimeAndDims timeAndDims : index.getFacts().keySet()) {
      final Object[] dims = timeAndDims.getDims();

      for (IncrementalIndex.DimensionDesc dimension : dimensions) {
        final int dimIndex = dimension.getIndex();
        DimensionAccessor accessor = accessors.get(dimension.getName());

        // Add 'null' to the dimension's dictionary.
        if (dimIndex >= dims.length || dims[dimIndex] == null) {
          accessor.indexer.processRowValsToUnsortedEncodedKeyComponent(null);
          continue;
        }
        final ColumnCapabilities capabilities = dimension.getCapabilities();

        if(capabilities.hasBitmapIndexes()) {
          final MutableBitmap[] bitmapIndexes = accessor.invertedIndexes;
          final DimensionIndexer indexer = accessor.indexer;
          indexer.fillBitmapsFromUnsortedEncodedKeyComponent(dims[dimIndex], rowNum, bitmapIndexes, bitmapFactory);
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
    return new ListIndexed<String>(index.getDimensionNames(), String.class);
  }

  @Override
  public Indexed<String> getMetricNames()
  {
    return new ListIndexed<String>(index.getMetricNames(), String.class);
  }

  @Override
  public Indexed<Comparable> getDimValueLookup(String dimension)
  {
    final DimensionAccessor accessor = accessors.get(dimension);
    if (accessor == null) {
      return null;
    }

    final DimensionIndexer indexer = accessor.dimensionDesc.getIndexer();

    return indexer.getSortedIndexedValues();
  }

  @Override
  public Iterable<Rowboat> getRows()
  {
    return new Iterable<Rowboat>()
    {
      @Override
      public Iterator<Rowboat> iterator()
      {
        final List<IncrementalIndex.DimensionDesc> dimensions = index.getDimensions();
        final DimensionHandler[] handlers = new DimensionHandler[dimensions.size()];
        final DimensionIndexer[] indexers = new DimensionIndexer[dimensions.size()];
        for (IncrementalIndex.DimensionDesc dimension : dimensions) {
          handlers[dimension.getIndex()] = dimension.getHandler();
          indexers[dimension.getIndex()] = dimension.getIndexer();
        }

        /*
         * Note that the transform function increments a counter to determine the rowNum of
         * the iterated Rowboats. We need to return a new iterator on each
         * iterator() call to ensure the counter starts at 0.
         */
        return Iterators.transform(
            index.getFacts().keySet().iterator(),
            new Function<IncrementalIndex.TimeAndDims, Rowboat>()
            {
              int count = 0;

              @Override
              public Rowboat apply(IncrementalIndex.TimeAndDims timeAndDims)
              {
                final Object[] dimValues = timeAndDims.getDims();
                final int rowOffset = timeAndDims.getRowIndex();

                Object[] dims = new Object[dimValues.length];
                for (IncrementalIndex.DimensionDesc dimension : dimensions) {
                  final int dimIndex = dimension.getIndex();

                  if (dimIndex >= dimValues.length || dimValues[dimIndex] == null) {
                    continue;
                  }

                  final DimensionIndexer indexer = indexers[dimIndex];
                  Object sortedDimVals = indexer.convertUnsortedEncodedKeyComponentToSortedEncodedKeyComponent(dimValues[dimIndex]);
                  dims[dimIndex] = sortedDimVals;
                }

                Object[] metrics = new Object[index.getMetricAggs().length];
                for (int i = 0; i < metrics.length; i++) {
                  metrics[i] = index.getMetricObjectValue(rowOffset, i);
                }

                return new Rowboat(
                    timeAndDims.getTimestamp(),
                    dims,
                    metrics,
                    count++,
                    handlers
                );
              }
            }
        );
      }
    };
  }

  @Override
  public IndexedInts getBitmapIndex(String dimension, int index)
  {
    DimensionAccessor accessor = accessors.get(dimension);
    if (accessor == null) {
      return EmptyIndexedInts.EMPTY_INDEXED_INTS;
    }
    ColumnCapabilities capabilities = accessor.dimensionDesc.getCapabilities();
    DimensionIndexer indexer = accessor.dimensionDesc.getIndexer();

    if (!capabilities.hasBitmapIndexes()) {
      return EmptyIndexedInts.EMPTY_INDEXED_INTS;
    }

    final int id = (Integer) indexer.getUnsortedEncodedValueFromSorted(index);
    if (id < 0 || id >= indexer.getCardinality()) {
      return EmptyIndexedInts.EMPTY_INDEXED_INTS;
    }

    MutableBitmap bitmapIndex = accessor.invertedIndexes[id];

    if (bitmapIndex == null) {
      return EmptyIndexedInts.EMPTY_INDEXED_INTS;
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

  static class BitmapIndexedInts implements IndexedInts
  {

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
    public IntIterator iterator()
    {
      return IntIteratorUtils.fromRoaringBitmapIntIterator(bitmapIndex.iterator());
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

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("bitmapIndex", bitmapIndex);
    }
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return index.getDimensionHandlers();
  }
}
