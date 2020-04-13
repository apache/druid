/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.incremental;

import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.IntIteratorUtils;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.TransformableRowIterator;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.BitmapValues;
import org.apache.druid.segment.data.CloseableIndexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 */
public class IncrementalIndexAdapter implements IndexableAdapter
{
  private final Interval dataInterval;
  private final IncrementalIndex<?> index;
  private final Map<String, DimensionAccessor> accessors;

  private static class DimensionAccessor
  {
    private final IncrementalIndex.DimensionDesc dimensionDesc;
    @Nullable
    private final MutableBitmap[] invertedIndexes;
    private final DimensionIndexer indexer;

    public DimensionAccessor(IncrementalIndex.DimensionDesc dimensionDesc)
    {
      this.dimensionDesc = dimensionDesc;
      this.indexer = dimensionDesc.getIndexer();
      if (dimensionDesc.getCapabilities().hasBitmapIndexes()) {
        this.invertedIndexes = new MutableBitmap[indexer.getCardinality() + 1];
      } else {
        this.invertedIndexes = null;
      }
    }
  }

  public IncrementalIndexAdapter(Interval dataInterval, IncrementalIndex<?> index, BitmapFactory bitmapFactory)
  {
    this.dataInterval = dataInterval;
    this.index = index;

    final List<IncrementalIndex.DimensionDesc> dimensions = index.getDimensions();
    accessors = dimensions
        .stream()
        .collect(Collectors.toMap(IncrementalIndex.DimensionDesc::getName, DimensionAccessor::new));

    processRows(index, bitmapFactory, dimensions);
  }

  /**
   * Sometimes it's hard to tell whether one dimension contains a null value or not.
   * If one dimension had show a null or empty value explicitly, then yes, it contains
   * null value. But if one dimension's values are all non-null, it still early to say
   * this dimension does not contain null value. Consider a two row case, first row had
   * "dimA=1" and "dimB=2", the second row only had "dimA=3". To dimB, its value are "2" and
   * never showed a null or empty value. But when we combines these two rows, dimB is null
   * in row 2. So we should iterate all rows to determine whether one dimension contains
   * a null value.
   */
  private void processRows(
      IncrementalIndex<?> index,
      BitmapFactory bitmapFactory,
      List<IncrementalIndex.DimensionDesc> dimensions
  )
  {
    int rowNum = 0;
    for (IncrementalIndexRow row : index.getFacts().persistIterable()) {
      final Object[] dims = row.getDims();

      for (IncrementalIndex.DimensionDesc dimension : dimensions) {
        final int dimIndex = dimension.getIndex();
        DimensionAccessor accessor = accessors.get(dimension.getName());

        // Add 'null' to the dimension's dictionary.
        if (dimIndex >= dims.length || dims[dimIndex] == null) {
          accessor.indexer.processRowValsToUnsortedEncodedKeyComponent(null, true);
          continue;
        }
        final ColumnCapabilities capabilities = dimension.getCapabilities();

        if (capabilities.hasBitmapIndexes()) {
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
  public List<String> getDimensionNames()
  {
    return index.getDimensionNames();
  }

  @Override
  public List<String> getMetricNames()
  {
    return index.getMetricNames();
  }

  @Nullable
  @Override
  public <T extends Comparable<? super T>> CloseableIndexed<T> getDimValueLookup(String dimension)
  {
    final DimensionAccessor accessor = accessors.get(dimension);
    if (accessor == null) {
      return null;
    }

    final DimensionIndexer indexer = accessor.dimensionDesc.getIndexer();

    return indexer.getSortedIndexedValues();
  }

  @Override
  public TransformableRowIterator getRows()
  {
    return new IncrementalIndexRowIterator(index);
  }

  @Override
  public BitmapValues getBitmapValues(String dimension, int index)
  {
    DimensionAccessor accessor = accessors.get(dimension);
    if (accessor == null) {
      return BitmapValues.EMPTY;
    }
    ColumnCapabilities capabilities = accessor.dimensionDesc.getCapabilities();
    DimensionIndexer indexer = accessor.dimensionDesc.getIndexer();

    if (!capabilities.hasBitmapIndexes()) {
      return BitmapValues.EMPTY;
    }

    final int id = (Integer) indexer.getUnsortedEncodedValueFromSorted(index);
    if (id < 0 || id >= indexer.getCardinality()) {
      return BitmapValues.EMPTY;
    }

    MutableBitmap bitmapIndex = accessor.invertedIndexes[id];

    if (bitmapIndex == null) {
      return BitmapValues.EMPTY;
    }

    return new MutableBitmapValues(bitmapIndex);
  }

  static class MutableBitmapValues implements BitmapValues
  {
    private final MutableBitmap bitmapIndex;

    MutableBitmapValues(MutableBitmap bitmapIndex)
    {
      this.bitmapIndex = bitmapIndex;
    }

    @Override
    public int size()
    {
      return bitmapIndex.size();
    }

    @Override
    public IntIterator iterator()
    {
      return IntIteratorUtils.fromRoaringBitmapIntIterator(bitmapIndex.iterator());
    }
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
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }
}
