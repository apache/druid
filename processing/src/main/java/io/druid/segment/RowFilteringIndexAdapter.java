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

package io.druid.segment;

import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.BitmapValues;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import java.util.List;
import java.util.function.Predicate;

/**
 */
public class RowFilteringIndexAdapter implements IndexableAdapter
{
  private final QueryableIndexIndexableAdapter baseAdapter;
  private final Predicate<RowPointer> filter;

  public RowFilteringIndexAdapter(QueryableIndexIndexableAdapter baseAdapter, Predicate<RowPointer> filter)
  {
    this.baseAdapter = baseAdapter;
    this.filter = filter;
  }

  @Override
  public Interval getDataInterval()
  {
    return baseAdapter.getDataInterval();
  }

  @Override
  public int getNumRows()
  {
    return baseAdapter.getNumRows();
  }

  @Override
  public List<String> getDimensionNames()
  {
    return baseAdapter.getDimensionNames();
  }

  @Override
  public List<String> getMetricNames()
  {
    return baseAdapter.getMetricNames();
  }

  @Override
  public <T extends Comparable<T>> Indexed<T> getDimValueLookup(String dimension)
  {
    return baseAdapter.getDimValueLookup(dimension);
  }

  @Override
  public TransformableRowIterator getRows()
  {
    QueryableIndexIndexableAdapter.RowIteratorImpl baseRowIterator = baseAdapter.getRows();
    return new ForwardingRowIterator(baseRowIterator)
    {
      /**
       * This memoization is needed to conform to {@link RowIterator#getPointer()} specification.
       */
      private boolean memoizedOffset = false;

      @Override
      public boolean moveToNext()
      {
        while (baseRowIterator.moveToNext()) {
          if (filter.test(baseRowIterator.getPointer())) {
            baseRowIterator.memoizeOffset();
            memoizedOffset = true;
            return true;
          }
        }
        // Setting back to the last valid offset in this iterator, as required by RowIterator.getPointer() spec.
        if (memoizedOffset) {
          baseRowIterator.resetToMemoizedOffset();
        }
        return false;
      }
    };
  }

  @Override
  public String getMetricType(String metric)
  {
    return baseAdapter.getMetricType(metric);
  }

  @Override
  public ColumnCapabilities getCapabilities(String column)
  {
    return baseAdapter.getCapabilities(column);
  }

  @Override
  public BitmapValues getBitmapValues(String dimension, int dictId)
  {
    return baseAdapter.getBitmapValues(dimension, dictId);
  }

  @Override
  public Metadata getMetadata()
  {
    return baseAdapter.getMetadata();
  }
}
