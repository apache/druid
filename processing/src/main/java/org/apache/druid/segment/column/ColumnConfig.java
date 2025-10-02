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

package org.apache.druid.segment.column;

import org.apache.druid.segment.index.semantic.NumericRangeIndexes;

public interface ColumnConfig
{
  /**
   * this value was chosen testing range filters on double columns with a variety of ranges at which this ratio
   * of number of bitmaps compared to total number of rows appeared to be around the threshold where indexes stopped
   * performing consistently faster than a full scan + value matcher
   */
  double DEFAULT_SKIP_VALUE_RANGE_INDEX_SCALE = 0.1;

  ColumnConfig DEFAULT = new ColumnConfig() {};

  /**
   * Use range indexes if dictionary range is same size or smaller than selection size
   */
  ColumnConfig SELECTION_SIZE = new ColumnConfig()
  {

    @Override
    public double skipValueRangeIndexScale()
    {
      return 1.0;
    }
  };

  /**
   * If the number of selected rows in a column multiplied by this value is smaller than the total number of bitmap
   * index operations required to perform to use {@link NumericRangeIndexes}, then for any {@link ColumnIndexSupplier}
   * which chooses to participate in this config it will skip computing the index, indicated by a return value of null
   * from the 'forRange' methods, to force the filter to be processed with a scan using a
   * {@link org.apache.druid.query.filter.ValueMatcher} instead.
   * <p>
   * For range indexes on columns where every value has an index, the number of bitmap operations is determined by how
   * many individual values fall in the range, a subset of the columns total cardinality.
   * <p>
   * Currently only the {@link org.apache.druid.segment.nested.NestedCommonFormatColumn} implementations of
   * {@link ColumnIndexSupplier} support this behavior.
   * <p>
   * This can make some standalone filters faster in cases where the overhead of walking the value dictionary and
   * combining bitmaps to construct a {@link org.apache.druid.segment.BitmapOffset} or
   * {@link org.apache.druid.segment.vector.BitmapVectorOffset} can exceed the cost of just using doing a full scan
   * and using a {@link org.apache.druid.query.filter.ValueMatcher}.
   * <p>
   * Where this is especially useful is in cases where the range index is used as part of some
   * {@link org.apache.druid.segment.filter.AndFilter}, which segment processing partitions into groups of 'pre'
   * filters, composed of those which should use indexes, and 'post' filters, which should use a matcher on the offset
   * created by the indexes to filter the remaining results. This value pushes what would have been expensive index
   * computations to go into the 'pre' group into using a value matcher as part of the 'post' group instead, sometimes
   * providing an order of magnitude or higher performance increase.
   */
  default double skipValueRangeIndexScale()
  {
    return DEFAULT_SKIP_VALUE_RANGE_INDEX_SCALE;
  }
}
