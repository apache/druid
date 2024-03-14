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

package org.apache.druid.segment.index;

import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.segment.column.ColumnConfig;

import javax.annotation.Nullable;

/**
 * Common {@link BitmapColumnIndex} implementation for indexes which need to scan a range of values. Contains logic
 * to skip computing indexes with {@link #computeBitmapResult(BitmapResultFactory, int, int, boolean)} if
 * {@link #rangeSize} is larger than {@link #sizeScale} multiplied by the number of selected rows. Numeric range
 * indexes will typically want to set {@link #sizeScale} to a double closer to 0.0 than to 1.0 because numeric
 * comparisons are relatively cheap compared to bitmap operations. Most numerical implementations should use the
 * value of {@link ColumnConfig#skipValueRangeIndexScale()}.
 * <p>
 * Other implementations should adjust {@link #sizeScale} as appropriate for the expense of the value matcher compared
 * to the expense of the bitmap operations.
 */
public abstract class DictionaryRangeScanningBitmapIndex extends SimpleImmutableBitmapDelegatingIterableIndex
{
  private final double sizeScale;
  private final int rangeSize;

  public DictionaryRangeScanningBitmapIndex(double sizeScale, int rangeSize)
  {
    this.sizeScale = sizeScale;
    this.rangeSize = rangeSize;
  }

  @Nullable
  @Override
  public final <T> T computeBitmapResult(
      BitmapResultFactory<T> bitmapResultFactory,
      int selectionRowCount,
      int totalRowCount,
      boolean includeUnknown
  )
  {
    final int scale = (int) Math.ceil(sizeScale * selectionRowCount);
    if (rangeSize > scale) {
      return null;
    }
    return computeBitmapResult(bitmapResultFactory, includeUnknown);
  }
}
