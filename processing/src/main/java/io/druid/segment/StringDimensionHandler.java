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

import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.java.util.common.ISE;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ZeroIndexedInts;
import io.druid.segment.selector.settable.SettableColumnValueSelector;
import io.druid.segment.selector.settable.SettableDimensionValueSelector;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.util.Comparator;

public class StringDimensionHandler implements DimensionHandler<Integer, int[], String>
{

  /**
   * Compares {@link IndexedInts} lexicographically, with the exception that if a row contains only zeros (that's the
   * index of null) at all positions, it is considered "null" as a whole and is "less" than any "non-null" row. Empty
   * row (size is zero) is also considered "null".
   *
   * The implementation is a bit complicated because it tries to check each position of both rows only once.
   */
  private static final Comparator<ColumnValueSelector> DIMENSION_SELECTOR_COMPARATOR = (s1, s2) -> {
    IndexedInts row1 = getRow(s1);
    IndexedInts row2 = getRow(s2);
    int len1 = row1.size();
    int len2 = row2.size();
    boolean row1IsNull = true;
    boolean row2IsNull = true;
    for (int i = 0; i < Math.min(len1, len2); i++) {
      int v1 = row1.get(i);
      row1IsNull &= v1 == 0;
      int v2 = row2.get(i);
      row2IsNull &= v2 == 0;
      int valueDiff = Integer.compare(v1, v2);
      if (valueDiff != 0) {
        return valueDiff;
      }
    }
    //noinspection SubtractionInCompareTo -- substraction is safe here, because lenghts or rows are small numbers.
    int lenDiff = len1 - len2;
    if (lenDiff == 0) {
      return 0;
    } else {
      if (!row1IsNull || !row2IsNull) {
        return lenDiff;
      } else {
        return compareRestNulls(row1, len1, row2, len2);
      }
    }
  };

  private static int compareRestNulls(IndexedInts row1, int len1, IndexedInts row2, int len2)
  {
    if (len1 < len2) {
      for (int i = len1; i < len2; i++) {
        if (row2.get(i) != 0) {
          return -1;
        }
      }
    } else {
      for (int i = len2; i < len1; i++) {
        if (row1.get(i) != 0) {
          return 1;
        }
      }
    }
    return 0;
  }

  /**
   * Value for absent column, i. e. {@link NilColumnValueSelector}, should be equivalent to [null] during index merging.
   *
   * During index merging, if one of the merged indexes has absent columns, {@link StringDimensionMergerV9} ensures
   * that null value is present, and it has index = 0 after sorting, because sorting puts null first. See {@link
   * StringDimensionMergerV9#hasNull} and the place where it is assigned.
   */
  private static IndexedInts getRow(ColumnValueSelector s)
  {
    if (s instanceof DimensionSelector) {
      return ((DimensionSelector) s).getRow();
    } else if (s instanceof NilColumnValueSelector) {
      return ZeroIndexedInts.instance();
    } else {
      throw new ISE(
          "ColumnValueSelector[%s], only DimensionSelector or NilColumnValueSelector is supported",
          s.getClass()
      );
    }
  }

  private final String dimensionName;
  private final MultiValueHandling multiValueHandling;
  private final boolean hasBitmapIndexes;

  public StringDimensionHandler(String dimensionName, MultiValueHandling multiValueHandling, boolean hasBitmapIndexes)
  {
    this.dimensionName = dimensionName;
    this.multiValueHandling = multiValueHandling;
    this.hasBitmapIndexes = hasBitmapIndexes;
  }

  @Override
  public String getDimensionName()
  {
    return dimensionName;
  }

  @Override
  public MultiValueHandling getMultivalueHandling()
  {
    return multiValueHandling;
  }

  @Override
  public int getLengthOfEncodedKeyComponent(int[] dimVals)
  {
    return dimVals.length;
  }

  @Override
  public Comparator<ColumnValueSelector> getEncodedValueSelectorComparator()
  {
    return DIMENSION_SELECTOR_COMPARATOR;
  }

  @Override
  public SettableColumnValueSelector makeNewSettableEncodedValueSelector()
  {
    return new SettableDimensionValueSelector();
  }

  @Override
  public DimensionIndexer<Integer, int[], String> makeIndexer()
  {
    return new StringDimensionIndexer(multiValueHandling, hasBitmapIndexes);
  }

  @Override
  public DimensionMergerV9 makeMerger(
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    // Sanity-check capabilities.
    if (hasBitmapIndexes != capabilities.hasBitmapIndexes()) {
      throw new ISE(
          "capabilities.hasBitmapIndexes[%s] != this.hasBitmapIndexes[%s]",
          capabilities.hasBitmapIndexes(),
          hasBitmapIndexes
      );
    }

    return new StringDimensionMergerV9(dimensionName, indexSpec, segmentWriteOutMedium, capabilities, progress);
  }
}
