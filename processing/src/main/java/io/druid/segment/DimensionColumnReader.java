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

import com.google.common.base.Predicate;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.search.Bound;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;

import java.io.Closeable;
import java.util.Map;

/**
 * Processing related interface
 *
 * A DimensionColumnReader is an object that encapsulates Column-reading operations used by
 * QueryableIndex and its associated wrappers.
 *
 * @param <EncodedType> class of the encoded values
 * @param <ActualType> class of the actual values
 */
public interface DimensionColumnReader<EncodedType extends Comparable<EncodedType>, ActualType extends Comparable<ActualType>>
{
  /**
   * Filter on a value and return a result bitmap
   *
   * If the dimension type being implemented does not support bitmap indexes, this function can be
   * left unimplemented.
   *
   * @param value         value to filter on
   * @return A bitmap indicating rows that matched the provided value
   */
  public ImmutableBitmap getBitmapIndex(String value);


  /**
   * Get the minimum value for this dimension.
   *
   * @return min value
   */
  public ActualType getMinValue();


  /**
   * Get the maximum value for this dimension.
   *
   * @return max value
   */
  public ActualType getMaxValue();


  /**
   * Returns an indexed structure of this dimension's sorted actual values.
   * The integer IDs represent the ordering of the sorted values.
   *
   * @return Sorted index of actual values
   */
  public Indexed<ActualType> getSortedIndexedValues();

  /**
   * Get the ColumnCapabilites of this dimension.
   *
   * @return column capabilities
   */
  public ColumnCapabilities getCapabilities();


  /**
   * Return a DimensionSelector, an object used to read rows from a StorageAdapter's Cursor.
   *
   * See StringDimensionColumnReader.makeDimensionSelector() for a reference implementation.
   *
   * @param dimensionSpec Specifies the output name of a dimension and any extraction functions to be applied.
   * @param cursorOffsetHolder Provides access to the current column offset within the Cursor
   * @param dimensionColumnCache Externally provided cache for columns.
   * @return A new DimensionSelector that reads rows at the offsets specified by cursorOffsetHolder
   */
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      QueryableIndexStorageAdapter.CursorOffsetHolder cursorOffsetHolder,
      Map<String, Closeable> dimensionColumnCache
  );
}
