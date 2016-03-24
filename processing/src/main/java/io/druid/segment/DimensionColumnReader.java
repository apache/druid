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
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;

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
   * @param value         value to filter on
   * @param bitmapFactory bitmap factory
   * @param numRows       number of rows in the column to be read
   *
   * @return A bitmap indicating rows that matched the provided value
   */
  public ImmutableBitmap getBitmapIndex(String value, BitmapFactory bitmapFactory, int numRows);


  /**
   * Filter on a predicate and return a result bitmap
   *
   * @param predicate     predicate to filter on
   * @param bitmapFactory bitmap factory
   *
   * @return A bitmap indicating rows that matched the provided predicate
   */
  public ImmutableBitmap getBitmapIndex(Predicate predicate, BitmapFactory bitmapFactory);


  /**
   * Filter on a Bound and return a result bitmap
   *
   * @param bound          spatial boundary, used for spatial filtering
   * @param bitmapFactory  bitmap factory
   *
   * @return A bitmap indicating rows that matched the provided bound
   */
  public ImmutableBitmap getBitmapIndex(Bound bound, BitmapFactory bitmapFactory);


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
}
