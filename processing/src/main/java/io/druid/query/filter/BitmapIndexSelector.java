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

package io.druid.query.filter;

import com.google.common.base.Predicate;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.collections.spatial.search.Bound;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;

/**
 */
public interface BitmapIndexSelector
{
  public boolean hasBitmapIndexes(String dimension);
  public ValueType getDimensionType(String dimension);

  // for scans on dims that don't have bitmap indexes
  public ImmutableBitmap getBitmapIndexFromColumnScan(String dimension, Predicate predicate);

  // for scans with bitmap indexes
  public Indexed<String> getDimensionValues(String dimension);
  public int getNumRows();
  public BitmapFactory getBitmapFactory();
  public ImmutableBitmap getBitmapIndex(String dimension, String value);

  // for scans with spatial indexes
  public ImmutableBitmap getBitmapIndex(String dimension, Bound bound);
}
