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

package org.apache.druid.benchmark;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;

public class MockBitmapIndexSelector implements BitmapIndexSelector
{
  private final GenericIndexed<String> dictionary;
  private final BitmapFactory bitmapFactory;
  private final BitmapIndex bitmapIndex;

  public MockBitmapIndexSelector(
      GenericIndexed<String> dictionary,
      BitmapFactory bitmapFactory,
      BitmapIndex bitmapIndex)
  {
    this.dictionary = dictionary;
    this.bitmapFactory = bitmapFactory;
    this.bitmapIndex = bitmapIndex;
  }

  @Override
  public CloseableIndexed<String> getDimensionValues(String dimension)
  {
    return dictionary;
  }

  @Override
  public ColumnCapabilities.Capable hasMultipleValues(final String dimension)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumRows()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(String dimension, String value)
  {
    return bitmapIndex.getBitmapForValue(value);
  }

  @Override
  public BitmapIndex getBitmapIndex(String dimension)
  {
    return bitmapIndex;
  }

  @Override
  public ImmutableRTree getSpatialIndex(String dimension)
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return null;
  }
}
