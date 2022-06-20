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

package org.apache.druid.segment.serde;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;

public class StringFrontCodedColumnIndexSupplier implements ColumnIndexSupplier
{
  private final BitmapFactory bitmapFactory;
  private final FrontCodedIndexed dictionary;

  @Nullable
  private final GenericIndexed<ImmutableBitmap> bitmaps;

  @Nullable
  private final ImmutableRTree indexedTree;

  public StringFrontCodedColumnIndexSupplier(
      BitmapFactory bitmapFactory,
      FrontCodedIndexed dictionary,
      @Nullable GenericIndexed<ImmutableBitmap> bitmaps,
      @Nullable ImmutableRTree indexedTree
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.dictionary = dictionary;
    this.indexedTree = indexedTree;
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    return null;
  }
}
