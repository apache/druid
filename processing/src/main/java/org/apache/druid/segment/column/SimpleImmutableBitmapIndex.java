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

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.BitmapResultFactory;

/**
 * {@link SimpleBitmapColumnIndex} which wraps a single {@link ImmutableBitmap}
 */
public final class SimpleImmutableBitmapIndex extends SimpleBitmapColumnIndex
{
  private final ImmutableBitmap bitmap;

  public SimpleImmutableBitmapIndex(ImmutableBitmap bitmap)
  {
    this.bitmap = bitmap;
  }

  @Override
  public double estimateSelectivity(int totalRows)
  {
    return Math.min(1, (double) bitmap.size() / totalRows);
  }

  @Override
  public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
  {
    return bitmapResultFactory.wrapDimensionValue(bitmap);
  }
}
