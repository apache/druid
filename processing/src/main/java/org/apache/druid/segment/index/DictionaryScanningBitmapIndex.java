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

import javax.annotation.Nullable;

/**
 * Common {@link BitmapColumnIndex} implementation for indexes which are computed by scanning the entire value
 * dictionary of the underlying column to check if the value index matches the filter. Contains logic to skip computing
 * indexes with {@link #computeBitmapResult(BitmapResultFactory, int, int, boolean)} if 'selectionRowCount' does not
 * equal 'totalRowCount' and 'selectionRowCount' is smaller than {@link #dictionarySize} multiplied by
 * {@link #scaleThreshold}. The default {@link #scaleThreshold} value is 1.0, meaning that if {@link #dictionarySize}
 * is larger than 'selectionRowCount' we skip using indexes, the idea being we would either have to perform the check
 * against the values in the dictionary or the values in the remaining rows, since remaining rows is smaller we should
 * just do that instead of spending time to compute indexes to further shrink 'selectionRowCount'.
 */
public abstract class DictionaryScanningBitmapIndex extends SimpleImmutableBitmapIterableIndex
{
  private final int dictionarySize;
  private final double scaleThreshold;

  public DictionaryScanningBitmapIndex(int dictionarySize)
  {
    this(dictionarySize, 1.0);
  }

  public DictionaryScanningBitmapIndex(int dictionarySize, double scaleThreshold)
  {
    this.dictionarySize = dictionarySize;
    this.scaleThreshold = scaleThreshold;
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
    if (selectionRowCount != totalRowCount && selectionRowCount < (dictionarySize * scaleThreshold)) {
      return null;
    }
    return bitmapResultFactory.unionDimensionValueBitmaps(getBitmapIterable(includeUnknown));
  }
}
