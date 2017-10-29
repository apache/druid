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

package io.druid.query;

import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;

public final class DefaultBitmapResultFactory implements BitmapResultFactory<ImmutableBitmap>
{
  private final BitmapFactory factory;

  public DefaultBitmapResultFactory(BitmapFactory factory)
  {
    this.factory = factory;
  }

  @Override
  public ImmutableBitmap wrapUnknown(ImmutableBitmap bitmap)
  {
    return bitmap;
  }

  @Override
  public ImmutableBitmap wrapDimensionValue(ImmutableBitmap bitmap)
  {
    return bitmap;
  }

  @Override
  public ImmutableBitmap wrapAllFalse(ImmutableBitmap allFalseBitmap)
  {
    return allFalseBitmap;
  }

  @Override
  public ImmutableBitmap wrapAllTrue(ImmutableBitmap allTrueBitmap)
  {
    return allTrueBitmap;
  }

  @Override
  public boolean isEmpty(ImmutableBitmap bitmapResult)
  {
    return bitmapResult.isEmpty();
  }

  @Override
  public ImmutableBitmap intersection(Iterable<ImmutableBitmap> bitmapResults)
  {
    return factory.intersection(bitmapResults);
  }

  @Override
  public ImmutableBitmap union(Iterable<ImmutableBitmap> bitmapResults)
  {
    return factory.union(bitmapResults);
  }

  @Override
  public ImmutableBitmap unionDimensionValueBitmaps(Iterable<ImmutableBitmap> dimensionValueBitmaps)
  {
    return factory.union(dimensionValueBitmaps);
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap bitmapResult, int numRows)
  {
    return factory.complement(bitmapResult, numRows);
  }

  @Override
  public ImmutableBitmap toImmutableBitmap(ImmutableBitmap bitmapResult)
  {
    return bitmapResult;
  }
}
