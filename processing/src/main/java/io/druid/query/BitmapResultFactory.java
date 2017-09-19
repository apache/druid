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

/**
 * BitmapResultFactory is an abstraction that allows to record something along with preFilter bitmap construction, and
 * emit this information as dimension(s) of query metrics. BitmapResultFactory is similar to {@link BitmapFactory}: it
 * has the same methods with the exception that it accepts generic type T (bitmap wrapper type) instead of {@link
 * ImmutableBitmap}.
 *
 * {@link DefaultBitmapResultFactory} is a no-op implementation, where "wrapper" type is {@code ImmutableBitmap} itself.
 *
 * BitmapResultFactory delegates actual operations on bitmaps to a {@code BitmapFactory}, which it accepts in
 * constructor, called from {@link QueryMetrics#makeBitmapResultFactory(BitmapFactory)}.
 *
 * Emitting of query metric dimension(s) should be done from {@link #toImmutableBitmap(Object)}, the "unwrapping"
 * method, called only once to obtain the final preFilter bitmap.
 *
 * Implementors expectations
 * -------------------------
 * BitmapResultFactory is a part of the {@link QueryMetrics} subsystem, so this interface could be changed often, in
 * every Druid release (including "patch" releases). Users who create their custom implementations of
 * BitmapResultFactory should be ready to fix the code of their code to accommodate interface changes (e. g. implement
 * new methods) when they update Druid. See {@link QueryMetrics} Javadoc for more info.
 *
 * @param <T> the bitmap result (wrapper) type
 * @see QueryMetrics#makeBitmapResultFactory(BitmapFactory)
 * @see QueryMetrics#reportBitmapConstructionTime(long)
 */
public interface BitmapResultFactory<T>
{
  /**
   * Wraps a bitmap of unknown nature.
   */
  T wrapUnknown(ImmutableBitmap bitmap);

  /**
   * Wraps a bitmap which designates rows in a segment with some specific dimension value.
   */
  T wrapDimensionValue(ImmutableBitmap bitmap);

  /**
   * Wraps a bitmap which is a result of {@link BitmapFactory#makeEmptyImmutableBitmap()} call.
   */
  T wrapAllFalse(ImmutableBitmap allFalseBitmap);

  /**
   * Wraps a bitmap which is a result of {@link BitmapFactory#complement(ImmutableBitmap)} called with
   * {@link BitmapFactory#makeEmptyImmutableBitmap()} as argument.
   */
  T wrapAllTrue(ImmutableBitmap allTrueBitmap);

  /**
   * Checks that the wrapped bitmap is empty, see {@link ImmutableBitmap#isEmpty()}.
   */
  boolean isEmpty(T bitmapResult);

  /**
   * Delegates to {@link BitmapFactory#intersection(Iterable)} on the wrapped bitmaps, and returns a bitmap result
   * wrapping the resulting intersection ImmutableBitmap.
   */
  T intersection(Iterable<T> bitmapResults);

  /**
   * Delegates to {@link BitmapFactory#union(Iterable)} on the wrapped bitmaps, and returns a bitmap result wrapping
   * the resulting union ImmutableBitmap.
   */
  T union(Iterable<T> bitmapResults);

  /**
   * Equivalent of intersection(Iterables.transform(dimensionValueBitmaps, factory::wrapDimensionValue)), but doesn't
   * create a lot of bitmap result objects.
   */
  T unionDimensionValueBitmaps(Iterable<ImmutableBitmap> dimensionValueBitmaps);

  /**
   * Delegates to {@link BitmapFactory#complement(ImmutableBitmap, int)} on the wrapped bitmap, and returns a bitmap
   * result wrapping the resulting complement ImmutableBitmap.
   */
  T complement(T bitmapResult, int numRows);

  /**
   * Unwraps bitmapResult back to ImmutableBitmap. BitmapResultFactory should emit query metric dimension(s) in the
   * implementation of this method.
   */
  ImmutableBitmap toImmutableBitmap(T bitmapResult);
}
