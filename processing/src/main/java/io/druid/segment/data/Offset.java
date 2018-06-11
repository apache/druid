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

package io.druid.segment.data;

import io.druid.annotations.SubclassesMustBePublic;
import io.druid.query.monomorphicprocessing.CalledFromHotLoop;

/**
 * The "mutable" version of a ReadableOffset.  Introduces "increment()" and "withinBounds()" methods, which are
 * very similar to "next()" and "hasNext()" on the Iterator interface except increment() does not return a value.
 * 
 * This class is not thread-safe, all it's methods, including {@link #reset()} and {@link #clone()}, must be called
 * from a single thread.
 *
 * Annotated with {@link SubclassesMustBePublic} because Offset occurrences are replaced with a subclass in {@link
 * io.druid.query.topn.Historical1SimpleDoubleAggPooledTopNScannerPrototype} and {@link
 * io.druid.query.topn.HistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopNScannerPrototype} during
 * specialization, and specialized version of those prototypes must be able to any subclass of Offset.
 *
 * This interface is the core "pointer" interface that is used to create {@link io.druid.segment.ColumnValueSelector}s
 * over historical segments. It's counterpart for incremental index is {@link
 * io.druid.segment.incremental.IncrementalIndexRowHolder}.
 */
@SubclassesMustBePublic
public abstract class Offset implements ReadableOffset, Cloneable
{
  @CalledFromHotLoop
  public abstract void increment();

  @CalledFromHotLoop
  public abstract boolean withinBounds();

  /**
   * Resets the Offset to the position it was created or cloned with.
   */
  public abstract void reset();

  /**
   * Returns the same offset ("this") or a readable "view" of this offset, which always returns the same value from
   * {@link #getOffset()}, as this offset. This method is useful for "unwrapping" such offsets as {@link
   * io.druid.segment.FilteredOffset} and reduce reference indirection, when only {@link ReadableOffset} API is needed.
   */
  public abstract ReadableOffset getBaseReadableOffset();

  @Override
  public Offset clone()
  {
    try {
      return (Offset) super.clone();
    }
    catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}
