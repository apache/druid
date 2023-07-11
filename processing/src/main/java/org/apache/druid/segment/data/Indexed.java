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

package org.apache.druid.segment.data;

import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;

/**
 * Indexed is a fixed-size, immutable, indexed set of values which allows
 * locating a specific index via an exact match, the semantics of which are defined
 * by the implementation. The indexed is ordered, and may contain duplicate values.
 *
 * @param <T> the type of the value
 */
@PublicApi
public interface Indexed<T> extends Iterable<T>, HotLoopCallee
{
  static <T> Indexed<T> empty()
  {
    return new Indexed<T>()
    {
      @Override
      public int size()
      {
        return 0;
      }

      @Nullable
      @Override
      public T get(int index)
      {
        Indexed.checkIndex(index, 0);
        return null;
      }

      @Override
      public int indexOf(@Nullable T value)
      {
        return -1;
      }

      @Override
      public Iterator<T> iterator()
      {
        return Collections.emptyIterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    };
  }

  /**
   * Number of elements in the value set
   */
  int size();

  /**
   * Get the value at specified position
   */
  @CalledFromHotLoop
  @Nullable
  T get(int index);

  /**
   * Returns the index of "value" in this Indexed object, or a negative number if the value is not present.
   * The negative number is not guaranteed to be any particular number unless {@link #isSorted()} returns true, in
   * which case it will be a negative number equal to (-(insertion point) - 1), in the manner of Arrays.binarySearch.
   *
   * @param value value to search for
   *
   * @return index of value, or a negative number (equal to (-(insertion point) - 1) if {@link #isSorted()})
   */
  int indexOf(@Nullable T value);

  /**
   * Indicates if this value set is sorted, the implication being that the contract of {@link #indexOf} is strenthened
   * to return a negative number equal to (-(insertion point) - 1) when the value is not present in the set.
   */
  default boolean isSorted()
  {
    return false;
  }

  /**
   * Checks  if {@code index} is between 0 and {@code size}. Similar to Preconditions.checkElementIndex() except this
   * method throws {@link IAE} with custom error message.
   * <p>
   * Used here to get existing behavior(same error message and exception) of V1 {@link GenericIndexed}.
   *
   * @param index identifying an element of an {@link Indexed}
   * @param size size of the {@link Indexed}
   */
  static void checkIndex(int index, int size)
  {
    if (index < 0) {
      throw new IAE("Index[%s] < 0", index);
    }
    if (index >= size) {
      throw new IAE("Index[%d] >= size[%d]", index, size);
    }
  }
}
