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
import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;

import javax.annotation.Nullable;
import java.util.Comparator;

@PublicApi
public interface Indexed<T> extends Iterable<T>, HotLoopCallee
{

  int size();

  @CalledFromHotLoop
  @Nullable
  T get(int index);

  /**
   * Returns the index of "value" in this Indexed object, or a negative number if the value is not present.
   * The negative number is not guaranteed to be any particular number. Subclasses may tighten this contract
   * (GenericIndexed does this).
   *
   * @param value value to search for
   *
   * @return index of value, or a negative number
   */
  int indexOf(@Nullable T value);

  /**
   * Returns the index of "value" in some object whose values are accessible by index some {@link IndexedGetter}, or
   * (-(insertion point) - 1) if the value is not present, in the manner of Arrays.binarySearch.
   *
   * This is used by {@link GenericIndexed} to strengthen the contract of {@link #indexOf(Object)}, which only
   * guarantees that values-not-found will return some negative number.
   *
   * @param value value to search for
   *
   * @return index of value, or negative number equal to (-(insertion point) - 1).
   */
  static <T> int indexOf(IndexedGetter<T> indexed, int size, Comparator<T> comparator, @Nullable T value)
  {
    int minIndex = 0;
    int maxIndex = size - 1;
    while (minIndex <= maxIndex) {
      int currIndex = (minIndex + maxIndex) >>> 1;

      T currValue = indexed.get(currIndex);
      int comparison = comparator.compare(currValue, value);
      if (comparison == 0) {
        return currIndex;
      }

      if (comparison < 0) {
        minIndex = currIndex + 1;
      } else {
        maxIndex = currIndex - 1;
      }
    }

    return -(minIndex + 1);
  }

  @FunctionalInterface
  interface IndexedGetter<T>
  {
    @Nullable
    T get(int id);
  }
}
