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

package org.apache.druid.collections.fastutil;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;

public class DruidIntList extends IntArrayList
{
  public DruidIntList(int capacity)
  {
    super(capacity);
  }

  public void addArray(int[] vals)
  {
    grow(size + vals.length);
    System.arraycopy(vals, 0, a, size, vals.length);
    size += vals.length;
  }

  public void fill(int val, int count)
  {
    grow(size + count);
    java.util.Arrays.fill(a, size, size + count, val);
    size += count;
  }

  public void fillWithRepeat(int[] vals, int repeat)
  {
    int count = vals.length * repeat;
    grow(size + count);
    for (int i = 0; i < count; i += vals.length) {
      System.arraycopy(vals, 0, a, size + i, vals.length);
    }
    size += count;
  }

  public void fillRuns(int[] vals, int runLength, int repeat)
  {
    if (runLength == 1) {
      fillWithRepeat(vals, repeat);
      return;
    }
    int count = vals.length * runLength * repeat;
    grow(size + count);
    for (int i = 0; i < repeat; ++i) {
      for (int val : vals) {
        // there's a += hidden in there, don't be tricked!
        java.util.Arrays.fill(a, size, size += runLength, val);
      }
    }
  }

  public void resetToSize(int targetSize)
  {
    a = new int[targetSize];
    size = 0;
  }

  /**
   * Method gratuitously "borrowed" from IntArrayList, would've been nice if that method wasn't private, but ah well.
   *
   * @param capacity the capacity to grow to
   * @see IntArrayList#grow(int)
   */
  @SuppressWarnings("ArrayEquality")
  private void grow(int capacity)
  {
    if (capacity <= a.length) {
      return;
    }
    if (a != IntArrays.DEFAULT_EMPTY_ARRAY) {
      capacity = (int) Math.max(Math.min((long) a.length + (a.length >> 1), Arrays.MAX_ARRAY_SIZE), capacity);
    } else if (capacity < DEFAULT_INITIAL_CAPACITY) {
      capacity = DEFAULT_INITIAL_CAPACITY;
    }
    a = IntArrays.forceCapacity(a, capacity, size);
    assert size <= a.length;
  }
}
