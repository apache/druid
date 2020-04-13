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

package org.apache.druid.common.utils;

public class IntArrayUtils
{
  /**
   * Inverses the values of the given array with their indexes.
   * For example, the result for [2, 0, 1] is [1, 2, 0] because
   *
   * a[0]: 2 => a[2]: 0
   * a[1]: 0 => a[0]: 1
   * a[2]: 1 => a[1]: 2
   */
  public static void inverse(int[] a)
  {
    for (int i = 0; i < a.length; i++) {
      if (a[i] >= 0) {
        inverseLoop(a, i);
      }
    }

    for (int i = 0; i < a.length; i++) {
      a[i] = ~a[i];
    }
  }

  private static void inverseLoop(int[] a, int startValue)
  {
    final int startIndex = a[startValue];

    int nextIndex = startIndex;
    int nextValue = startValue;

    do {
      final int curIndex = nextIndex;
      final int curValue = nextValue;

      nextValue = curIndex;
      nextIndex = a[curIndex];

      a[curIndex] = ~curValue;
    } while (nextIndex != startIndex);
  }

  private IntArrayUtils()
  {
  }
}
