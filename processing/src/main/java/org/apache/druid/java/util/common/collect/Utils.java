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

package org.apache.druid.java.util.common.collect;


import org.apache.druid.java.util.common.guava.MergeIterator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Utils
{

  /**
   * Create a Map from iterables of keys and values. If there are more keys than values, or more values than keys,
   * the excess will be omitted.
   */
  public static <K, V> Map<K, V> zipMapPartial(Iterable<K> keys, Iterable<V> values)
  {
    Map<K, V> retVal = new LinkedHashMap<>();

    Iterator<K> keysIter = keys.iterator();
    Iterator<V> valsIter = values.iterator();

    while (keysIter.hasNext()) {
      final K key = keysIter.next();

      if (valsIter.hasNext()) {
        retVal.put(key, valsIter.next());
      } else {
        break;
      }

    }

    return retVal;
  }

  @SafeVarargs
  public static <T> List<T> nullableListOf(@Nullable T... elements)
  {
    final List<T> list;
    if (elements == null) {
      list = new ArrayList<>(1);
      list.add(null);
    } else {
      list = new ArrayList<>(elements.length);
      list.addAll(Arrays.asList(elements));
    }
    return list;
  }

  public static <T> boolean isPrefix(List<T> small, List<T> big)
  {
    for (int i = 0; i < small.size(); i++) {
      if (!small.get(i).equals(big.get(i))) {
        return false;
      }
    }

    return true;
  }

  public static String safeObjectClassGetName(@Nullable Object o)
  {
    if (o == null) {
      return "NULL";
    } else {
      return o.getClass().getName();
    }
  }

  /**
   * Like Guava's {@link com.google.common.collect.Iterators#mergeSorted(Iterable, Comparator)}, but avoids
   * calling next() on any iterator prior to returning the value returned by the previous call to next(). This is
   * important when merging iterators that reuse container objects across calls to next().
   *
   * If the Iterators are {@link org.apache.druid.java.util.common.parsers.CloseableIterator}, use
   * {@link org.apache.druid.java.util.common.CloseableIterators#mergeSorted} instead.
   */
  public static <T> Iterator<T> mergeSorted(
      final Iterable<? extends Iterator<? extends T>> sortedIterators,
      final Comparator<? super T> comparator
  )
  {
    return new MergeIterator<>(sortedIterators, comparator);
  }
}
