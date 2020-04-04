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

package org.apache.druid.utils;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.netty.util.SuppressForbidden;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class CollectionUtils
{
  private static final int MAX_EXPECTED_SIZE = (1 << 30);

  /**
   * Returns a lazy collection from a stream supplier and a size. {@link Collection#iterator()} of the returned
   * collection delegates to {@link Stream#iterator()} on the stream returned from the supplier.
   */
  public static <E> Collection<E> createLazyCollectionFromStream(Supplier<Stream<E>> sequentialStreamSupplier, int size)
  {
    return new AbstractCollection<E>()
    {
      @Override
      public Iterator<E> iterator()
      {
        return sequentialStreamSupplier.get().iterator();
      }

      @Override
      public Spliterator<E> spliterator()
      {
        return sequentialStreamSupplier.get().spliterator();
      }

      @Override
      public Stream<E> stream()
      {
        return sequentialStreamSupplier.get();
      }

      @Override
      public Stream<E> parallelStream()
      {
        return sequentialStreamSupplier.get().parallel();
      }

      @Override
      public int size()
      {
        return size;
      }
    };
  }

  public static <E> TreeSet<E> newTreeSet(Comparator<? super E> comparator, Iterable<E> elements)
  {
    TreeSet<E> set = new TreeSet<>(comparator);
    Iterables.addAll(set, elements);
    return set;
  }

  /**
   * Returns a transformed map from the given input map where the value is modified based on the given valueMapper
   * function.
   * Unlike {@link Maps#transformValues}, this method applies the mapping function eagerly to all key-value pairs
   * in the source map and returns a new {@link HashMap}, while {@link Maps#transformValues} returns a lazy map view.
   */
  public static <K, V, V2> Map<K, V2> mapValues(Map<K, V> map, Function<V, V2> valueMapper)
  {
    final Map<K, V2> result = Maps.newHashMapWithExpectedSize(map.size());
    map.forEach((k, v) -> result.put(k, valueMapper.apply(v)));
    return result;
  }

  /**
   * Returns a transformed map from the given input map where the key is modified based on the given keyMapper
   * function. This method fails if keys collide after applying the  given keyMapper function and
   * throws a IllegalStateException.
   *
   * @throws ISE if key collisions occur while applying specified keyMapper
   */
  public static <K, V, K2> Map<K2, V> mapKeys(Map<K, V> map, Function<K, K2> keyMapper)
  {
    final Map<K2, V> result = Maps.newHashMapWithExpectedSize(map.size());
    map.forEach((k, v) -> {
      final K2 k2 = keyMapper.apply(k);
      if (result.putIfAbsent(k2, v) != null) {
        throw new ISE("Conflicting key[%s] calculated via keyMapper for original key[%s]", k2, k);
      }
    });
    return result;
  }

  /**
   * Returns a LinkedHashMap with an appropriate size based on the callers expectedSize. This methods functionality
   * mirrors that of com.google.common.collect.Maps#newLinkedHashMapWithExpectedSize in Guava 19+. Thus, this method
   * can be replaced with Guava's implementation once Druid has upgraded its Guava dependency to a sufficient version.
   *
   * @param expectedSize the expected size of the LinkedHashMap
   * @return LinkedHashMap object with appropriate size based on callers expectedSize
   */
  @SuppressForbidden(reason = "java.util.LinkedHashMap#<init>(int)")
  public static <K, V> LinkedHashMap<K, V> newLinkedHashMapWithExpectedSize(int expectedSize)
  {
    // Gracefully handle negative paramaters.
    expectedSize = Math.max(0, expectedSize);
    if (expectedSize < 3) {
      return new LinkedHashMap<>(expectedSize + 1);
    }
    if (expectedSize < MAX_EXPECTED_SIZE) {
      return new LinkedHashMap<>((int) ((float) expectedSize / 0.75f + 1.0f));
    }
    return new LinkedHashMap<>(Integer.MAX_VALUE);
  }

  public static boolean isNullOrEmpty(@Nullable Collection<?> list)
  {
    return list == null || list.isEmpty();
  }

  private CollectionUtils()
  {
  }
}
