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

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class CollectionUtils
{
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
   */
  public static <K, V, V2> Map<K, V2> mapValues(Map<K, V> map, Function<V, V2> valueMapper)
  {
    final Map<K, V2> result = Maps.newHashMapWithExpectedSize(map.size());
    map.forEach((k, v) -> result.put(k, valueMapper.apply(v)));
    return result;
  }

  private CollectionUtils()
  {
  }
}
