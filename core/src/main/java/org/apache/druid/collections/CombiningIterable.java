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

package org.apache.druid.collections;

import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.guava.MergeIterable;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.BinaryOperator;

/**
 */
public class CombiningIterable<T> implements Iterable<T>
{
  /**
   * Creates a CombiningIterable around a MergeIterable such that equivalent elements are thrown away
   *
   * If there are multiple Iterables in parameter "in" with equivalent objects, there are no guarantees
   * around which object will win.  You will get *some* object from one of the Iterables, but which Iterable is
   * unknown.
   *
   * @param in An Iterable of Iterables to be merged
   * @param comparator the Comparator to determine sort and equality
   * @param <T> Type of object
   * @return An Iterable that is the merge of all Iterables from in such that there is only one instance of
   * equivalent objects.
   */
  @SuppressWarnings("unchecked")
  public static <T> CombiningIterable<T> createSplatted(
      Iterable<? extends Iterable<T>> in,
      Comparator<T> comparator
  )
  {
    return create(
        new MergeIterable<>(comparator, (Iterable<Iterable<T>>) in),
        comparator,
        GuavaUtils::firstNonNull
    );
  }

  public static <T> CombiningIterable<T> create(
      Iterable<T> it,
      Comparator<T> comparator,
      BinaryOperator<T> fn
  )
  {
    return new CombiningIterable<>(it, comparator, fn);
  }

  private final Iterable<T> it;
  private final Comparator<T> comparator;
  private final BinaryOperator<T> fn;

  public CombiningIterable(
      Iterable<T> it,
      Comparator<T> comparator,
      BinaryOperator<T> fn
  )
  {
    this.it = it;
    this.comparator = comparator;
    this.fn = fn;
  }

  @Override
  public Iterator<T> iterator()
  {
    return CombiningIterator.create(it.iterator(), comparator, fn);
  }
}
