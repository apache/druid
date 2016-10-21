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

package io.druid.collections;

import java.util.Comparator;
import java.util.Iterator;

import io.druid.java.util.common.guava.MergeIterable;
import io.druid.java.util.common.guava.nary.BinaryFn;

/**
 */
public class CombiningIterable<InType> implements Iterable<InType>
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
   * @param <InType> Type of object
   * @return An Iterable that is the merge of all Iterables from in such that there is only one instance of
   * equivalent objects.
   */
  @SuppressWarnings("unchecked")
  public static <InType> CombiningIterable<InType> createSplatted(
      Iterable<? extends Iterable<InType>> in,
      Comparator<InType> comparator
  )
  {
    return create(
        new MergeIterable<InType>(comparator, (Iterable<Iterable<InType>>) in),
        comparator,
        new BinaryFn<InType, InType, InType>()
        {
          @Override
          public InType apply(InType arg1, InType arg2)
          {
            if (arg1 == null) {
              return arg2;
            }
            return arg1;
          }
        }
    );
  }

  public static <InType> CombiningIterable<InType> create(
    Iterable<InType> it,
    Comparator<InType> comparator,
    BinaryFn<InType, InType, InType> fn
  )
  {
    return new CombiningIterable<InType>(it, comparator, fn);
  }

  private final Iterable<InType> it;
  private final Comparator<InType> comparator;
  private final BinaryFn<InType, InType, InType> fn;

  public CombiningIterable(
      Iterable<InType> it,
      Comparator<InType> comparator,
      BinaryFn<InType, InType, InType> fn
  )
  {
    this.it = it;
    this.comparator = comparator;
    this.fn = fn;
  }

  @Override
  public Iterator<InType> iterator()
  {
    return CombiningIterator.create(it.iterator(), comparator, fn);
  }
}
