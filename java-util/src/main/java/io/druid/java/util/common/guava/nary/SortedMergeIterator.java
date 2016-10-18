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

package io.druid.java.util.common.guava.nary;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A SortedMergeIterator is an Iterator that combines two other Iterators into one.
 *
 * It assumes that the two Iterators are in sorted order and walks through them, passing their values to the
 * BinaryFn in sorted order.  If a value appears in one Iterator and not in the other, e.g. if the lhs has a value "1"
 * and the rhs does not, the BinaryFn will be called with "1" for first argument and null for the second argument.
 * Thus, the BinaryFn implementation *must* be aware of nulls.
 *
 */
public class SortedMergeIterator<InType, OutType> implements Iterator<OutType>
{
  public static <InType, OutType> SortedMergeIterator<InType, OutType> create(
      Iterator<InType> lhs,
      Iterator<InType> rhs,
      Comparator<InType> comparator,
      BinaryFn<InType, InType, OutType> fn
  )
  {
    return new SortedMergeIterator<>(lhs, rhs, comparator, fn);
  }

  private final PeekingIterator<InType> lhs;
  private final PeekingIterator<InType> rhs;
  private final Comparator<InType> comparator;
  private final BinaryFn<InType, InType, OutType> fn;

  public SortedMergeIterator(
      Iterator<InType> lhs,
      Iterator<InType> rhs,
      Comparator<InType> comparator,
      BinaryFn<InType, InType, OutType> fn
  )
  {
    this.lhs = Iterators.peekingIterator(lhs);
    this.rhs = Iterators.peekingIterator(rhs);
    this.comparator = comparator;
    this.fn = fn;
  }

  @Override
  public boolean hasNext()
  {
    return lhs.hasNext() || rhs.hasNext();
  }

  @Override
  public OutType next()
  {
    if (! hasNext()) {
      throw new NoSuchElementException();
    }

    if (! lhs.hasNext()) {
      return fn.apply(null, rhs.next());
    }
    if (! rhs.hasNext()) {
      return fn.apply(lhs.next(), null);
    }

    int compared = comparator.compare(lhs.peek(), rhs.peek());

    if (compared < 0) {
      return fn.apply(lhs.next(), null);
    }
    if (compared == 0) {
      return fn.apply(lhs.next(), rhs.next());
    }

    return fn.apply(null, rhs.next());
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
