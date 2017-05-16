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

package io.druid.java.util.common.guava;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.java.util.common.guava.nary.BinaryTransformIterator;
import io.druid.java.util.common.guava.nary.TrinaryFn;
import io.druid.java.util.common.guava.nary.TrinaryTransformIterator;

import java.util.Iterator;

/**
 */
public class FunctionalIterator<T> implements Iterator<T>
{
  private final Iterator<T> delegate;

  public static <T> FunctionalIterator<T> create(Iterator<T> delegate)
  {
    return new FunctionalIterator<>(delegate);
  }

  public static <T> FunctionalIterator<T> fromConcatenation(Iterator<T>... toConcat)
  {
    return new FunctionalIterator<>(Iterators.concat(toConcat));
  }

  public static <T> FunctionalIterator<T> fromConcatenation(Iterator<Iterator<T>> toConcat)
  {
    return new FunctionalIterator<>(Iterators.concat(toConcat));
  }

  public FunctionalIterator(
      Iterator<T> delegate
  )
  {
    this.delegate = delegate;
  }

  @Override
  public boolean hasNext()
  {
    return delegate.hasNext();
  }

  @Override
  public T next()
  {
    return delegate.next();
  }

  @Override
  public void remove()
  {
    delegate.remove();
  }

  public <RetType> FunctionalIterator<RetType> transform(Function<T, RetType> fn)
  {
    return new FunctionalIterator<>(Iterators.transform(delegate, fn));
  }

  public <RetType> FunctionalIterator<RetType> transformCat(Function<T, Iterator<RetType>> fn)
  {
    return new FunctionalIterator<>(Iterators.concat(Iterators.transform(delegate, fn)));
  }

  public <RetType> FunctionalIterator<RetType> keep(Function<T, RetType> fn)
  {
    return new FunctionalIterator<>(Iterators.filter(Iterators.transform(delegate, fn), Predicates.notNull()));
  }

  public FunctionalIterator<T> filter(Predicate<T> pred)
  {
    return new FunctionalIterator<>(Iterators.filter(delegate, pred));
  }

  public FunctionalIterator<T> drop(int numToDrop)
  {
    return new FunctionalIterator<>(new DroppingIterator<>(delegate, numToDrop));
  }

  public FunctionalIterator<T> limit(int limit)
  {
    return new FunctionalIterator<>(Iterators.limit(delegate, limit));
  }

  public FunctionalIterator<T> concat(Iterator<T>... toConcat)
  {
    if (toConcat.length == 1) {
      return new FunctionalIterator<>(Iterators.concat(delegate, toConcat[0]));
    }
    return new FunctionalIterator<>(Iterators.concat(delegate, Iterators.concat(toConcat)));
  }

  public FunctionalIterator<T> concat(Iterator<Iterator<T>> toConcat)
  {
    return new FunctionalIterator<>(Iterators.concat(delegate, Iterators.concat(toConcat)));
  }

  public <InType, RetType> FunctionalIterator<RetType> binaryTransform(
      final Iterator<InType> otherIterator, final BinaryFn<T, InType, RetType> binaryFn
  )
  {
    return new FunctionalIterator<>(BinaryTransformIterator.create(delegate, otherIterator, binaryFn));
  }

  public <InType1, InType2, RetType> FunctionalIterator<RetType> trinaryTransform(
      final Iterator<InType1> iterator1,
      final Iterator<InType2> iterator2,
      final TrinaryFn<T, InType1, InType2, RetType> trinaryFn
  )
  {
    return new FunctionalIterator<>(TrinaryTransformIterator.create(delegate, iterator1, iterator2, trinaryFn));
  }
}
