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

package org.apache.druid.java.util.common.guava;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

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

}
