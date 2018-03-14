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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 */
public class BinaryTransformIterator<Type1, Type2, RetType> implements Iterator<RetType>
{
  public static <Type1, Type2, RetType> BinaryTransformIterator<Type1, Type2, RetType> create(
      Iterator<Type1> lhs,
      Iterator<Type2> rhs,
      BinaryFn<Type1, Type2, RetType> fn
  )
  {
    return new BinaryTransformIterator<>(lhs, rhs, fn);
  }

  private final Iterator<Type1> lhsIter;
  private final Iterator<Type2> rhsIter;
  private final BinaryFn<Type1, Type2, RetType> binaryFn;

  public BinaryTransformIterator(Iterator<Type1> lhsIter, Iterator<Type2> rhsIter, BinaryFn<Type1, Type2, RetType> binaryFn)
  {
    this.lhsIter = lhsIter;
    this.rhsIter = rhsIter;
    this.binaryFn = binaryFn;
  }

  @Override
  public boolean hasNext()
  {
    return lhsIter.hasNext() || rhsIter.hasNext();
  }

  @Override
  public RetType next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    return binaryFn.apply(
        lhsIter.hasNext() ? lhsIter.next() : null,
        rhsIter.hasNext() ? rhsIter.next() : null
    );
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
