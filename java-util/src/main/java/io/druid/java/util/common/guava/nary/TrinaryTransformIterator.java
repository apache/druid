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
public class TrinaryTransformIterator<Type1, Type2, Type3, RetType> implements Iterator<RetType>
{
  public static <Type1, Type2, Type3, RetType> TrinaryTransformIterator<Type1, Type2, Type3, RetType> create(
      Iterator<Type1> iterator1,
      Iterator<Type2> iterator2,
      Iterator<Type3> iterator3,
      TrinaryFn<Type1, Type2, Type3, RetType> fn
  )
  {
    return new TrinaryTransformIterator<>(iterator1, iterator2, iterator3, fn);
  }

  private final Iterator<Type1> iterator1;
  private final Iterator<Type2> iterator2;
  private final Iterator<Type3> iterator3;
  private final TrinaryFn<Type1, Type2, Type3, RetType> trinaryFn;

  public TrinaryTransformIterator(
      Iterator<Type1> iterator1,
      Iterator<Type2> iterator2,
      Iterator<Type3> iterator3,
      TrinaryFn<Type1, Type2, Type3, RetType> trinaryFn
  )
  {
    this.iterator1 = iterator1;
    this.iterator2 = iterator2;
    this.iterator3 = iterator3;
    this.trinaryFn = trinaryFn;
  }

  @Override
  public boolean hasNext()
  {
    return iterator1.hasNext() || iterator2.hasNext() || iterator3.hasNext();
  }

  @Override
  public RetType next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    return trinaryFn.apply(
        iterator1.hasNext() ? iterator1.next() : null,
        iterator2.hasNext() ? iterator2.next() : null,
        iterator3.hasNext() ? iterator3.next() : null
    );
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
