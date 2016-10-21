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

/**
 */
public class TrinaryTransformIterable<Type1, Type2, Type3, RetType> implements Iterable<RetType>
{
  public static <Type1, Type2, Type3, RetType> TrinaryTransformIterable<Type1, Type2, Type3, RetType> create(
      Iterable<Type1> iterable1,
      Iterable<Type2> iterable2,
      Iterable<Type3> iterable3,
      TrinaryFn<Type1, Type2, Type3, RetType> fn
  )
  {
    return new TrinaryTransformIterable<>(iterable1, iterable2, iterable3, fn);
  }

  private final Iterable<Type1> iterable1;
  private final Iterable<Type2> iterable2;
  private final Iterable<Type3> iterable3;
  private final TrinaryFn<Type1, Type2, Type3, RetType> trinaryFn;

  public TrinaryTransformIterable(
      Iterable<Type1> iterable1,
      Iterable<Type2> iterable2,
      Iterable<Type3> iterable3,
      TrinaryFn<Type1, Type2, Type3, RetType> trinaryFn
  )
  {
    this.iterable1 = iterable1;
    this.iterable2 = iterable2;
    this.iterable3 = iterable3;
    this.trinaryFn = trinaryFn;
  }

  @Override
  public Iterator<RetType> iterator()
  {
    return TrinaryTransformIterator.create(iterable1.iterator(), iterable2.iterator(), iterable3.iterator(), trinaryFn);
  }
}
