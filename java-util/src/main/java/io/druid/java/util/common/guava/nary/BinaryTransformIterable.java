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
public class BinaryTransformIterable<Type1, Type2, RetType> implements Iterable<RetType>
{
  public static <Type1, Type2, RetType> BinaryTransformIterable<Type1, Type2, RetType> create(
      Iterable<Type1> lhs,
      Iterable<Type2> rhs,
      BinaryFn<Type1, Type2, RetType> fn
  )
  {
    return new BinaryTransformIterable<>(lhs, rhs, fn);
  }

  private final Iterable<Type1> lhs;
  private final Iterable<Type2> rhs;
  private final BinaryFn<Type1, Type2, RetType> binaryFn;

  public BinaryTransformIterable(
      Iterable<Type1> lhs,
      Iterable<Type2> rhs,
      BinaryFn<Type1, Type2, RetType> binaryFn
  )
  {
    this.lhs = lhs;
    this.rhs = rhs;
    this.binaryFn = binaryFn;
  }

  @Override
  public Iterator<RetType> iterator()
  {
    return BinaryTransformIterator.create(lhs.iterator(), rhs.iterator(), binaryFn);
  }
}
