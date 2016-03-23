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

package io.druid.segment.data;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

/**
 */
public class ArrayIndexed<T> implements Indexed<T>
{
  private final T[] baseArray;
  private final Comparator<T> comparator;
  private final Class<? extends T> clazz;

  public ArrayIndexed(
      T[] baseArray,
      Class<? extends T> clazz
  )
  {
    this.baseArray = baseArray;
    this.comparator = null;
    this.clazz = clazz;
  }

  public ArrayIndexed(
      T[] baseArray,
      Comparator<T> comparator,
      Class<? extends T> clazz
  )
  {
    this.baseArray = baseArray;
    this.comparator = comparator;
    this.clazz = clazz;
  }

  @Override
  public Class<? extends T> getClazz()
  {
    return clazz;
  }

  @Override
  public int size()
  {
    return baseArray.length;
  }

  @Override
  public T get(int index)
  {
    return baseArray[index];
  }

  @Override
  public int indexOf(T value)
  {
    return comparator == null
           ? Arrays.binarySearch(baseArray, value)
           : Arrays.binarySearch(baseArray, value, comparator);
  }

  @Override
  public Iterator<T> iterator()
  {
    return Arrays.asList(baseArray).iterator();
  }
}
