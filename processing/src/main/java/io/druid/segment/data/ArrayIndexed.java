/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.data;

import java.util.Arrays;
import java.util.Iterator;

/**
 */
public class ArrayIndexed<T> implements Indexed<T>
{
  private final T[] baseArray;
  private final Class<? extends T> clazz;

  public ArrayIndexed(
      T[] baseArray,
      Class<? extends T> clazz
  )
  {
    this.baseArray = baseArray;
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
    return Arrays.asList(baseArray).indexOf(value);
  }

  @Override
  public Iterator<T> iterator()
  {
    return Arrays.asList(baseArray).iterator();
  }
}
