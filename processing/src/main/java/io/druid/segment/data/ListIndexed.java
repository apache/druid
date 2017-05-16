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

import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import java.util.Iterator;
import java.util.List;

/**
 */
public class ListIndexed<T> implements Indexed<T>
{
  private final List<T> baseList;
  private final Class<? extends T> clazz;

  public ListIndexed(
      List<T> baseList,
      Class<? extends T> clazz
  )
  {
    this.baseList = baseList;
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
    return baseList.size();
  }

  @Override
  public T get(int index)
  {
    return baseList.get(index);
  }

  @Override
  public int indexOf(T value)
  {
    return baseList.indexOf(value);
  }

  @Override
  public Iterator<T> iterator()
  {
    return baseList.iterator();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("baseList", baseList);
  }
}
