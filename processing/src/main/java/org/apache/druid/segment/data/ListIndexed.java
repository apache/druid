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

package org.apache.druid.segment.data;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 */
public class ListIndexed<T> implements Indexed<T>
{
  private final List<T> baseList;

  public ListIndexed(List<T> baseList)
  {
    this.baseList = baseList;
  }

  @SafeVarargs
  public ListIndexed(T... values)
  {
    this(Arrays.asList(values));
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
  public int indexOf(@Nullable T value)
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
