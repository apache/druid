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
import java.io.Closeable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class SparseArrayIndexed<T> implements Indexed<T>, Closeable
{
  private final GenericIndexed<T> delegate;
  private final Object[] cacheValues;
  private final int indexGranularity;

  public SparseArrayIndexed(GenericIndexed<T> delegate, final int indexGranularity)
  {
    this.delegate = delegate;

    this.indexGranularity = indexGranularity;
    final int size = delegate.size();
    final int len = (size + indexGranularity - 1) / indexGranularity;

    cacheValues = new Object[len];
    for (int i = 0; i < cacheValues.length; i++) {
      cacheValues[i] = delegate.get(getValueIndex(i));
    }
  }

  @Override
  public int size()
  {
    return delegate.size();
  }

  @Override
  public T get(int index)
  {
    return delegate.get(index);
  }

  @Override
  public int indexOf(@Nullable T value)
  {
    int index = Arrays.binarySearch(cacheValues, value, (Comparator) delegate.getStrategy());
    if (index >= 0) {
      return getValueIndex(index);
    }
    int insertPos = -index - 1;
    if (insertPos == 0) {
      return -1;
    }

    int maxIndex = Math.min(size() - 1, getValueIndex(insertPos));
    int minIndex = Math.max(0, getValueIndex(insertPos - 1));
    return delegate.indexOf(value, minIndex, maxIndex);
  }

  private int getValueIndex(int cacheIndex)
  {
    return indexGranularity * cacheIndex;
  }

  @Override
  public Iterator<T> iterator()
  {
    return delegate.iterator();
  }

  @Override
  public void close()
  {
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("cachedValues", cacheValues.length);
    inspector.visit("delegate", delegate);
  }
}
