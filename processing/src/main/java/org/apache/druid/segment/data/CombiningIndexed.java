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

import com.google.common.collect.Iterators;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

/**
 * An Indexed that combines multiple underlying Indexed objects.
 * Callers can use this class to access those underlying Indexed objects in a way as if they were concatenated.
 * For example, suppose you have a CombiningIndexed that has 2 underlying Indexeds. Those underlying Indexeds
 * have the size of 3 and 2, respectively. You can read the items in the first Indexed using an index
 * in a range of 0 - 2. For the second Indexed, you can use an index in a range of 3 - 4 to read items in it.
 *
 * NOTE: this class assumes that the underlying Indexeds have distinct values. Deduplication should be done
 * outside this class if needed.
 */
public class CombiningIndexed<T> implements Indexed<T>
{
  private final List<Indexed<T>> delegates;

  public CombiningIndexed(List<Indexed<T>> delegates)
  {
    this.delegates = delegates;
  }

  @Override
  public int size()
  {
    return delegates.stream().mapToInt(Indexed::size).sum();
  }

  @Nullable
  @Override
  public T get(int index)
  {
    int startIndex = 0;
    for (Indexed<T> indexed : delegates) {
      if (index >= startIndex && index < startIndex + indexed.size()) {
        return indexed.get(index - startIndex);
      }
      startIndex += indexed.size();
    }

    return null;
  }

  @Override
  public int indexOf(@Nullable T value)
  {
    int startIndex = 0;
    for (Indexed<T> indexed : delegates) {
      final int index = indexed.indexOf(value);
      if (index >= 0) {
        return startIndex + index;
      }
      startIndex += indexed.size();
    }
    return -1;
  }

  @Override
  public Iterator<T> iterator()
  {
    return Iterators.concat(delegates.stream().map(Indexed::iterator).iterator());
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    delegates.forEach(indexed -> indexed.inspectRuntimeShape(inspector));
  }
}
