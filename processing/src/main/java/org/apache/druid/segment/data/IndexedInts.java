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

import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;

import java.util.function.IntConsumer;

/**
 * Get a int an index (array or list lookup abstraction without boxing).
 *
 * Doesn't extend {@link Iterable} (or {@link it.unimi.dsi.fastutil.ints.IntIterable} to avoid accidential
 * for-each iteration with boxing.
 */
@PublicApi
public interface IndexedInts extends HotLoopCallee
{
  static IndexedInts empty()
  {
    return ArrayBasedIndexedInts.EMPTY;
  }

  @CalledFromHotLoop
  int size();
  @CalledFromHotLoop
  int get(int index);

  default void forEach(IntConsumer action)
  {
    int size = size();
    for (int i = 0; i < size; i++) {
      action.accept(get(i));
    }
  }

  @SuppressWarnings("unused") // Set up your IDE to render IndexedInts impls using this method during debug.
  default String debugToString()
  {
    StringBuilder sb = new StringBuilder("[");
    forEach(v -> sb.append(v).append(',').append(' '));
    if (sb.length() > 1) {
      sb.setLength(sb.length() - 2);
    }
    sb.append(']');
    return sb.toString();
  }
}
