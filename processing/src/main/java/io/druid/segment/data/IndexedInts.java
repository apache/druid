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

import io.druid.guice.annotations.PublicApi;
import io.druid.query.monomorphicprocessing.CalledFromHotLoop;
import io.druid.query.monomorphicprocessing.HotLoopCallee;

import java.io.Closeable;
import java.util.function.IntConsumer;

/**
 * Get a int an index (array or list lookup abstraction without boxing).
 *
 * Doesn't extend {@link Iterable} (or {@link it.unimi.dsi.fastutil.ints.IntIterable} to avoid accidential
 * for-each iteration with boxing.
 */
@PublicApi
public interface IndexedInts extends Closeable, HotLoopCallee
{
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
}
