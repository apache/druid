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

package io.druid.java.util.common.guava;

import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 */
public class MergeIterable<T> implements Iterable<T>
{
  private final Comparator<T> comparator;
  private final Iterable<Iterable<T>> baseIterables;

  public MergeIterable(
      Comparator<T> comparator,
      Iterable<Iterable<T>> baseIterables
  )
  {
    this.comparator = comparator;
    this.baseIterables = baseIterables;
  }

  @Override
  public Iterator<T> iterator()
  {
    List<Iterator<T>> iterators = Lists.newArrayList();
    for (Iterable<T> baseIterable : baseIterables) {
      iterators.add(baseIterable.iterator());
    }

    return new MergeIterator<>(comparator, iterators);
  }
}
