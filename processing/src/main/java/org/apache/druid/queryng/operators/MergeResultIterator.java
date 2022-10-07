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

package org.apache.druid.queryng.operators;

import com.google.common.collect.Ordering;

import java.util.PriorityQueue;

public class MergeResultIterator<T> implements ResultIterator<T>
{
  public interface Input<T>
  {
    boolean next();
    T get();
    void close();
  }

  private final PriorityQueue<MergeResultIterator.Input<T>> pQueue;
  public int rowCount;

  public MergeResultIterator(
      Ordering<? super T> ordering,
      int approxInputCount
  )
  {
    this.pQueue = new PriorityQueue<>(
        approxInputCount == 0 ? 1 : approxInputCount,
        ordering.onResultOf(input -> input.get())
    );
  }

  public void add(MergeResultIterator.Input<T> input)
  {
    pQueue.add(input);
  }

  @Override
  public T next() throws ResultIterator.EofException
  {
    if (pQueue.isEmpty()) {
      throw Operators.eof();
    }
    rowCount++;
    MergeResultIterator.Input<T> entry = pQueue.remove();
    T row = entry.get();
    if (entry.next()) {
      pQueue.add(entry);
    }
    return row;
  }

  public void close(boolean cascade)
  {
    while (!pQueue.isEmpty()) {
      MergeResultIterator.Input<T> input = pQueue.remove();
      if (cascade) {
        input.close();
      }
    }
  }
}
