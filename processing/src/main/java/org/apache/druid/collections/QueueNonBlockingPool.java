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

package org.apache.druid.collections;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;

/**
 * Implementation of {@link NonBlockingPool} based on a pre-created {@link BlockingQueue} that never actually blocks.
 * If the pool is empty when {@link #take()} is called, it throws {@link NoSuchElementException}.
 */
public class QueueNonBlockingPool<T> implements NonBlockingPool<T>
{
  private final BlockingQueue<T> queue;

  public QueueNonBlockingPool(final BlockingQueue<T> queue)
  {
    this.queue = queue;
  }

  @Override
  public ResourceHolder<T> take()
  {
    final T item = queue.poll();
    if (item == null) {
      throw new NoSuchElementException("No items available");
    }

    return new ReferenceCountingResourceHolder<>(item, () -> queue.add(item));
  }
}
