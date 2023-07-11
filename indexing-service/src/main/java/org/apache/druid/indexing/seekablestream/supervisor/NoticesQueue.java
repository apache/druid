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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.LinkedHashSet;

/**
 * Queue that de-duplicates items on addition using {@link Object#equals}.
 */
public class NoticesQueue<T>
{
  @GuardedBy("this")
  private final LinkedHashSet<T> queue = new LinkedHashSet<>();

  /**
   * Adds an item. Throws {@link NullPointerException} if the item is null.
   */
  public void add(final T item)
  {
    Preconditions.checkNotNull(item, "item");

    synchronized (this) {
      queue.add(item);
      this.notifyAll();
    }
  }

  /**
   * Retrieves the head of the queue (eldest item). Returns null if the queue is empty and the timeout has elapsed.
   */
  @Nullable
  public T poll(final long timeoutMillis) throws InterruptedException
  {
    synchronized (this) {
      final long timeoutAt = System.currentTimeMillis() + timeoutMillis;

      long waitMillis = timeoutMillis;
      while (queue.isEmpty() && waitMillis > 0) {
        wait(waitMillis);
        waitMillis = timeoutAt - System.currentTimeMillis();
      }

      final Iterator<T> it = queue.iterator();
      if (it.hasNext()) {
        final T item = it.next();
        it.remove();
        return item;
      } else {
        return null;
      }
    }
  }

  public int size()
  {
    synchronized (this) {
      return queue.size();
    }
  }
}
