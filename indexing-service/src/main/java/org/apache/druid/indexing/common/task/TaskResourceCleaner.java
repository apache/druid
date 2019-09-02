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

package org.apache.druid.indexing.common.task;

import org.apache.druid.indexing.common.config.TaskConfig;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

/**
 * Executes all registered {@link Consumer}s in LIFO order.
 * Similar to {@link org.apache.druid.java.util.common.io.Closer}, but this class is tweaked to be used in
 * {@link Task#stopGracefully(TaskConfig)}.
 */
public class TaskResourceCleaner
{
  private final Deque<Consumer<TaskConfig>> stack = new ArrayDeque<>(4);

  public void register(Consumer<TaskConfig> cleaner)
  {
    stack.addFirst(cleaner);
  }

  public void clean(TaskConfig config)
  {
    Throwable throwable = null;

    // Clean up in LIFO order
    while (!stack.isEmpty()) {
      final Consumer<TaskConfig> cleaner = stack.removeFirst();
      try {
        cleaner.accept(config);
      }
      catch (Throwable t) {
        if (throwable == null) {
          throwable = t;
        } else {
          suppress(throwable, t);
        }
      }
    }

    if (throwable != null) {
      throw new RuntimeException(throwable);
    }
  }

  private void suppress(Throwable thrown, Throwable suppressed)
  {
    //noinspection ObjectEquality
    if (thrown != suppressed) {
      thrown.addSuppressed(suppressed);
    }
  }
}
