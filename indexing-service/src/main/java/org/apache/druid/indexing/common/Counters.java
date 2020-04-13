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

package org.apache.druid.indexing.common;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class Counters
{
  public static <K> int getAndIncrementInt(ConcurrentHashMap<K, AtomicInteger> counters, K key)
  {
    // get() before computeIfAbsent() is an optimization to avoid locking in computeIfAbsent() if not needed.
    // See https://github.com/apache/druid/pull/6898#discussion_r251384586.
    AtomicInteger counter = counters.get(key);
    if (counter == null) {
      counter = counters.computeIfAbsent(key, k -> new AtomicInteger());
    }
    return counter.getAndIncrement();
  }

  public static <K> long incrementAndGetLong(ConcurrentHashMap<K, AtomicLong> counters, K key)
  {
    // get() before computeIfAbsent() is an optimization to avoid locking in computeIfAbsent() if not needed.
    // See https://github.com/apache/druid/pull/6898#discussion_r251384586.
    AtomicLong counter = counters.get(key);
    if (counter == null) {
      counter = counters.computeIfAbsent(key, k -> new AtomicLong());
    }
    return counter.incrementAndGet();
  }

  private Counters()
  {
  }
}
