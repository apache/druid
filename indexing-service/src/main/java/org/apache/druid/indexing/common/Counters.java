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

import com.google.common.util.concurrent.AtomicDouble;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;

public class Counters
{
  private final ConcurrentMap<String, AtomicInteger> intCounters = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicDouble> doubleCounters = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicReference> objectCounters = new ConcurrentHashMap<>();

  public int increment(String key, int val)
  {
    return intCounters.computeIfAbsent(key, k -> new AtomicInteger()).addAndGet(val);
  }

  public double increment(String key, double val)
  {
    return doubleCounters.computeIfAbsent(key, k -> new AtomicDouble()).addAndGet(val);
  }

  public Object increment(String key, Object obj, BinaryOperator mergeFunction)
  {
    return objectCounters.computeIfAbsent(key, k -> new AtomicReference()).accumulateAndGet(obj, mergeFunction);
  }

  @Nullable
  public Integer getIntCounter(String key)
  {
    final AtomicInteger atomicInteger = intCounters.get(key);
    return atomicInteger == null ? null : atomicInteger.get();
  }

  @Nullable
  public Double getDoubleCounter(String key)
  {
    final AtomicDouble atomicDouble = doubleCounters.get(key);
    return atomicDouble == null ? null : atomicDouble.get();
  }

  @Nullable
  public Object getObjectCounter(String key)
  {
    final AtomicReference atomicReference = objectCounters.get(key);
    return atomicReference == null ? null : atomicReference.get();
  }
}
