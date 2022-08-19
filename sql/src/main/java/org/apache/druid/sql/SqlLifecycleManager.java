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

package org.apache.druid.sql;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.server.security.ResourceAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class manages only <i>authorized</i> {@link DirectStatement}s submitted via
 * HTTP, such as {@link org.apache.druid.sql.http.SqlResource}. The main use case of
 * this class is tracking running queries so that the cancel API can identify
 * the statements to cancel.
 *
 * This class is thread-safe as there are 2 or more threads that can access
 * statements at the same time for query running or query canceling.
 *
 * For managing and canceling native queries, see
 * {@link org.apache.druid.server.QueryScheduler}. As its name indicates, it
 * also performs resource scheduling for native queries based on query lanes
 * {@link org.apache.druid.server.QueryLaningStrategy}.
 *
 * @see org.apache.druid.server.QueryScheduler#cancelQuery(String)
 */
@LazySingleton
public class SqlLifecycleManager
{
  public interface Cancelable
  {
    Set<ResourceAction> resources();
    void cancel();
  }

  private final Object lock = new Object();

  @GuardedBy("lock")
  private final Map<String, List<Cancelable>> sqlLifecycles = new HashMap<>();

  public void add(String sqlQueryId, Cancelable lifecycle)
  {
    synchronized (lock) {
      sqlLifecycles.computeIfAbsent(sqlQueryId, k -> new ArrayList<>())
                   .add(lifecycle);
    }
  }

  /**
   * Removes the given lifecycle of the given query ID.
   * This method uses {@link Object#equals} to find the lifecycle matched to the given parameter.
   */
  public void remove(String sqlQueryId, Cancelable lifecycle)
  {
    synchronized (lock) {
      List<Cancelable> lifecycles = sqlLifecycles.get(sqlQueryId);
      if (lifecycles != null) {
        lifecycles.remove(lifecycle);
        if (lifecycles.isEmpty()) {
          sqlLifecycles.remove(sqlQueryId);
        }
      }
    }
  }

  /**
   * For the given sqlQueryId, this method removes all lifecycles that match to the given list of lifecycles.
   * This method uses {@link Object#equals} for matching lifecycles.
   */
  public void removeAll(String sqlQueryId, List<Cancelable> lifecyclesToRemove)
  {
    synchronized (lock) {
      List<Cancelable> lifecycles = sqlLifecycles.get(sqlQueryId);
      if (lifecycles != null) {
        lifecycles.removeAll(lifecyclesToRemove);
        if (lifecycles.isEmpty()) {
          sqlLifecycles.remove(sqlQueryId);
        }
      }
    }
  }

  /**
   * Returns a snapshot of the lifecycles for the given sqlQueryId.
   */
  public List<Cancelable> getAll(String sqlQueryId)
  {
    synchronized (lock) {
      List<Cancelable> lifecycles = sqlLifecycles.get(sqlQueryId);
      return lifecycles == null ? Collections.emptyList() : ImmutableList.copyOf(lifecycles);
    }
  }
}
