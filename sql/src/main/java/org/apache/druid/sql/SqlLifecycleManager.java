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
import org.apache.druid.sql.SqlLifecycle.State;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class manages only _authorized_ {@link SqlLifecycle}s. The main use case of this class is
 * tracking running queries so that the cancel API can identify the lifecycles to cancel.
 *
 * This class is thread-safe as there are 2 or more threads that can access lifecycles at the same time
 * for query running or query canceling.
 */
@LazySingleton
public class SqlLifecycleManager
{
  private final Object lock = new Object();

  @GuardedBy("lock")
  private final Map<String, List<SqlLifecycle>> sqlLifecycles = new HashMap<>();

  public void add(String sqlQueryId, SqlLifecycle lifecycle)
  {
    synchronized (lock) {
      assert lifecycle.getState().ordinal() == State.AUTHORIZED.ordinal();
      sqlLifecycles.computeIfAbsent(sqlQueryId, k -> new ArrayList<>())
                   .add(lifecycle);
    }
  }

  /**
   * Removes the given lifecycle of the given query ID.
   * This method uses {@link Object#equals} to find the lifecycle matched to the given parameter.
   */
  public void remove(String sqlQueryId, SqlLifecycle lifecycle)
  {
    synchronized (lock) {
      List<SqlLifecycle> lifecycles = sqlLifecycles.get(sqlQueryId);
      if (lifecycles != null) {
        lifecycles.remove(lifecycle);
        if (lifecycles.isEmpty()) {
          sqlLifecycles.remove(sqlQueryId);
        }
      }
    }
  }

  /**
   * Removes all lifecycles of the given query ID.
   * This method uses {@link Object#equals} to find the lifecycles matched to the given parameter.
   */
  public void removeAll(String sqlQueryId, List<SqlLifecycle> lifecyclesToRemove)
  {
    synchronized (lock) {
      List<SqlLifecycle> lifecycles = sqlLifecycles.get(sqlQueryId);
      if (lifecycles != null) {
        lifecycles.removeAll(lifecyclesToRemove);
        if (lifecycles.isEmpty()) {
          sqlLifecycles.remove(sqlQueryId);
        }
      }
    }
  }

  public List<SqlLifecycle> getAll(String sqlQueryId)
  {
    synchronized (lock) {
      List<SqlLifecycle> lifecycles = sqlLifecycles.get(sqlQueryId);
      return lifecycles == null ? Collections.emptyList() : ImmutableList.copyOf(lifecycles);
    }
  }
}
