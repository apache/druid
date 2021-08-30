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

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.guice.LazySingleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

// TODO: name test
@LazySingleton
public class SqlLifecycleManager
{
  private final Object lock = new Object();

  @GuardedBy("lock")
  private final Map<String, List<SqlLifecycle>> sqlLifecycles = new HashMap<>();

  public void add(String sqlQueryId, SqlLifecycle lifecycle)
  {
    synchronized (lock) {
      sqlLifecycles.computeIfAbsent(sqlQueryId, k -> new ArrayList<>())
                   .add(lifecycle);
    }
  }

  // javadoc
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

  // javadoc
  public List<SqlLifecycle> removeAll(String sqlQueryId, Predicate<SqlLifecycle> filter)
  {
    final List<SqlLifecycle> authorizedLifecycles = new ArrayList<>();
    synchronized (lock) {
      List<SqlLifecycle> lifecycles = sqlLifecycles.get(sqlQueryId);
      if (lifecycles == null) {
        return authorizedLifecycles;
      }
      Iterator<SqlLifecycle> iterator = lifecycles.iterator();
      while (iterator.hasNext()) {
        SqlLifecycle lifecycle = iterator.next();
        if (filter.test(lifecycle)) {
          iterator.remove();
          authorizedLifecycles.add(lifecycle);
        }
      }
      if (lifecycles.isEmpty()) {
        sqlLifecycles.remove(sqlQueryId);
      }
    }
    return authorizedLifecycles;
  }

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
      return lifecycles == null ? Collections.emptyList() : lifecycles;
    }
  }
}
