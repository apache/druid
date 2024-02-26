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
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.SqlLifecycleManager.Cancelable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlLifecycleManagerTest
{
  private SqlLifecycleManager lifecycleManager;

  @BeforeEach
  void setup()
  {
    lifecycleManager = new SqlLifecycleManager();
  }

  @Test
  void addAuthorizedLifecycle()
  {
    final String sqlId = "sqlId";
    Cancelable lifecycle = mockLifecycle();
    lifecycleManager.add(sqlId, lifecycle);
    assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
  }

  @Test
  void removeValidLifecycle()
  {
    final String sqlId = "sqlId";
    Cancelable lifecycle = mockLifecycle();
    lifecycleManager.add(sqlId, lifecycle);
    assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
    lifecycleManager.remove(sqlId, lifecycle);
    assertEquals(ImmutableList.of(), lifecycleManager.getAll(sqlId));
  }

  @Test
  void removeInvalidSqlQueryId()
  {
    final String sqlId = "sqlId";
    Cancelable lifecycle = mockLifecycle();
    lifecycleManager.add(sqlId, lifecycle);
    assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
    lifecycleManager.remove("invalid", lifecycle);
    assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
  }

  @Test
  void removeValidSqlQueryIdDifferntLifecycleObject()
  {
    final String sqlId = "sqlId";
    Cancelable lifecycle = mockLifecycle();
    lifecycleManager.add(sqlId, lifecycle);
    assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
    lifecycleManager.remove(sqlId, mockLifecycle());
    assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
  }

  @Test
  void removeAllValidSqlQueryIdSubsetOfLifecycles()
  {
    final String sqlId = "sqlId";
    final List<Cancelable> lifecycles = ImmutableList.of(
        mockLifecycle(),
        mockLifecycle(),
        mockLifecycle()
    );
    lifecycles.forEach(lifecycle -> lifecycleManager.add(sqlId, lifecycle));
    assertEquals(lifecycles, lifecycleManager.getAll(sqlId));
    lifecycleManager.removeAll(sqlId, ImmutableList.of(lifecycles.get(0), lifecycles.get(1)));
    assertEquals(ImmutableList.of(lifecycles.get(2)), lifecycleManager.getAll(sqlId));
  }

  @Test
  void removeAllInvalidSqlQueryId()
  {
    final String sqlId = "sqlId";
    final List<Cancelable> lifecycles = ImmutableList.of(
        mockLifecycle(),
        mockLifecycle(),
        mockLifecycle()
    );
    lifecycles.forEach(lifecycle -> lifecycleManager.add(sqlId, lifecycle));
    assertEquals(lifecycles, lifecycleManager.getAll(sqlId));
    lifecycleManager.removeAll("invalid", ImmutableList.of(lifecycles.get(0), lifecycles.get(1)));
    assertEquals(lifecycles, lifecycleManager.getAll(sqlId));
  }

  @Test
  void getAllReturnsListCopy()
  {
    final String sqlId = "sqlId";
    final List<Cancelable> lifecycles = ImmutableList.of(
        mockLifecycle(),
        mockLifecycle(),
        mockLifecycle()
    );
    lifecycles.forEach(lifecycle -> lifecycleManager.add(sqlId, lifecycle));
    final List<Cancelable> lifecyclesFromGetAll = lifecycleManager.getAll(sqlId);
    lifecycleManager.removeAll(sqlId, lifecyclesFromGetAll);
    assertEquals(lifecycles, lifecyclesFromGetAll);
    assertTrue(lifecycleManager.getAll(sqlId).isEmpty());
  }

  private static Cancelable mockLifecycle()
  {
    return new MockCancellable();
  }

  private static class MockCancellable implements Cancelable
  {
    @Override
    public Set<ResourceAction> resources()
    {
      return Collections.emptySet();
    }

    @Override
    public void cancel()
    {
    }
  }
}
