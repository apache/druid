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
import org.apache.druid.sql.SqlLifecycle.State;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class SqlLifecycleManagerTest
{
  private SqlLifecycleManager lifecycleManager;

  @Before
  public void setup()
  {
    lifecycleManager = new SqlLifecycleManager();
  }

  @Test
  public void testAddNonAuthorizedLifeCycle()
  {
    SqlLifecycle lifecycle = mockLifecycle(State.INITIALIZED);
    Assert.assertThrows(AssertionError.class, () -> lifecycleManager.add("sqlId", lifecycle));
  }

  @Test
  public void testAddAuthorizedLifecycle()
  {
    final String sqlId = "sqlId";
    SqlLifecycle lifecycle = mockLifecycle(State.AUTHORIZED);
    lifecycleManager.add(sqlId, lifecycle);
    Assert.assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
  }

  @Test
  public void testRemoveValidLifecycle()
  {
    final String sqlId = "sqlId";
    SqlLifecycle lifecycle = mockLifecycle(State.AUTHORIZED);
    lifecycleManager.add(sqlId, lifecycle);
    Assert.assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
    lifecycleManager.remove(sqlId, lifecycle);
    Assert.assertEquals(ImmutableList.of(), lifecycleManager.getAll(sqlId));
  }

  @Test
  public void testRemoveInvalidSqlQueryId()
  {
    final String sqlId = "sqlId";
    SqlLifecycle lifecycle = mockLifecycle(State.AUTHORIZED);
    lifecycleManager.add(sqlId, lifecycle);
    Assert.assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
    lifecycleManager.remove("invalid", lifecycle);
    Assert.assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
  }

  @Test
  public void testRemoveValidSqlQueryIdDifferntLifecycleObject()
  {
    final String sqlId = "sqlId";
    SqlLifecycle lifecycle = mockLifecycle(State.AUTHORIZED);
    lifecycleManager.add(sqlId, lifecycle);
    Assert.assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
    lifecycleManager.remove(sqlId, mockLifecycle(State.AUTHORIZED));
    Assert.assertEquals(ImmutableList.of(lifecycle), lifecycleManager.getAll(sqlId));
  }

  @Test
  public void testRemoveAllValidSqlQueryIdSubsetOfLifecycles()
  {
    final String sqlId = "sqlId";
    final List<SqlLifecycle> lifecycles = ImmutableList.of(
        mockLifecycle(State.AUTHORIZED),
        mockLifecycle(State.AUTHORIZED),
        mockLifecycle(State.AUTHORIZED)
    );
    lifecycles.forEach(lifecycle -> lifecycleManager.add(sqlId, lifecycle));
    Assert.assertEquals(lifecycles, lifecycleManager.getAll(sqlId));
    lifecycleManager.removeAll(sqlId, ImmutableList.of(lifecycles.get(0), lifecycles.get(1)));
    Assert.assertEquals(ImmutableList.of(lifecycles.get(2)), lifecycleManager.getAll(sqlId));
  }

  @Test
  public void testRemoveAllInvalidSqlQueryId()
  {
    final String sqlId = "sqlId";
    final List<SqlLifecycle> lifecycles = ImmutableList.of(
        mockLifecycle(State.AUTHORIZED),
        mockLifecycle(State.AUTHORIZED),
        mockLifecycle(State.AUTHORIZED)
    );
    lifecycles.forEach(lifecycle -> lifecycleManager.add(sqlId, lifecycle));
    Assert.assertEquals(lifecycles, lifecycleManager.getAll(sqlId));
    lifecycleManager.removeAll("invalid", ImmutableList.of(lifecycles.get(0), lifecycles.get(1)));
    Assert.assertEquals(lifecycles, lifecycleManager.getAll(sqlId));
  }

  @Test
  public void testGetAllReturnsListCopy()
  {
    final String sqlId = "sqlId";
    final List<SqlLifecycle> lifecycles = ImmutableList.of(
        mockLifecycle(State.AUTHORIZED),
        mockLifecycle(State.AUTHORIZED),
        mockLifecycle(State.AUTHORIZED)
    );
    lifecycles.forEach(lifecycle -> lifecycleManager.add(sqlId, lifecycle));
    final List<SqlLifecycle> lifecyclesFromGetAll = lifecycleManager.getAll(sqlId);
    lifecycleManager.removeAll(sqlId, lifecyclesFromGetAll);
    Assert.assertEquals(lifecycles, lifecyclesFromGetAll);
    Assert.assertTrue(lifecycleManager.getAll(sqlId).isEmpty());
  }

  private static SqlLifecycle mockLifecycle(State state)
  {
    SqlLifecycle lifecycle = Mockito.mock(SqlLifecycle.class);
    Mockito.when(lifecycle.getState()).thenReturn(state);
    return lifecycle;
  }
}
