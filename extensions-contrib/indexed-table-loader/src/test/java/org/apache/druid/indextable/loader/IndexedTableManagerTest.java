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

package org.apache.druid.indextable.loader;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.indextable.loader.config.IndexedTableConfig;
import org.apache.druid.indextable.loader.config.IndexedTableLoaderConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.join.table.IndexedTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

@RunWith(MockitoJUnitRunner.class)
public class IndexedTableManagerTest
{
  private static final String TABLE_1 = "TABLE_1";
  private static final String TABLE_2 = "TABLE_2";

  @Mock
  private IndexedTableConfig tableConfig1;
  @Mock
  private IndexedTableConfig tableConfig2;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private IndexedTableLoaderConfig config;
  private Map<String, IndexedTableConfig> indexedTableLoaders;
  @Mock
  private ExecutorService executorService;
  @Mock
  private LifecycleLock lifecycleLock;
  private ConcurrentMap<String, IndexedTable> indexedTables;

  private IndexedTableManager target;

  @Before
  public void setUp()
  {
    indexedTables = new ConcurrentHashMap<>();

    Mockito.when(lifecycleLock.canStart()).thenReturn(true);
    Mockito.when(lifecycleLock.canStop()).thenReturn(true);
    indexedTableLoaders = ImmutableMap.of(TABLE_1, tableConfig1, TABLE_2, tableConfig2);
    target = new IndexedTableManager(indexedTableLoaders, executorService, lifecycleLock, indexedTables);
  }

  @Test(expected = ISE.class)
  public void testStartLifecycleCanNotStartShouldThrowISE()
  {
    Mockito.doReturn(false).when(lifecycleLock).canStart();
    target.start();
  }

  @Test(expected = RuntimeException.class)
  public void testStartWithLoadersThatFailToSubmitShouldExitAndRethrowException()
  {
    Mockito.doThrow(RuntimeException.class).when(executorService).submit(ArgumentMatchers.any(Runnable.class));
    try {
      target.start();
    }
    finally {
      Mockito.verify(lifecycleLock).exitStart();
    }
  }

  @Test
  public void testStartWithLoadersShouldSubmitAndExit()
  {
    target.start();
    Mockito.verify(executorService, Mockito.times(indexedTableLoaders.size()))
           .submit(ArgumentMatchers.any(Runnable.class));
    Mockito.verify(lifecycleLock).exitStart();
  }

  @Test(expected = ISE.class)
  public void testStopLifecycleCanNotStopShouldThrowISE()
  {
    Mockito.doReturn(false).when(lifecycleLock).canStop();
    target.stop();
  }

  @Test(expected = RuntimeException.class)
  public void testStopWithShutdownThatFailsShouldExitAndRethrowException()
  {
    Mockito.doThrow(RuntimeException.class).when(executorService).shutdownNow();
    try {
      target.stop();
    }
    finally {
      Mockito.verify(lifecycleLock).exitStop();
    }
  }

  @Test
  public void testStopShouldShutdownAndExit()
  {
    target.stop();
    Mockito.verify(executorService).shutdownNow();
    Mockito.verify(lifecycleLock).exitStop();
  }
}
