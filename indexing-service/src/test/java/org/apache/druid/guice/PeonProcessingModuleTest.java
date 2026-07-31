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

package org.apache.druid.guice;

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DummyBlockingPool;
import org.apache.druid.collections.DummyNonBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ExecutorServiceMonitor;
import org.apache.druid.query.NoopQueryProcessingPool;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.utils.RuntimeInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

public class PeonProcessingModuleTest
{
  private final PeonProcessingModule module = new PeonProcessingModule();

  @Test
  public void testConfigBuilder()
  {
    final PeonProcessingModule.Config config = new PeonProcessingModule.Config()
        .withProcessingBuffers()
        .withProcessingThreads()
        .withMergeBuffers();
    Assert.assertTrue(config.hasProcessingBuffers());
    Assert.assertTrue(config.hasProcessingThreads());
    Assert.assertTrue(config.hasMergeBuffers());
  }

  @Test
  public void testGetProcessingExecutorPool_whenNotNeeded()
  {
    final Task task = NoopTask.create();
    final DruidProcessingConfig config = Mockito.mock(DruidProcessingConfig.class);
    Mockito.when(config.isNumThreadsConfigured()).thenReturn(false);

    final QueryProcessingPool pool = module.getProcessingExecutorPool(
        task,
        config,
        new ExecutorServiceMonitor(),
        new Lifecycle()
    );
    Assert.assertSame(NoopQueryProcessingPool.instance(), pool);
  }

  @Test
  public void testGetProcessingExecutorPool_whenNotNeeded_andThreadsConfigured()
  {
    final Task task = NoopTask.create();
    final DruidProcessingConfig config = Mockito.mock(DruidProcessingConfig.class);
    Mockito.when(config.isNumThreadsConfigured()).thenReturn(true);
    Mockito.when(config.getNumThreads()).thenReturn(2);

    final QueryProcessingPool pool = module.getProcessingExecutorPool(
        task,
        config,
        new ExecutorServiceMonitor(),
        new Lifecycle()
    );
    Assert.assertSame(NoopQueryProcessingPool.instance(), pool);
  }

  @Test
  public void testGetIntermediateResultsPool_whenNotNeeded()
  {
    final Task task = NoopTask.create();
    final DruidProcessingConfig config = Mockito.mock(DruidProcessingConfig.class);
    final RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);

    final NonBlockingPool<ByteBuffer> pool = module.getIntermediateResultsPool(task, config, runtimeInfo);
    Assert.assertSame(DummyNonBlockingPool.instance(), pool);
  }

  @Test
  public void testGetMergeBufferPool_whenNotNeeded()
  {
    final Task task = NoopTask.create();
    final DruidProcessingConfig config = Mockito.mock(DruidProcessingConfig.class);
    Mockito.when(config.isNumMergeBuffersConfigured()).thenReturn(false);
    final RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);

    final BlockingPool<ByteBuffer> pool = module.getMergeBufferPool(task, config, runtimeInfo);
    Assert.assertSame(DummyBlockingPool.instance(), pool);
  }

  @Test
  public void testGetMergeBufferPool_whenNotNeeded_andMergeBuffersConfigured()
  {
    // Covers the inner "if (config.isNumMergeBuffersConfigured())" warning path.
    final Task task = NoopTask.create();
    final DruidProcessingConfig config = Mockito.mock(DruidProcessingConfig.class);
    Mockito.when(config.isNumMergeBuffersConfigured()).thenReturn(true);
    Mockito.when(config.getNumMergeBuffers()).thenReturn(2);
    final RuntimeInfo runtimeInfo = Mockito.mock(RuntimeInfo.class);

    final BlockingPool<ByteBuffer> pool = module.getMergeBufferPool(task, config, runtimeInfo);
    Assert.assertSame(DummyBlockingPool.instance(), pool);
  }
}
