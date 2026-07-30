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

package org.apache.druid.indexing.worker.executor;

import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class ExecutorLifecycleTest
{
  @Test
  public void testAcquireTaskFileLockRetriesUntilSuccess() throws Exception
  {
    final ExecutorLifecycle lifecycle = createLifecycle(Period.seconds(1));
    final FileChannel channel = Mockito.mock(FileChannel.class);
    final FileLock lock = Mockito.mock(FileLock.class);
    Mockito.when(channel.tryLock()).thenReturn(null, lock);

    Assert.assertSame(lock, lifecycle.acquireTaskFileLock(channel, new File("task.lock")));
    Mockito.verify(channel, Mockito.times(2)).tryLock();

    lifecycle.stop();
    Mockito.verify(lock).release();
  }

  @Test
  public void testAcquireTaskFileLockTimesOut() throws Exception
  {
    final ExecutorLifecycle lifecycle = createLifecycle(Period.ZERO);
    final FileChannel channel = Mockito.mock(FileChannel.class);
    final File lockFile = new File("task.lock");

    final ISE exception = Assert.assertThrows(
        ISE.class,
        () -> lifecycle.acquireTaskFileLock(channel, lockFile)
    );

    Assert.assertEquals("Could not acquire lock file[task.lock] within 0ms.", exception.getMessage());
    Mockito.verifyNoInteractions(channel);
    lifecycle.stop();
  }

  private static ExecutorLifecycle createLifecycle(Period directoryLockTimeout)
  {
    final TaskConfig taskConfig = Mockito.mock(TaskConfig.class);
    Mockito.when(taskConfig.getDirectoryLockTimeout()).thenReturn(directoryLockTimeout);
    return new ExecutorLifecycle(
        new ExecutorLifecycleConfig(),
        taskConfig,
        Mockito.mock(TaskActionClientFactory.class),
        Mockito.mock(TaskRunner.class),
        new DefaultObjectMapper()
    );
  }
}
