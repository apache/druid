/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.resources;

import com.metamx.common.ISE;
import io.druid.indexing.overlord.config.TierLocalTaskRunnerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class DeadhandMonitorTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File deadhandFile;
  private static final long timeout = 100L;
  DeadhandMonitor monitor;
  private final AtomicLong exitCalled = new AtomicLong(0L);
  private final CountDownLatch exitCalledLatch = new CountDownLatch(1);

  @Before
  public void setUp() throws IOException
  {
    deadhandFile = temporaryFolder.newFile();
    exitCalled.set(0L);
    monitor = new DeadhandMonitor(
        new DeadhandResource(),
        new TierLocalTaskRunnerConfig()
        {
          @Override
          public long getHeartbeatTimeLimit()
          {
            return timeout;
          }
        },
        deadhandFile
    )
    {
      @Override
      void exit()
      {
        exitCalled.incrementAndGet();
        exitCalledLatch.countDown();
      }
    };
  }

  @After
  public void tearDown()
  {
    monitor.stop();
  }

  @Test
  public void testStartStop()
  {
    Assert.assertEquals(0, exitCalled.get());
    monitor.start();
    Assert.assertEquals(0, exitCalled.get());
    monitor.stop();
    Assert.assertEquals(0, exitCalled.get());
  }

  @Test(expected = ISE.class)
  public void testStartAfterStop()
  {
    Assert.assertEquals(0, exitCalled.get());
    monitor.start();
    Assert.assertEquals(0, exitCalled.get());
    monitor.stop();
    Assert.assertEquals(0, exitCalled.get());
    monitor.start();
  }

  @Test
  public void testWackyStartStop()
  {
    Assert.assertEquals(0, exitCalled.get());
    monitor.start();
    monitor.start();
    monitor.start();
    monitor.start();
    monitor.start();
    monitor.start();
    monitor.start();
    Assert.assertEquals(0, exitCalled.get());
    monitor.stop();
    monitor.stop();
    monitor.stop();
    monitor.stop();
    monitor.stop();
    Assert.assertEquals(0, exitCalled.get());
  }

  @Test
  public void testTimeout() throws InterruptedException
  {
    Assert.assertEquals(0, exitCalled.get());
    Thread.sleep(timeout * 2);
    Assert.assertEquals(0, exitCalled.get());
    monitor.start();
    exitCalledLatch.await(timeout * 3, TimeUnit.MILLISECONDS);
    Assert.assertNotEquals(0, exitCalled.get());
  }

  @Test
  public void testDeadhandFile() throws InterruptedException
  {
    Assert.assertTrue(deadhandFile.delete());
    Assert.assertEquals(0, exitCalled.get());
    Thread.sleep(timeout * 2);
    Assert.assertEquals(0, exitCalled.get());
    monitor.start();
    exitCalledLatch.await(timeout * 3, TimeUnit.MILLISECONDS);
    Assert.assertNotEquals(0, exitCalled.get());
  }
}
