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

package io.druid.indexing.overlord;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import io.druid.concurrent.Execs;
import io.druid.server.DruidNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PortWriterTest
{
  private static final DruidNode me = new DruidNode("testService", "localhost", 8080);
  private final File portFile = new File(TierLocalTaskRunner.PORT_FILE_NAME);

  @Before
  public void setUp()
  {
    deletePortFile();
  }

  @After
  public void tearDown()
  {
    deletePortFile();
  }

  private void deletePortFile()
  {
    Assert.assertTrue("cannot cleanup port file", (!portFile.exists() || portFile.delete()) || (!portFile.exists()));
  }

  @Test
  public void testPortWriting() throws IOException
  {
    Assert.assertFalse(portFile.exists());
    final PortWriter writer = new PortWriter(me);
    writer.start();
    Assert.assertTrue(portFile.exists());
    final String portString;
    try (final FileInputStream fis = new FileInputStream(portFile)) {
      try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        ByteStreams.copy(fis, baos);
        portString = StringUtils.fromUtf8(baos.toByteArray());
      }
    }
    final int port = Integer.parseInt(portString);
    Assert.assertEquals(me.getPort(), port);
    writer.stop();
    Assert.assertFalse(portFile.exists());
  }

  @Test(expected = ISE.class)
  public void testFailIfExists() throws IOException
  {
    Assert.assertFalse(portFile.exists());
    Assert.assertTrue(portFile.createNewFile());
    final PortWriter writer = new PortWriter(me);
    writer.start();
  }

  // Unfortunately, the same JVM can't hold multiple instances of the lock
  // This is just to verify expectations
  @Test(expected = OverlappingFileLockException.class)
  public void testLocking() throws IOException, InterruptedException, ExecutionException
  {
    final CountDownLatch readyLatch = new CountDownLatch(1);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(1);
    final ExecutorService executorService = Execs.singleThreaded("testLocker");
    final Future<?> future = executorService.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try (FileChannel portFileChannel = FileChannel.open(
                portFile.toPath(),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
            )) {
              readyLatch.countDown();
              startLatch.await();
              final FileLock fileLock = portFileChannel.lock();
              try {
                doneLatch.await();
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
              }
              finally {
                fileLock.release();
              }
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw Throwables.propagate(e);
            }
          }
        }
    );
    readyLatch.await();
    try (FileChannel portFileChannel = FileChannel.open(
        portFile.toPath(),
        StandardOpenOption.READ,
        StandardOpenOption.WRITE
    )) {
      final FileLock fileLock = portFileChannel.lock();
      try {
        startLatch.countDown();
        Assert.assertFalse("Task did not lock!", doneLatch.await(100, TimeUnit.MILLISECONDS));
        if (future.isDone()) {
          future.get();
        }
      }
      catch (ExecutionException e) {
        Throwables.propagateIfInstanceOf(e.getCause(), OverlappingFileLockException.class);
        throw e;
      }
      finally {
        fileLock.release();
      }
      // Eventually some future test may be able to reach here.
      Assert.assertTrue("Unlock did not work!", doneLatch.await(100, TimeUnit.MILLISECONDS));
    }
    finally {
      executorService.shutdownNow();
    }
  }
}
