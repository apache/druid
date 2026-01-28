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

package org.apache.druid.segment.writeout;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class TmpFileSegmentWriteOutMediumTest
{
  private volatile TmpFileSegmentWriteOutMedium writeOutMedium;
  private ExecutorService executorService;

  @Before
  public void setUp() throws IOException
  {
    writeOutMedium = new TmpFileSegmentWriteOutMedium(FileUtils.createTempDir());
    executorService = Execs.multiThreaded(3, "writeOutMedium-%d");
  }

  @After
  public void tearDown()
  {
    executorService.shutdownNow();
  }

  @Test
  public void testFileCount() throws InterruptedException
  {
    final int threadCount = 3;
    final CountDownLatch latch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      executorService.submit(
          () -> {
            WriteOutBytes writeOutBytes = writeOutMedium.makeWriteOutBytes();
            Assert.assertEquals(0, writeOutMedium.getNumLocallyCreated());
            try {
              latch.countDown();
              latch.await();
              ByteBuffer allocate = ByteBuffer.allocate(4096);
              writeOutBytes.write(allocate);
            }
            catch (Exception e) {
              throw new RuntimeException(e);
            }
            Assert.assertEquals(1, writeOutMedium.getNumLocallyCreated());
          }
      );
    }
    executorService.awaitTermination(15, TimeUnit.SECONDS);
    Assert.assertEquals(threadCount, writeOutMedium.getFilesCreated());
  }
}
