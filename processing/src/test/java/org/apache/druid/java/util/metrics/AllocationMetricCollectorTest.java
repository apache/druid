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

package org.apache.druid.java.util.metrics;

import org.apache.druid.java.util.common.logger.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class AllocationMetricCollectorTest
{
  private static final Logger log = new Logger(AllocationMetricCollectorTest.class);
  private static final int WORKER_COUNT = 2;
  private final List<Thread> threads = new ArrayList<>();
  private final List<byte[]> retainedAllocations = new CopyOnWriteArrayList<>();

  /**
   * Test a calculated delta includes allocations from multiple threads.
   * @throws InterruptedException
   */
  @SuppressWarnings("OptionalIsPresent")
  @Test
  public void testDelta() throws InterruptedException
  {
    AllocationMetricCollector collector = AllocationMetricCollectors.getAllocationMetricCollector();
    if (collector == null) {
      return;
    }

    long delta = collector.calculateDelta();
    Assertions.assertTrue(delta > 0);
    log.info("First delta: %s", delta);

    final int generatedSize2 = generateBytesConcurrently(16_000);
    long delta2 = collector.calculateDelta();
    Assertions.assertTrue(delta2 >= generatedSize2);
    Assertions.assertEquals(generatedSize2, retainedAllocationBytes());
    log.info("Second delta: %s", delta2);

    final int generatedSize3 = generateBytesConcurrently(1_600_000);
    long delta3 = collector.calculateDelta();
    Assertions.assertTrue(delta3 >= generatedSize3);
    Assertions.assertEquals(generatedSize2 + generatedSize3, retainedAllocationBytes());
    log.info("Third delta: %s", delta3);
  }

  private int generateBytesConcurrently(int bytesPerThread) throws InterruptedException
  {
    final int totalSize = bytesPerThread * WORKER_COUNT;
    final CountDownLatch countDownLatch = new CountDownLatch(WORKER_COUNT);
    for (int i = 0; i < WORKER_COUNT; i++) {
      final Thread thread = new Thread(() -> {
        // Keep the arrays reachable until after the allocation delta is measured.
        retainedAllocations.add(new byte[bytesPerThread]);
        countDownLatch.countDown();
        try {
          Thread.sleep(Long.MAX_VALUE);
        }
        catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
      });
      thread.setDaemon(true);
      thread.start();
      this.threads.add(thread);
    }
    countDownLatch.await();
    return totalSize;
  }

  private long retainedAllocationBytes()
  {
    return retainedAllocations.stream().mapToLong(array -> array.length).sum();
  }

  @AfterEach
  public void stopThreads() throws InterruptedException
  {
    // threads are in sleep so that their ids are still present in JVM allocation "registry"
    // so stop them manually
    for (Thread thread : threads) {
      thread.interrupt();
    }
    for (Thread thread : threads) {
      thread.join();
    }
  }
}
