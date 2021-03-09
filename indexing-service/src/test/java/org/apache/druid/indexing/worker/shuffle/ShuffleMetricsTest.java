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

package org.apache.druid.indexing.worker.shuffle;

import org.apache.druid.com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.worker.shuffle.ShuffleMetrics.PerDatasourceShuffleMetrics;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class ShuffleMetricsTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testShuffleRequested()
  {
    final ShuffleMetrics metrics = new ShuffleMetrics();
    final String supervisorTask1 = "supervisor1";
    final String supervisorTask2 = "supervisor2";
    final String supervisorTask3 = "supervisor3";
    metrics.shuffleRequested(supervisorTask1, 1024);
    metrics.shuffleRequested(supervisorTask2, 10);
    metrics.shuffleRequested(supervisorTask1, 512);
    metrics.shuffleRequested(supervisorTask3, 10000);
    metrics.shuffleRequested(supervisorTask2, 30);

    final Map<String, PerDatasourceShuffleMetrics> snapshot = metrics.snapshotAndReset();
    Assert.assertEquals(ImmutableSet.of(supervisorTask1, supervisorTask2, supervisorTask3), snapshot.keySet());

    PerDatasourceShuffleMetrics perDatasourceShuffleMetrics = snapshot.get(supervisorTask1);
    Assert.assertEquals(2, perDatasourceShuffleMetrics.getShuffleRequests());
    Assert.assertEquals(1536, perDatasourceShuffleMetrics.getShuffleBytes());

    perDatasourceShuffleMetrics = snapshot.get(supervisorTask2);
    Assert.assertEquals(2, perDatasourceShuffleMetrics.getShuffleRequests());
    Assert.assertEquals(40, perDatasourceShuffleMetrics.getShuffleBytes());

    perDatasourceShuffleMetrics = snapshot.get(supervisorTask3);
    Assert.assertEquals(1, perDatasourceShuffleMetrics.getShuffleRequests());
    Assert.assertEquals(10000, perDatasourceShuffleMetrics.getShuffleBytes());
  }

  @Test
  public void testSnapshotUnmodifiable()
  {
    expectedException.expect(UnsupportedOperationException.class);
    new ShuffleMetrics().snapshotAndReset().put("k", new PerDatasourceShuffleMetrics());
  }

  @Test
  public void testResetDatasourceMetricsAfterSnapshot()
  {
    final ShuffleMetrics shuffleMetrics = new ShuffleMetrics();
    shuffleMetrics.shuffleRequested("supervisor", 10);
    shuffleMetrics.shuffleRequested("supervisor", 10);
    shuffleMetrics.shuffleRequested("supervisor2", 10);
    shuffleMetrics.snapshotAndReset();

    Assert.assertEquals(Collections.emptyMap(), shuffleMetrics.getDatasourceMetrics());
  }

  @Test(timeout = 5000L)
  public void testConcurrency() throws ExecutionException, InterruptedException
  {
    final ExecutorService exec = Execs.multiThreaded(3, "shuffle-metrics-test-%d"); // 2 for write, 1 for read

    try {
      final ShuffleMetrics metrics = new ShuffleMetrics();
      final String supervisorTask1 = "supervisor1";
      final String supervisorTask2 = "supervisor2";

      final CountDownLatch firstUpdatelatch = new CountDownLatch(2);
      final List<Future<Void>> futures = new ArrayList<>();

      futures.add(
          exec.submit(() -> {
            metrics.shuffleRequested(supervisorTask1, 1024);
            metrics.shuffleRequested(supervisorTask2, 30);
            firstUpdatelatch.countDown();
            Thread.sleep(ThreadLocalRandom.current().nextInt(10));
            metrics.shuffleRequested(supervisorTask2, 10);
            return null;
          })
      );
      futures.add(
          exec.submit(() -> {
            metrics.shuffleRequested(supervisorTask2, 30);
            metrics.shuffleRequested(supervisorTask1, 1024);
            firstUpdatelatch.countDown();
            Thread.sleep(ThreadLocalRandom.current().nextInt(10));
            metrics.shuffleRequested(supervisorTask1, 32);
            return null;
          })
      );
      final Map<String, PerDatasourceShuffleMetrics> firstSnapshot = exec.submit(() -> {
        firstUpdatelatch.await();
        Thread.sleep(ThreadLocalRandom.current().nextInt(10));
        return metrics.snapshotAndReset();
      }).get();

      int expectedSecondSnapshotSize = 0;
      boolean task1ShouldBeInSecondSnapshot = false;
      boolean task2ShouldBeInSecondSnapshot = false;

      Assert.assertEquals(2, firstSnapshot.size());
      Assert.assertNotNull(firstSnapshot.get(supervisorTask1));
      Assert.assertTrue(
          2048 == firstSnapshot.get(supervisorTask1).getShuffleBytes()
          || 2080 == firstSnapshot.get(supervisorTask1).getShuffleBytes()
      );
      Assert.assertTrue(
          2 == firstSnapshot.get(supervisorTask1).getShuffleRequests()
          || 3 == firstSnapshot.get(supervisorTask1).getShuffleRequests()
      );
      if (firstSnapshot.get(supervisorTask1).getShuffleRequests() == 2) {
        expectedSecondSnapshotSize++;
        task1ShouldBeInSecondSnapshot = true;
      }
      Assert.assertNotNull(firstSnapshot.get(supervisorTask2));
      Assert.assertTrue(
          60 == firstSnapshot.get(supervisorTask2).getShuffleBytes()
          || 70 == firstSnapshot.get(supervisorTask2).getShuffleBytes()
      );
      Assert.assertTrue(
          2 == firstSnapshot.get(supervisorTask2).getShuffleRequests()
          || 3 == firstSnapshot.get(supervisorTask2).getShuffleRequests()
      );
      if (firstSnapshot.get(supervisorTask2).getShuffleRequests() == 2) {
        expectedSecondSnapshotSize++;
        task2ShouldBeInSecondSnapshot = true;
      }

      for (Future<Void> future : futures) {
        future.get();
      }
      final Map<String, PerDatasourceShuffleMetrics> secondSnapshot = metrics.snapshotAndReset();

      Assert.assertEquals(expectedSecondSnapshotSize, secondSnapshot.size());
      Assert.assertEquals(task1ShouldBeInSecondSnapshot, secondSnapshot.containsKey(supervisorTask1));
      if (task1ShouldBeInSecondSnapshot) {
        Assert.assertEquals(32, secondSnapshot.get(supervisorTask1).getShuffleBytes());
        Assert.assertEquals(1, secondSnapshot.get(supervisorTask1).getShuffleRequests());
      }
      Assert.assertEquals(task2ShouldBeInSecondSnapshot, secondSnapshot.containsKey(supervisorTask2));
      if (task2ShouldBeInSecondSnapshot) {
        Assert.assertEquals(10, secondSnapshot.get(supervisorTask2).getShuffleBytes());
        Assert.assertEquals(1, secondSnapshot.get(supervisorTask2).getShuffleRequests());
      }

    }
    finally {
      exec.shutdown();
    }
  }
}
