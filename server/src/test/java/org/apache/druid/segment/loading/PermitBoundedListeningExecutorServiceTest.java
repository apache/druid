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

package org.apache.druid.segment.loading;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.loading.StorageLoadingThreadPool.PermitBoundedListeningExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests the cancellation behavior of {@link PermitBoundedListeningExecutorService}: a task parked on the permit (but
 * not yet running its body) must be abortable via {@code cancel(true)}. This is the "mid-tier" download cancellation
 * that lets the on-demand load path stop queued/permit-blocked column downloads before they touch deep storage.
 */
class PermitBoundedListeningExecutorServiceTest
{
  private ListeningExecutorService backing;

  @AfterEach
  void tearDown()
  {
    if (backing != null) {
      backing.shutdownNow();
    }
  }

  @Test
  @Timeout(30)
  void testCancelInterruptsTaskWaitingOnPermitBeforeItRuns() throws Exception
  {
    backing = MoreExecutors.listeningDecorator(Execs.multiThreaded(2, "permit-bounded-test-%d"));
    final Semaphore permits = new Semaphore(1);
    final PermitBoundedListeningExecutorService exec =
        new PermitBoundedListeningExecutorService(backing, permits);

    final CountDownLatch holderRunning = new CountDownLatch(1);
    final CountDownLatch releaseHolder = new CountDownLatch(1);

    // Task A takes the only permit and parks in its body, so any other task must wait on the permit to start.
    final ListenableFuture<?> holder = exec.submit(() -> {
      holderRunning.countDown();
      try {
        releaseHolder.await();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    Assertions.assertTrue(holderRunning.await(10, TimeUnit.SECONDS), "holder task should start and take the permit");

    // Task B starts on another thread but blocks acquiring the permit (0 available) before its body can run.
    final AtomicBoolean bodyRan = new AtomicBoolean(false);
    final ListenableFuture<?> blocked = exec.submit(() -> {
      bodyRan.set(true);
      return null;
    });

    // Wait until B is genuinely parked on the semaphore, not merely submitted. Guarded by @Timeout against a hang.
    for (int i = 0; i < 2000 && !permits.hasQueuedThreads(); i++) {
      Thread.sleep(5);
    }
    Assertions.assertTrue(permits.hasQueuedThreads(), "task B should be parked waiting on the permit");
    Assertions.assertEquals(0, permits.availablePermits(), "the holder still owns the only permit");

    // Cancelling with interruption must abort the permit wait before B's body runs.
    Assertions.assertTrue(blocked.cancel(true), "cancel(true) should interrupt the permit-blocked task");
    Assertions.assertTrue(blocked.isCancelled());
    Assertions.assertThrows(CancellationException.class, blocked::get);
    Assertions.assertFalse(bodyRan.get(), "permit-blocked task body must not run after cancellation");

    // An interrupted acquire takes no permit, so cancelling B must not have consumed or leaked one: the holder still
    // owns the only permit, and releasing the holder returns it.
    Assertions.assertEquals(0, permits.availablePermits());
    releaseHolder.countDown();
    holder.get(10, TimeUnit.SECONDS);
    Assertions.assertEquals(1, permits.availablePermits(), "the holder must return its permit on completion");
  }
}
