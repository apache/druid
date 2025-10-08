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

package org.apache.druid.k8s.overlord.common;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KubernetesResourceEventNotifierTest
{
  private KubernetesResourceEventNotifier notifier;

  @BeforeEach
  public void setUp()
  {
    notifier = new KubernetesResourceEventNotifier();
  }

  @AfterEach
  public void tearDown()
  {
    notifier.cancelAll();
  }

  @Test
  public void testWaitForJobChange_CompletesOnNotification() throws Exception
  {
    String jobName = "test-job";
    Job mockJob = createMockJob(jobName);

    CompletableFuture<Job> future = notifier.waitForJobChange(jobName);
    assertFalse(future.isDone());

    notifier.notifyJobChange(jobName, mockJob);

    Job result = future.get(1, TimeUnit.SECONDS);
    assertSame(mockJob, result);
    assertTrue(future.isDone());
  }

  @Test
  public void testWaitForPodChange_CompletesOnNotification() throws Exception
  {
    String jobName = "test-job";
    Pod mockPod = createMockPod(jobName);

    CompletableFuture<Pod> future = notifier.waitForPodChange(jobName);
    assertFalse(future.isDone());

    notifier.notifyPodChange(jobName, mockPod);

    Pod result = future.get(1, TimeUnit.SECONDS);
    assertSame(mockPod, result);
    assertTrue(future.isDone());
  }

  @Test
  public void testMultipleWatchers_AllNotified() throws Exception
  {
    String jobName = "test-job";
    Job mockJob = createMockJob(jobName);

    CompletableFuture<Job> future1 = notifier.waitForJobChange(jobName);
    CompletableFuture<Job> future2 = notifier.waitForJobChange(jobName);
    CompletableFuture<Job> future3 = notifier.waitForJobChange(jobName);

    assertFalse(future1.isDone());
    assertFalse(future2.isDone());
    assertFalse(future3.isDone());

    notifier.notifyJobChange(jobName, mockJob);

    Job result1 = future1.get(1, TimeUnit.SECONDS);
    Job result2 = future2.get(1, TimeUnit.SECONDS);
    Job result3 = future3.get(1, TimeUnit.SECONDS);

    assertSame(mockJob, result1);
    assertSame(mockJob, result2);
    assertSame(mockJob, result3);
  }

  @Test
  public void testMultiplePodWatchers_AllNotified() throws Exception
  {
    String jobName = "test-job";
    Pod mockPod = createMockPod(jobName);

    CompletableFuture<Pod> future1 = notifier.waitForPodChange(jobName);
    CompletableFuture<Pod> future2 = notifier.waitForPodChange(jobName);
    CompletableFuture<Pod> future3 = notifier.waitForPodChange(jobName);

    assertFalse(future1.isDone());
    assertFalse(future2.isDone());
    assertFalse(future3.isDone());

    notifier.notifyPodChange(jobName, mockPod);

    Pod result1 = future1.get(1, TimeUnit.SECONDS);
    Pod result2 = future2.get(1, TimeUnit.SECONDS);
    Pod result3 = future3.get(1, TimeUnit.SECONDS);

    assertSame(mockPod, result1);
    assertSame(mockPod, result2);
    assertSame(mockPod, result3);
  }

  @Test
  public void testNotifyWithoutWatchers_NoException()
  {
    String jobName = "test-job";
    Job mockJob = createMockJob(jobName);

    // Should not throw exception
    notifier.notifyJobChange(jobName, mockJob);
    notifier.notifyPodChange(jobName, createMockPod(jobName));
  }

  @Test
  public void testDifferentJobNames_IndependentNotifications() throws Exception
  {
    String jobName1 = "job-1";
    String jobName2 = "job-2";
    Job mockJob1 = createMockJob(jobName1);
    Job mockJob2 = createMockJob(jobName2);

    CompletableFuture<Job> future1 = notifier.waitForJobChange(jobName1);
    CompletableFuture<Job> future2 = notifier.waitForJobChange(jobName2);

    notifier.notifyJobChange(jobName1, mockJob1);

    Job result1 = future1.get(1, TimeUnit.SECONDS);
    assertSame(mockJob1, result1);
    assertFalse(future2.isDone());

    notifier.notifyJobChange(jobName2, mockJob2);

    Job result2 = future2.get(1, TimeUnit.SECONDS);
    assertSame(mockJob2, result2);
  }

  @Test
  public void testDifferentPodJobNames_IndependentNotifications() throws Exception
  {
    String jobName1 = "job-1";
    String jobName2 = "job-2";
    Pod mockPod1 = createMockPod(jobName1);
    Pod mockPod2 = createMockPod(jobName2);

    CompletableFuture<Pod> future1 = notifier.waitForPodChange(jobName1);
    CompletableFuture<Pod> future2 = notifier.waitForPodChange(jobName2);

    notifier.notifyPodChange(jobName1, mockPod1);

    Pod result1 = future1.get(1, TimeUnit.SECONDS);
    assertSame(mockPod1, result1);
    assertFalse(future2.isDone());

    notifier.notifyPodChange(jobName2, mockPod2);

    Pod result2 = future2.get(1, TimeUnit.SECONDS);
    assertSame(mockPod2, result2);
  }

  @Test
  public void testCancelAll_CancelsAllPendingWatchers()
  {
    String jobName1 = "job-1";
    String jobName2 = "job-2";

    CompletableFuture<Job> jobFuture1 = notifier.waitForJobChange(jobName1);
    CompletableFuture<Job> jobFuture2 = notifier.waitForJobChange(jobName2);
    CompletableFuture<Pod> podFuture1 = notifier.waitForPodChange(jobName1);
    CompletableFuture<Pod> podFuture2 = notifier.waitForPodChange(jobName2);

    assertFalse(jobFuture1.isDone());
    assertFalse(jobFuture2.isDone());
    assertFalse(podFuture1.isDone());
    assertFalse(podFuture2.isDone());

    notifier.cancelAll();

    assertTrue(jobFuture1.isCancelled());
    assertTrue(jobFuture2.isCancelled());
    assertTrue(podFuture1.isCancelled());
    assertTrue(podFuture2.isCancelled());
  }

  @Test
  public void testCancelAll_CancelledFuturesThrowException()
  {
    String jobName = "test-job";

    CompletableFuture<Job> future = notifier.waitForJobChange(jobName);
    notifier.cancelAll();

    assertThrows(CancellationException.class, future::get);
  }

  @Test
  public void testSequentialNotifications_WatchersAreCleared() throws Exception
  {
    String jobName = "test-job";
    Job mockJob1 = createMockJob(jobName);
    Job mockJob2 = createMockJob(jobName);

    // First notification
    CompletableFuture<Job> future1 = notifier.waitForJobChange(jobName);
    notifier.notifyJobChange(jobName, mockJob1);
    Job result1 = future1.get(1, TimeUnit.SECONDS);
    assertSame(mockJob1, result1);

    // Second notification - should require new watcher
    CompletableFuture<Job> future2 = notifier.waitForJobChange(jobName);
    assertFalse(future2.isDone());
    notifier.notifyJobChange(jobName, mockJob2);
    Job result2 = future2.get(1, TimeUnit.SECONDS);
    assertSame(mockJob2, result2);
  }

  @Test
  public void testJobAndPodWatchers_Independent() throws Exception
  {
    String jobName = "test-job";
    Job mockJob = createMockJob(jobName);
    Pod mockPod = createMockPod(jobName);

    CompletableFuture<Job> jobFuture = notifier.waitForJobChange(jobName);
    CompletableFuture<Pod> podFuture = notifier.waitForPodChange(jobName);

    // Notify job change - should not affect pod watcher
    notifier.notifyJobChange(jobName, mockJob);
    Job jobResult = jobFuture.get(1, TimeUnit.SECONDS);
    assertSame(mockJob, jobResult);
    assertFalse(podFuture.isDone());

    // Notify pod change
    notifier.notifyPodChange(jobName, mockPod);
    Pod podResult = podFuture.get(1, TimeUnit.SECONDS);
    assertSame(mockPod, podResult);
  }

  @Test
  public void testWaitForJobChange_ReturnsNewFutureEachTime()
  {
    String jobName = "test-job";

    CompletableFuture<Job> future1 = notifier.waitForJobChange(jobName);
    CompletableFuture<Job> future2 = notifier.waitForJobChange(jobName);

    // Should be different future instances
    assertNotEquals(future1, future2);
  }

  @Test
  public void testWaitForPodChange_ReturnsNewFutureEachTime()
  {
    String jobName = "test-job";

    CompletableFuture<Pod> future1 = notifier.waitForPodChange(jobName);
    CompletableFuture<Pod> future2 = notifier.waitForPodChange(jobName);

    // Should be different future instances
    assertNotEquals(future1, future2);
  }

  private Job createMockJob(String name)
  {
    Job job = new Job();
    ObjectMeta metadata = new ObjectMeta();
    metadata.setName(name);
    job.setMetadata(metadata);
    return job;
  }

  private Pod createMockPod(String jobName)
  {
    Pod pod = new Pod();
    ObjectMeta metadata = new ObjectMeta();
    metadata.setName(jobName + "-pod");
    pod.setMetadata(metadata);
    return pod;
  }
}
