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

import com.google.common.base.Optional;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@EnableKubernetesMockClient(crud = true)
public class CachingKubernetesPeonClientTest
{
  private static final String NAMESPACE = "test-namespace";
  private static final String OVERLORD_NAMESPACE = "overlord-test";
  private static final String JOB_NAME = "test-job-abc123";
  private static final String POD_NAME = "test-job-abc123-pod";

  private KubernetesClient client;
  private KubernetesMockServer server;
  private CachingKubernetesPeonClient peonClient;
  private StubServiceEmitter serviceEmitter;
  private TestCachingKubernetesClient cachingClient;

  @BeforeEach
  public void setup() throws Exception
  {
    serviceEmitter = new StubServiceEmitter("service", "host");

    // Set up real informers with the mock client
    TestKubernetesClient clientApi = new TestKubernetesClient(client, NAMESPACE);

    cachingClient = new TestCachingKubernetesClient(clientApi, NAMESPACE);
    cachingClient.start();

    peonClient = new CachingKubernetesPeonClient(cachingClient, NAMESPACE, "", false, serviceEmitter);
  }

  @AfterEach
  public void teardown()
  {
    if (cachingClient != null) {
      cachingClient.stop();
    }
  }

  @Test
  public void test_getPeonPod_withPodInCache_returnsPresentOptional() throws Exception
  {
    // Create pod in mock server
    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .withNamespace(NAMESPACE)
        .addToLabels("job-name", JOB_NAME)
        .endMetadata()
        .withNewStatus()
        .withPodIP("10.0.0.1")
        .endStatus()
        .build();

    client.pods().inNamespace(NAMESPACE).resource(pod).create();

    // Wait for informer to sync
    cachingClient.waitForSync();

    // Query from cache
    Optional<Pod> result = peonClient.getPeonPod(JOB_NAME);

    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(POD_NAME, result.get().getMetadata().getName());
  }

  @Test
  public void test_getPeonPod_withoutPodInCache_returnsAbsentOptional() throws Exception
  {
    // Wait for informer to sync (empty cache)
    cachingClient.waitForSync();

    Optional<Pod> result = peonClient.getPeonPod(JOB_NAME);

    Assertions.assertFalse(result.isPresent());
  }

  @Test
  public void test_getPeonPod_withMultiplePodsForSameJob_returnsFirstOne() throws Exception
  {
    Pod pod1 = new PodBuilder()
        .withNewMetadata()
        .withName("pod-1")
        .withNamespace(NAMESPACE)
        .addToLabels("job-name", JOB_NAME)
        .endMetadata()
        .build();

    Pod pod2 = new PodBuilder()
        .withNewMetadata()
        .withName("pod-2")
        .withNamespace(NAMESPACE)
        .addToLabels("job-name", JOB_NAME)
        .endMetadata()
        .build();

    client.pods().inNamespace(NAMESPACE).resource(pod1).create();
    client.pods().inNamespace(NAMESPACE).resource(pod2).create();

    cachingClient.waitForSync();

    Optional<Pod> result = peonClient.getPeonPod(JOB_NAME);

    Assertions.assertTrue(result.isPresent());
    // Should return one of them (order may vary)
    String podName = result.get().getMetadata().getName();
    Assertions.assertTrue("pod-1".equals(podName) || "pod-2".equals(podName));
  }

  @Test
  public void test_getPeonJob_withJobInCache_returnsPresentOptional() throws Exception
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(JOB_NAME)
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    cachingClient.waitForSync();

    Optional<Job> result = peonClient.getPeonJob(JOB_NAME);

    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(JOB_NAME, result.get().getMetadata().getName());
  }

  @Test
  public void test_getPeonJob_withoutJobInCache_returnsAbsentOptional() throws Exception
  {
    cachingClient.waitForSync();

    Optional<Job> result = peonClient.getPeonJob(JOB_NAME);

    Assertions.assertFalse(result.isPresent());
  }

  @Test
  public void test_getPeonJobs_withoutOverlordNamespace_returnsAllJobsFromCache() throws Exception
  {
    Job job1 = new JobBuilder()
        .withNewMetadata()
        .withName("job-1")
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .build();

    Job job2 = new JobBuilder()
        .withNewMetadata()
        .withName("job-2")
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job1).create();
    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job2).create();

    cachingClient.waitForSync();

    List<Job> jobs = peonClient.getPeonJobs();

    Assertions.assertEquals(2, jobs.size());
  }

  @Test
  public void test_getPeonJobs_withOverlordNamespace_returnsFilteredJobs() throws Exception
  {
    peonClient = new CachingKubernetesPeonClient(cachingClient, NAMESPACE, OVERLORD_NAMESPACE, false, serviceEmitter);

    Job matchingJob = new JobBuilder()
        .withNewMetadata()
        .withName("matching-job")
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .addToLabels(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, OVERLORD_NAMESPACE)
        .endMetadata()
        .build();

    Job nonMatchingJob = new JobBuilder()
        .withNewMetadata()
        .withName("non-matching-job")
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .addToLabels(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, "other-namespace")
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(matchingJob).create();
    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(nonMatchingJob).create();

    cachingClient.waitForSync();

    List<Job> jobs = peonClient.getPeonJobs();

    Assertions.assertEquals(1, jobs.size());
    Assertions.assertEquals("matching-job", jobs.get(0).getMetadata().getName());
  }

  @Test
  public void test_getPeonJobs_whenCacheEmpty_returnsEmptyList() throws Exception
  {
    cachingClient.waitForSync();

    List<Job> jobs = peonClient.getPeonJobs();

    Assertions.assertEquals(0, jobs.size());
  }

  @Test
  public void test_waitForPeonJobCompletion_jobSucceeds() throws Exception
  {
    // Create job in running state
    K8sTaskId taskId = new K8sTaskId("", "original-task-id");
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(taskId.getK8sJobName())
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .withNewStatus()
        .withActive(1)
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();
    cachingClient.waitForSync();


    // Start waiting in background
    CompletableFuture<JobResponse> futureResponse = CompletableFuture.supplyAsync(() ->
        peonClient.waitForPeonJobCompletion(taskId, 60, TimeUnit.SECONDS)
    );

    // Give it a moment to start waiting
    Thread.sleep(500);

    // Update job to succeeded state
    Job succeededJob = new JobBuilder()
        .withNewMetadata()
        .withName(taskId.getK8sJobName())
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .withNewStatus()
        .withSucceeded(1)
        .withActive(0)
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(succeededJob).update();

    // Wait for response
    JobResponse response = futureResponse.get(60, TimeUnit.SECONDS);

    Assertions.assertEquals(PeonPhase.SUCCEEDED, response.getPhase());
    Assertions.assertNotNull(response.getJob());
  }

  @Test
  public void test_waitUntilPeonPodCreatedAndReady_podBecomesReady() throws Exception
  {
    // Create pod without IP (not ready)
    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .withNamespace(NAMESPACE)
        .addToLabels("job-name", JOB_NAME)
        .endMetadata()
        .withNewStatus()
        .endStatus()
        .build();

    client.pods().inNamespace(NAMESPACE).resource(pod).create();
    cachingClient.waitForSync();

    // Start waiting for pod to be ready in background
    CompletableFuture<Pod> futurePod = CompletableFuture.supplyAsync(() ->
        peonClient.waitUntilPeonPodCreatedAndReady(JOB_NAME, 10, TimeUnit.SECONDS)
    );

    // Give it a moment to start waiting
    Thread.sleep(500);

    // Update pod with IP (becomes ready)
    Pod readyPod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .withNamespace(NAMESPACE)
        .addToLabels("job-name", JOB_NAME)
        .endMetadata()
        .withNewStatus()
        .withPhase("Running")
        .withPodIP("10.0.0.1")
        .endStatus()
        .build();

    client.pods().inNamespace(NAMESPACE).resource(readyPod).update();

    // Wait for result
    Pod result = futurePod.get(5, TimeUnit.SECONDS);

    Assertions.assertNotNull(result);
    Assertions.assertEquals("10.0.0.1", result.getStatus().getPodIP());
  }

  @Test
  public void test_waitUntilPeonPodCreatedAndReady_timeoutWhenPodNotReady() throws Exception
  {
    // Create pod without IP (never becomes ready)
    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .withNamespace(NAMESPACE)
        .addToLabels("job-name", JOB_NAME)
        .endMetadata()
        .withNewStatus()
        .endStatus()
        .build();

    client.pods().inNamespace(NAMESPACE).resource(pod).create();
    cachingClient.waitForSync();

    // Wait for pod to be ready with short timeout
    Pod result = peonClient.waitUntilPeonPodCreatedAndReady(JOB_NAME, 1, TimeUnit.SECONDS);

    // Should return null on timeout
    Assertions.assertNull(result);
  }

  @Test
  public void test_waitUntilPeonPodCreatedAndReady_returnNullWhenPodNeverCreated() throws Exception
  {
    cachingClient.waitForSync();

    Assertions.assertNull(peonClient.waitUntilPeonPodCreatedAndReady(JOB_NAME, 1, TimeUnit.SECONDS));
  }

  @Test
  public void test_waitForPeonJobCompletion_timeoutWhenJobNeverCompletes() throws Exception
  {
    // Create job that stays in running state
    K8sTaskId taskId = new K8sTaskId("", "timeout-task-id");
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(taskId.getK8sJobName())
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .withNewStatus()
        .withActive(1)
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();
    cachingClient.waitForSync();

    // Wait with short timeout - job never completes
    JobResponse response = peonClient.waitForPeonJobCompletion(taskId, 500, TimeUnit.MILLISECONDS);

    // Should return FAILED phase on timeout
    Assertions.assertEquals(PeonPhase.FAILED, response.getPhase());
    Assertions.assertNull(response.getJob());
  }

  @Test
  public void test_waitForPeonJobCompletion_jobFails() throws Exception
  {
    // Create job in running state
    K8sTaskId taskId = new K8sTaskId("", "failing-task-id");
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(taskId.getK8sJobName())
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .withNewStatus()
        .withActive(1)
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();
    cachingClient.waitForSync();

    // Start waiting in background
    CompletableFuture<JobResponse> futureResponse = CompletableFuture.supplyAsync(() ->
        peonClient.waitForPeonJobCompletion(taskId, 60, TimeUnit.SECONDS)
    );

    // Give it a moment to start waiting
    Thread.sleep(500);

    // Update job to failed state
    Job failedJob = new JobBuilder()
        .withNewMetadata()
        .withName(taskId.getK8sJobName())
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .withNewStatus()
        .withFailed(1)
        .withActive(0)
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(failedJob).update();

    // Wait for response
    JobResponse response = futureResponse.get(60, TimeUnit.SECONDS);

    Assertions.assertEquals(PeonPhase.FAILED, response.getPhase());
    Assertions.assertNotNull(response.getJob());
  }

  @Test
  public void test_waitForPeonJobCompletion_jobGetsDeleted() throws Exception
  {
    // Create job in running state
    K8sTaskId taskId = new K8sTaskId("", "deleted-task-id");
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(taskId.getK8sJobName())
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .withNewStatus()
        .withActive(1)
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();
    cachingClient.waitForSync();

    // Start waiting in background
    CompletableFuture<JobResponse> futureResponse = CompletableFuture.supplyAsync(() ->
        peonClient.waitForPeonJobCompletion(taskId, 60, TimeUnit.SECONDS)
    );

    // Give it a moment to start waiting
    Thread.sleep(500);

    // Delete the job (simulates task cancellation/shutdown)
    client.batch().v1().jobs().inNamespace(NAMESPACE).withName(taskId.getK8sJobName()).delete();

    // Wait for response
    JobResponse response = futureResponse.get(10, TimeUnit.SECONDS);

    // Should detect deletion and return FAILED
    Assertions.assertEquals(PeonPhase.FAILED, response.getPhase());
    Assertions.assertNull(response.getJob());
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  public void test_waitForPeonJobCompletion_jobDeletedBeforeSeenInCache() throws Exception
  {
    // Create job
    K8sTaskId taskId = new K8sTaskId("", "quick-delete-task-id");
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(taskId.getK8sJobName())
        .withNamespace(NAMESPACE)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .withNewStatus()
        .withActive(1)
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    // Delete immediately before informer syncs
    client.batch().v1().jobs().inNamespace(NAMESPACE).withName(taskId.getK8sJobName()).delete();

    cachingClient.waitForSync();

    JobResponse response = peonClient.waitForPeonJobCompletion(taskId, 10, TimeUnit.SECONDS);

    // Should timeout or detect job was never seen and return FAILED
    Assertions.assertEquals(PeonPhase.FAILED, response.getPhase());
  }

  @Test
  void test_getPeonLogsWatcher_withJob_returnsWatchLogInOptional() throws InterruptedException
  {
    K8sTaskId taskId = new K8sTaskId("", "id");
    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .addToLabels("job-name", taskId.getK8sJobName())
        .endMetadata()
        .build();

    client.pods().inNamespace(NAMESPACE).resource(pod).create();

    cachingClient.waitForSync();

    server.expect().get()
          .withPath("/api/v1/namespaces/namespace/pods/id/log?pretty=false&container=main")
          .andReturn(HttpURLConnection.HTTP_OK, "data")
          .once();

    Optional<LogWatch> maybeLogWatch = peonClient.getPeonLogWatcher(taskId);
    Assertions.assertTrue(maybeLogWatch.isPresent());
  }
}
