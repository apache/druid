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
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@EnableKubernetesMockClient(crud = true)
public class CachingKubernetesPeonClientTest
{
  private static final String NAMESPACE = "test-namespace";
  private static final String OVERLORD_NAMESPACE = "overlord-test";
  private static final String JOB_NAME = "test-job-abc123";
  private static final String POD_NAME = "test-job-abc123-pod";

  private KubernetesClient client;
  private KubernetesMockServer server;
  private TestKubernetesClientApi clientApi;
  private CachingKubernetesPeonClient peonClient;
  private StubServiceEmitter serviceEmitter;

  @BeforeEach
  public void setup() throws Exception
  {
    serviceEmitter = new StubServiceEmitter("service", "host");

    // Set up real informers with the mock client
    clientApi = new TestKubernetesClientApi(client, NAMESPACE);
    clientApi.start();

    peonClient = new CachingKubernetesPeonClient(clientApi, NAMESPACE, false, serviceEmitter);
  }

  @AfterEach
  public void teardown()
  {
    if (clientApi != null) {
      clientApi.stop();
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
    clientApi.waitForSync();

    // Query from cache
    Optional<Pod> result = peonClient.getPeonPod(JOB_NAME);

    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(POD_NAME, result.get().getMetadata().getName());
  }

  @Test
  public void test_getPeonPod_withoutPodInCache_returnsAbsentOptional() throws Exception
  {
    // Wait for informer to sync (empty cache)
    clientApi.waitForSync();

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

    clientApi.waitForSync();

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

    clientApi.waitForSync();

    Optional<Job> result = peonClient.getPeonJob(JOB_NAME);

    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(JOB_NAME, result.get().getMetadata().getName());
  }

  @Test
  public void test_getPeonJob_withoutJobInCache_returnsAbsentOptional() throws Exception
  {
    clientApi.waitForSync();

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

    clientApi.waitForSync();

    List<Job> jobs = peonClient.getPeonJobs();

    Assertions.assertEquals(2, jobs.size());
  }

  @Test
  public void test_getPeonJobs_withOverlordNamespace_returnsFilteredJobs() throws Exception
  {
    peonClient = new CachingKubernetesPeonClient(clientApi, NAMESPACE, OVERLORD_NAMESPACE, false, serviceEmitter);

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

    clientApi.waitForSync();

    List<Job> jobs = peonClient.getPeonJobs();

    Assertions.assertEquals(1, jobs.size());
    Assertions.assertEquals("matching-job", jobs.get(0).getMetadata().getName());
  }

  @Test
  public void test_getPeonJobs_whenCacheEmpty_returnsEmptyList() throws Exception
  {
    clientApi.waitForSync();

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
    clientApi.waitForSync();


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
    clientApi.waitForSync();

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
    clientApi.waitForSync();

    // Wait for pod to be ready with short timeout
    Pod result = peonClient.waitUntilPeonPodCreatedAndReady(JOB_NAME, 1, TimeUnit.SECONDS);

    // Should return null on timeout
    Assertions.assertNull(result);
  }

  @Test
  public void test_waitUntilPeonPodCreatedAndReady_throwsWhenPodNeverCreated() throws Exception
  {
    clientApi.waitForSync();

    // Wait for pod that is never created with short timeout
    Exception exception = Assertions.assertThrows(
        RuntimeException.class,
        () -> peonClient.waitUntilPeonPodCreatedAndReady(JOB_NAME, 1, TimeUnit.SECONDS)
    );

    // Should throw DruidException about timeout waiting for pod creation
    Assertions.assertTrue(
        exception.getMessage().contains("Timeout waiting for pod") ||
        exception.getCause().getMessage().contains("Timeout waiting for pod")
    );
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
    clientApi.waitForSync();

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
    clientApi.waitForSync();

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

  /**
   * Test implementation of KubernetesClientApi that uses real informers with the mock server
   */
  private static class TestKubernetesClientApi implements KubernetesClientApi
  {
    private final KubernetesClient client;
    private final SharedIndexInformer<Pod> podInformer;
    private final SharedIndexInformer<Job> jobInformer;
    private final KubernetesResourceEventNotifier eventNotifier;
    private final CountDownLatch syncLatch;

    public TestKubernetesClientApi(KubernetesClient client, String namespace)
    {
      this.client = client;
      this.eventNotifier = new KubernetesResourceEventNotifier();
      this.syncLatch = new CountDownLatch(2); // Wait for both informers

      // Set up pod informer with real event handlers
      this.podInformer = client.pods()
          .inNamespace(namespace)
          .inform(new ResourceEventHandler<Pod>()
          {
            @Override
            public void onAdd(Pod pod)
            {
              notifyPodChange(pod);
            }

            @Override
            public void onUpdate(Pod oldPod, Pod newPod)
            {
              notifyPodChange(newPod);
            }

            @Override
            public void onDelete(Pod pod, boolean deletedFinalStateUnknown)
            {
              notifyPodChange(pod);
            }
          }, 1000L);

      // Add pod indexer
      Map<String, Function<Pod, List<String>>> podIndexers = new HashMap<>();
      podIndexers.put("byJobName", pod -> {
        if (pod.getMetadata() != null && pod.getMetadata().getLabels() != null) {
          String jobName = pod.getMetadata().getLabels().get("job-name");
          if (jobName != null) {
            return Collections.singletonList(jobName);
          }
        }
        return Collections.emptyList();
      });
      podInformer.addIndexers(podIndexers);

      // Set up job informer with real event handlers
      this.jobInformer = client.batch()
          .v1()
          .jobs()
          .inNamespace(namespace)
          .withLabel(DruidK8sConstants.LABEL_KEY)
          .inform(new ResourceEventHandler<Job>()
          {
            @Override
            public void onAdd(Job job)
            {
              eventNotifier.notifyJobChange(job.getMetadata().getName(), job);
            }

            @Override
            public void onUpdate(Job oldJob, Job newJob)
            {
              eventNotifier.notifyJobChange(newJob.getMetadata().getName(), newJob);
            }

            @Override
            public void onDelete(Job job, boolean deletedFinalStateUnknown)
            {
              eventNotifier.notifyJobChange(job.getMetadata().getName(), job);
            }
          }, 1000L);

      // Add job indexers
      Map<String, Function<Job, List<String>>> jobIndexers = new HashMap<>();
      jobIndexers.put("byJobName", job -> {
        if (job.getMetadata() != null && job.getMetadata().getName() != null) {
          return Collections.singletonList(job.getMetadata().getName());
        }
        return Collections.emptyList();
      });
      jobIndexers.put("byOverlordNamespace", job -> {
        if (job.getMetadata() != null && job.getMetadata().getLabels() != null) {
          String overlordNamespace = job.getMetadata().getLabels().get(DruidK8sConstants.OVERLORD_NAMESPACE_KEY);
          if (overlordNamespace != null) {
            return Collections.singletonList(overlordNamespace);
          }
        }
        return Collections.emptyList();
      });
      jobInformer.addIndexers(jobIndexers);
    }

    public void start()
    {
      // Add ready callbacks to count down latch
      podInformer.addEventHandlerWithResyncPeriod(new ResourceEventHandler<Pod>()
      {
        @Override
        public void onAdd(Pod obj)
        {

        }
        @Override
        public void onUpdate(Pod oldObj, Pod newObj)
        {

        }
        @Override
        public void onDelete(Pod obj, boolean deletedFinalStateUnknown)
        {

        }
      }, 1000L);

      jobInformer.addEventHandlerWithResyncPeriod(new ResourceEventHandler<Job>()
      {
        @Override
        public void onAdd(Job obj)
        {

        }
        @Override
        public void onUpdate(Job oldObj, Job newObj)
        {

        }
        @Override
        public void onDelete(Job obj, boolean deletedFinalStateUnknown)
        {

        }
      }, 1000L);

      podInformer.run();
      jobInformer.run();

      // Count down after starting
      syncLatch.countDown();
      syncLatch.countDown();
    }

    public void stop()
    {
      if (podInformer != null) {
        podInformer.stop();
      }
      if (jobInformer != null) {
        jobInformer.stop();
      }
      if (eventNotifier != null) {
        eventNotifier.cancelAll();
      }
    }

    public void waitForSync() throws InterruptedException
    {
      syncLatch.await(5, TimeUnit.SECONDS);
      // Give informers a bit more time to process
      Thread.sleep(200);
    }

    private void notifyPodChange(Pod pod)
    {
      if (pod.getMetadata() != null && pod.getMetadata().getLabels() != null) {
        String jobName = pod.getMetadata().getLabels().get("job-name");
        if (jobName != null) {
          eventNotifier.notifyPodChange(jobName, pod);
        }
      }
    }

    @Override
    public <T> T executeRequest(KubernetesExecutor<T> executor) throws KubernetesResourceNotFoundException
    {
      return executor.executeRequest(client);
    }

    @Override
    public <T> T executePodCacheRequest(KubernetesInformerExecutor<T, Pod> executor)
    {
      return executor.executeRequest(podInformer);
    }

    @Override
    public <T> T executeJobCacheRequest(KubernetesInformerExecutor<T, Job> executor)
    {
      return executor.executeRequest(jobInformer);
    }

    @Override
    public KubernetesClient getClient()
    {
      return client;
    }

    @Override
    public SharedIndexInformer<Pod> getPodInformer()
    {
      return podInformer;
    }

    @Override
    public SharedIndexInformer<Job> getJobInformer()
    {
      return jobInformer;
    }

    @Override
    public KubernetesResourceEventNotifier getEventNotifier()
    {
      return eventNotifier;
    }
  }
}
