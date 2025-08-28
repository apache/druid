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
import io.fabric8.kubernetes.api.model.EventListBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

@EnableKubernetesMockClient(crud = true)
public class KubernetesPeonClientTest
{
  private static final String ID = "id";
  private static final String TASK_NAME_PREFIX = "";
  private static final String JOB_NAME = ID;
  private static final String KUBERNETES_JOB_NAME = KubernetesOverlordUtils.convertTaskIdToJobName(JOB_NAME);
  private static final String POD_NAME = "name";
  private static final String NAMESPACE = "namespace";

  private KubernetesClient client;
  private KubernetesMockServer server;
  private KubernetesClientApi clientApi;
  private KubernetesPeonClient instance;
  private StubServiceEmitter serviceEmitter;

  @BeforeEach
  public void setup()
  {
    clientApi = new TestKubernetesClient(this.client);
    serviceEmitter = new StubServiceEmitter("service", "host");
    instance = new KubernetesPeonClient(clientApi, NAMESPACE, false, serviceEmitter);
  }

  @Test
  void test_launchPeonJobAndWaitForStart()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .addToLabels("job-name", KUBERNETES_JOB_NAME)
        .endMetadata()
        .withNewStatus()
        .withPodIP("ip")
        .endStatus()
        .build();

    client.pods().inNamespace(NAMESPACE).resource(pod).create();

    Pod peonPod = instance.launchPeonJobAndWaitForStart(job, NoopTask.create(), 1, TimeUnit.SECONDS);

    Assertions.assertNotNull(peonPod);
    Assertions.assertEquals(1, serviceEmitter.getNumEmittedEvents());
  }

  @Test
  void test_launchPeonJobAndWaitForStart_withDisappearingPod_throwIllegalStateExceptionn()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    server.expect().get()
        .withPath("/api/v1/namespaces/namespace/pods?labelSelector=job-name%3D" + KUBERNETES_JOB_NAME)
        .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder()
            .addNewItem()
            .withNewMetadata()
            .withName(POD_NAME)
            .addToLabels("job-name", JOB_NAME)
            .endMetadata()
            .endItem()
            .build()
        ).once();

    Assertions.assertThrows(
        IllegalStateException.class,
        () -> instance.launchPeonJobAndWaitForStart(job, NoopTask.create(), 1, TimeUnit.SECONDS)
    );
  }

  @Test
  void test_launchPeonJobAndWaitForStart_withPendingPod_throwIllegalStateExceptionn()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .addToLabels("job-name", KUBERNETES_JOB_NAME)
        .endMetadata()
        .withNewStatus()
        .withPodIP(null)
        .endStatus()
        .build();

    client.pods().inNamespace(NAMESPACE).resource(pod).create();

    Assertions.assertThrows(
        DruidException.class,
        () -> instance.launchPeonJobAndWaitForStart(job, NoopTask.create(), 1, TimeUnit.SECONDS)
    );
  }
  
  @Test
  void test_waitForPeonJobCompletion_withSuccessfulJob_returnsJobResponseWithJobAndSucceededPeonPhase()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .withNewStatus()
        .withActive(null)
        .withSucceeded(1)
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    JobResponse jobResponse = instance.waitForPeonJobCompletion(
        new K8sTaskId(TASK_NAME_PREFIX, ID),
        1,
        TimeUnit.SECONDS
    );

    Assertions.assertEquals(PeonPhase.SUCCEEDED, jobResponse.getPhase());
    Assertions.assertNotNull(jobResponse.getJob());
  }

  @Test
  void test_waitForPeonJobCompletion_withFailedJob_returnsJobResponseWithJobAndFailedPeonPhase()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .withNewStatus()
        .withActive(null)
        .withFailed(1)
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    JobResponse jobResponse = instance.waitForPeonJobCompletion(
        new K8sTaskId(TASK_NAME_PREFIX, ID),
        1,
        TimeUnit.SECONDS
    );

    Assertions.assertEquals(PeonPhase.FAILED, jobResponse.getPhase());
    Assertions.assertNotNull(jobResponse.getJob());
  }

  @Test
  void test_waitforPeonJobCompletion_withoutRunningJob_returnsJobResponseWithEmptyJobAndFailedPeonPhase()
  {
    JobResponse jobResponse = instance.waitForPeonJobCompletion(
        new K8sTaskId(TASK_NAME_PREFIX, ID),
        1,
        TimeUnit.SECONDS
    );

    Assertions.assertEquals(PeonPhase.FAILED, jobResponse.getPhase());
    Assertions.assertNull(jobResponse.getJob());
  }

  @Test
  void test_deletePeonJob_withJob_returnsTrue()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    Assertions.assertTrue(instance.deletePeonJob(new K8sTaskId(TASK_NAME_PREFIX, ID)));
  }

  @Test
  void test_deletePeonJob_withoutJob_returnsFalse()
  {
    Assertions.assertFalse(instance.deletePeonJob(new K8sTaskId(TASK_NAME_PREFIX, ID)));
  }

  @Test
  void test_deletePeonJob_withJob_withDebugJobsTrue_skipsDelete()
  {
    KubernetesPeonClient instance = new KubernetesPeonClient(
        new TestKubernetesClient(this.client),
        NAMESPACE,
        true,
        serviceEmitter
    );

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    Assertions.assertTrue(instance.deletePeonJob(new K8sTaskId(TASK_NAME_PREFIX, ID)));

    Assertions.assertNotNull(
        client.batch().v1().jobs().inNamespace(NAMESPACE).withName(KUBERNETES_JOB_NAME).get()
    );
  }

  @Test
  void test_deletePeonJob_withoutJob_withDebugJobsTrue_skipsDelete()
  {
    KubernetesPeonClient instance = new KubernetesPeonClient(
        new TestKubernetesClient(this.client),
        NAMESPACE,
        true,
        serviceEmitter
    );

    Assertions.assertTrue(instance.deletePeonJob(new K8sTaskId(TASK_NAME_PREFIX, ID)));
  }

  @Test
  void test_getPeonLogs_withJob_returnsInputStreamInOptional()
  {
    server.expect().get()
        .withPath("/apis/batch/v1/namespaces/namespace/jobs/" + KUBERNETES_JOB_NAME)
        .andReturn(HttpURLConnection.HTTP_OK, new JobBuilder()
            .withNewMetadata()
            .withName(KUBERNETES_JOB_NAME)
            .withUid("uid")
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .endContainer()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build()
        ).once();

    server.expect().get()
        .withPath("/api/v1/namespaces/namespace/pods?labelSelector=controller-uid%3Duid")
        .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder()
            .addNewItem()
            .withNewMetadata()
            .withName(POD_NAME)
            .addNewOwnerReference()
            .withUid("uid")
            .withController(true)
            .endOwnerReference()
            .endMetadata()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .endContainer()
            .endSpec()
            .endItem()
            .build()
        ).once();

    server.expect().get()
        .withPath("/api/v1/namespaces/namespace/pods/id/log?pretty=false&container=main")
        .andReturn(HttpURLConnection.HTTP_OK, "data")
        .once();

    Optional<InputStream> maybeInputStream = instance.getPeonLogs(new K8sTaskId(TASK_NAME_PREFIX, ID));
    Assertions.assertTrue(maybeInputStream.isPresent());
  }

  @Test
  void test_getPeonLogs_withJobWithoutPod_returnsEmptyOptional()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    Optional<InputStream> maybeInputStream = instance.getPeonLogs(new K8sTaskId(TASK_NAME_PREFIX, ID));
    Assertions.assertFalse(maybeInputStream.isPresent());
  }

  @Test
  void test_getPeonLogs_withoutJob_returnsEmptyOptional()
  {
    Optional<InputStream> stream = instance.getPeonLogs(new K8sTaskId(TASK_NAME_PREFIX, ID));
    Assertions.assertFalse(stream.isPresent());
  }

  @Test
  void test_getPeonJobs_withJob_returnsPodList()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    List<Job> jobs = instance.getPeonJobs();

    Assertions.assertEquals(1, jobs.size());
  }

  @Test
  void test_getPeonJobs_withJobInDifferentNamespace_returnsPodList()
  {
    instance = new KubernetesPeonClient(clientApi, NAMESPACE, "ns", false, serviceEmitter);

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .addToLabels(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, "ns")
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    List<Job> jobs = instance.getPeonJobs();

    Assertions.assertEquals(1, jobs.size());
  }

  @Test
  void test_getPeonJobs_withJobInDifferentNamespaceButOverlordNamespaceNotSpecified_doesNotReturnPodList()
  {
    instance = new KubernetesPeonClient(clientApi, NAMESPACE, "ns", false, serviceEmitter);

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .addToLabels(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, "someOtherNamespace")
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    List<Job> jobs = instance.getPeonJobs();

    Assertions.assertEquals(0, jobs.size());
  }

  @Test
  void test_getPeonJobs_withJobInSameNamespaceWithoutLabels_doesNotReturnPodList()
  {
    instance = new KubernetesPeonClient(clientApi, NAMESPACE, NAMESPACE, false, serviceEmitter);

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    List<Job> jobs = instance.getPeonJobs();

    Assertions.assertEquals(0, jobs.size());
  }

  @Test
  void test_getPeonJobs_withoutJob_returnsEmptyList()
  {
    List<Job> jobs = instance.getPeonJobs();
    Assertions.assertEquals(0, jobs.size());
  }

  @Test
  void test_deleteCompletedPeonJobsOlderThan_withoutJob_returnsZero()
  {
    int deleted = instance.deleteCompletedPeonJobsOlderThan(1, TimeUnit.MINUTES);
    Assertions.assertEquals(0, deleted);
  }

  @Test
  void test_deleteCompletedPeonJobsOlderThan_withActiveJob_returnsZero()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .withNewStatus()
        .withActive(1)
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    int deleted = instance.deleteCompletedPeonJobsOlderThan(1, TimeUnit.MINUTES);

    Assertions.assertEquals(0, deleted);
  }

  @Test
  void test_deleteCompletedPeonJobsOlderThan_withoutDeleteableJob_returnsZero()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .addToLabels(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, NAMESPACE)
        .endMetadata()
        .withNewStatus()
        .withCompletionTime(new Timestamp(System.currentTimeMillis()).toString())
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    int deleted = instance.deleteCompletedPeonJobsOlderThan(1, TimeUnit.MINUTES);

    Assertions.assertEquals(0, deleted);
  }

  @Test
  void test_deleteCompletedPeonJobsOlderThan_withDeletableJob_returnsOne()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .addToLabels(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, NAMESPACE)
        .endMetadata()
        .withNewStatus()
        .withCompletionTime(new Timestamp(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5)).toString())
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    int deleted = instance.deleteCompletedPeonJobsOlderThan(1, TimeUnit.MINUTES);

    Assertions.assertEquals(1, deleted);
  }

  @Test
  void test_deleteCompletedPeonJobsOlderThan_withActiveAndDeletableAndNotDeletableJob_returnsOne()
  {
    Job activeJob = new JobBuilder()
        .withNewMetadata()
        .withName(StringUtils.format("%s-active", KUBERNETES_JOB_NAME))
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .addToLabels(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, NAMESPACE)
        .endMetadata()
        .withNewStatus()
        .withActive(1)
        .endStatus()
        .build();

    Job deletableJob = new JobBuilder()
        .withNewMetadata()
        .withName(StringUtils.format("%s-deleteable", KUBERNETES_JOB_NAME))
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .addToLabels(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, NAMESPACE)
        .endMetadata()
        .withNewStatus()
        .withCompletionTime(new Timestamp(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5)).toString())
        .endStatus()
        .build();

    Job undeletableJob = new JobBuilder()
        .withNewMetadata()
        .withName(StringUtils.format("%s-undeletable", KUBERNETES_JOB_NAME))
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .addToLabels(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, NAMESPACE)
        .endMetadata()
        .withNewStatus()
        .withCompletionTime(new Timestamp(System.currentTimeMillis()).toString())
        .endStatus()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(activeJob).create();
    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(deletableJob).create();
    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(undeletableJob).create();

    int deleted = instance.deleteCompletedPeonJobsOlderThan(1, TimeUnit.MINUTES);

    Assertions.assertEquals(1, deleted);
  }

  @Test
  void test_getPeonPod_withPod_returnsPodInOptional()
  {
    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .addToLabels("job-name", KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    client.pods().inNamespace(NAMESPACE).resource(pod).create();

    Optional<Pod> maybePod = instance.getPeonPod(KUBERNETES_JOB_NAME);

    Assertions.assertTrue(maybePod.isPresent());
  }

  @Test
  void test_getPeonPod_withoutPod_returnsEmptyOptional()
  {
    Optional<Pod> maybePod = instance.getPeonPod(KUBERNETES_JOB_NAME);
    Assertions.assertFalse(maybePod.isPresent());
  }

  @Test
  void test_getPeonPodWithRetries_withPod_returnsPod()
  {
    server.expect().get()
        .withPath("/api/v1/namespaces/namespace/pods?labelSelector=job-name%3D" + KUBERNETES_JOB_NAME)
        .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder().build())
        .once();

    server.expect().get()
        .withPath("/api/v1/namespaces/namespace/pods?labelSelector=job-name%3D" + KUBERNETES_JOB_NAME)
        .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder()
            .addNewItem()
            .withNewMetadata()
            .withName(POD_NAME)
            .addToLabels("job-name", KUBERNETES_JOB_NAME)
            .endMetadata()
            .endItem()
            .build()
        ).once();

    Pod pod = instance.getPeonPodWithRetries(new K8sTaskId(TASK_NAME_PREFIX, ID).getK8sJobName());

    Assertions.assertNotNull(pod);
  }

  @Test
  void test_getPeonPodWithRetries_withoutPod_raisesKubernetesResourceNotFoundException()
  {
    String k8sJobName = new K8sTaskId(TASK_NAME_PREFIX, ID).getK8sJobName();
    KubernetesResourceNotFoundException e = Assertions.assertThrows(
        KubernetesResourceNotFoundException.class,
        () -> instance.getPeonPodWithRetries(clientApi.getClient(), k8sJobName, 1, 1)
    );

    Assertions.assertEquals(e.getMessage(),
                            StringUtils.format("K8s pod with label[job-name=%s] not found", k8sJobName));
  }

  @Test
  void test_getPeonPodWithRetries_withoutPod_noRestartForBlacklistedEvent_raisesKubernetesResourceNotFoundException()
  {
    String k8sJobName = new K8sTaskId(TASK_NAME_PREFIX, ID).getK8sJobName();
    String blacklistedMessage = DruidK8sConstants.BLACKLISTED_PEON_POD_ERROR_MESSAGES.get(0);

    final String eventsPath = "/api/v1/namespaces/namespace/events?fieldSelector=" +
                        "involvedObject.name%3D" + k8sJobName +
                        "%2CinvolvedObject.namespace%3D" + NAMESPACE +
                        "%2CinvolvedObject.kind%3DJob" +
                        "%2CinvolvedObject.apiVersion%3Dbatch%2Fv1";

    server.expect().get()
          .withPath(eventsPath)
          .andReturn(HttpURLConnection.HTTP_OK, new EventListBuilder()
              .addNewItem()
              .withMessage(blacklistedMessage)
              .withNewInvolvedObject()
              .withApiVersion("batch/v1")
              .withKind("Job")
              .withName(k8sJobName)
              .withNamespace(NAMESPACE)
              .endInvolvedObject()
              .endItem()
              .build())
          // Test will fail if task is retried more than once.
          .once();

    // Task declared to retry for 3 times should only try once when a blacklisted event message is found.
    KubernetesResourceNotFoundException e = Assertions.assertThrows(
        KubernetesResourceNotFoundException.class,
        () -> instance.getPeonPodWithRetries(clientApi.getClient(), k8sJobName, 0, 3)
    );

    // Ensure event message is propagated to the users.
    Assertions.assertTrue(e.getMessage().contains(blacklistedMessage));
  }

  @Test
  void test_getPeonLogsWatcher_withJob_returnsWatchLogInOptional()
  {
    server.expect().get()
        .withPath("/apis/batch/v1/namespaces/namespace/jobs/" + KUBERNETES_JOB_NAME)
        .andReturn(HttpURLConnection.HTTP_OK, new JobBuilder()
            .withNewMetadata()
            .withName(KUBERNETES_JOB_NAME)
            .withUid("uid")
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .endContainer()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build()
        ).once();

    server.expect().get()
        .withPath("/api/v1/namespaces/namespace/pods?labelSelector=controller-uid%3Duid")
        .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder()
            .addNewItem()
            .withNewMetadata()
            .withName(POD_NAME)
            .addNewOwnerReference()
            .withUid("uid")
            .withController(true)
            .endOwnerReference()
            .endMetadata()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .endContainer()
            .endSpec()
            .endItem()
            .build()
        ).once();

    server.expect().get()
        .withPath("/api/v1/namespaces/namespace/pods/id/log?pretty=false&container=main")
        .andReturn(HttpURLConnection.HTTP_OK, "data")
        .once();

    Optional<LogWatch> maybeLogWatch = instance.getPeonLogWatcher(new K8sTaskId(TASK_NAME_PREFIX, ID));
    Assertions.assertTrue(maybeLogWatch.isPresent());
  }


  @Test
  void test_getPeonLogsWatcher_withoutJob_returnsEmptyOptional()
  {
    Optional<LogWatch> maybeLogWatch = instance.getPeonLogWatcher(new K8sTaskId(TASK_NAME_PREFIX, ID));
    Assertions.assertFalse(maybeLogWatch.isPresent());
  }

  @Test
  void test_getPeonLogWatcher_withJobWithoutPod_returnsEmptyOptional()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(JOB_NAME)
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    Optional<LogWatch> maybeLogWatch = instance.getPeonLogWatcher(new K8sTaskId(TASK_NAME_PREFIX, ID));
    Assertions.assertFalse(maybeLogWatch.isPresent());
  }

  @Test
  void test_createK8sJobWithRetries_withSuccessfulCreation_createsJob()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    // Should not throw any exception
    instance.createK8sJobWithRetries(job);

    // Verify job was created
    Job createdJob = client.batch().v1().jobs().inNamespace(NAMESPACE).withName(KUBERNETES_JOB_NAME).get();
    Assertions.assertNotNull(createdJob);
    Assertions.assertEquals(KUBERNETES_JOB_NAME, createdJob.getMetadata().getName());
  }

  @Test
  void test_createK8sJobWithRetries_withNonRetryableException_failsImmediately()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    String jobPath = "/apis/batch/v1/namespaces/" + NAMESPACE + "/jobs";

    // Return 403 Forbidden - this is not a retryable exception
    server.expect().post()
        .withPath(jobPath)
        .andReturn(HttpURLConnection.HTTP_FORBIDDEN, "Forbidden: insufficient permissions")
        .once();

    // Should fail immediately without retries
    DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> instance.createK8sJobWithRetries(clientApi.getClient(), job, 0, 5)
    );

    // Verify the error message contains our job name
    Assertions.assertTrue(e.getMessage().contains(KUBERNETES_JOB_NAME));
  }

  @Test
  void test_createK8sJobWithRetries_withJobAlreadyExists_succeedsGracefully()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    String jobPath = "/apis/batch/v1/namespaces/" + NAMESPACE + "/jobs";

    // Return 409 Conflict - job already exists
    server.expect().post()
        .withPath(jobPath)
        .andReturn(HttpURLConnection.HTTP_CONFLICT, "Job already exists")
        .once();

    // Should succeed gracefully without throwing exception
    Assertions.assertDoesNotThrow(
        () -> instance.createK8sJobWithRetries(clientApi.getClient(), job, 0, 5)
    );
  }

  @Test
  void test_waitForPodResultWithRetries_withSuccessfulPodReady_returnsPod()
  {
    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .endMetadata()
        .withNewStatus()
        .withPodIP("192.168.1.100")
        .endStatus()
        .build();

    // Create the pod in the mock client
    client.pods().inNamespace(NAMESPACE).resource(pod).create();

    // Should return the pod successfully
    Pod result = instance.waitForPodResultWithRetries(
        clientApi.getClient(), 
        pod, 
        1, 
        TimeUnit.SECONDS, 
        0, 
        3
    );

    Assertions.assertNotNull(result);
    Assertions.assertEquals(POD_NAME, result.getMetadata().getName());
    Assertions.assertEquals("192.168.1.100", result.getStatus().getPodIP());
  }

  @Test
  void test_waitForPodResultWithRetries_withNonRetryableFailure_throwsDruidException()
  {
    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .endMetadata()
        .withNewStatus()
        .withPodIP(null) // Pod without IP, will timeout
        .endStatus()
        .build();

    String podPath = "/api/v1/namespaces/" + NAMESPACE + "/pods/" + POD_NAME;

    // Mock server to return the pod without IP, causing timeout
    server.expect().get()
        .withPath(podPath + "?watch=true")
        .andReturn(HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal server error")
        .once();

    // Should throw DruidException after failure
    DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> instance.waitForPodResultWithRetries(
            clientApi.getClient(), 
            pod, 
            1, 
            TimeUnit.MILLISECONDS, // Very short timeout to force failure
            0, 
            1
        )
    );

    // Verify the error message contains our pod name
    Assertions.assertTrue(e.getMessage().contains(POD_NAME));
  }
}
