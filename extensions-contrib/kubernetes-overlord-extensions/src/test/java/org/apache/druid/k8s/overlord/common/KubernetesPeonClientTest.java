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
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
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
    Assertions.assertEquals(1, serviceEmitter.getEvents().size());
  }

  @Test
  void test_launchPeonJobAndWaitForStart_withDisappearingPod_throwsKubernetesClientTimeoutException()
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
        KubernetesClientTimeoutException.class,
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
        new K8sTaskId(ID),
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
        new K8sTaskId(ID),
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
        new K8sTaskId(ID),
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

    Assertions.assertTrue(instance.deletePeonJob(new K8sTaskId(ID)));
  }

  @Test
  void test_deletePeonJob_withoutJob_returnsFalse()
  {
    Assertions.assertFalse(instance.deletePeonJob(new K8sTaskId(ID)));
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

    Assertions.assertTrue(instance.deletePeonJob(new K8sTaskId(ID)));

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

    Assertions.assertTrue(instance.deletePeonJob(new K8sTaskId(ID)));
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

    Optional<InputStream> maybeInputStream = instance.getPeonLogs(new K8sTaskId(ID));
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

    Optional<InputStream> maybeInputStream = instance.getPeonLogs(new K8sTaskId(ID));
    Assertions.assertFalse(maybeInputStream.isPresent());
  }

  @Test
  void test_getPeonLogs_withoutJob_returnsEmptyOptional()
  {
    Optional<InputStream> stream = instance.getPeonLogs(new K8sTaskId(ID));
    Assertions.assertFalse(stream.isPresent());
  }

  @Test
  void test_getPeonJobs_withJob_returnsPodList()
  {
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .addToLabels("druid.k8s.peons", "true")
        .endMetadata()
        .build();

    client.batch().v1().jobs().inNamespace(NAMESPACE).resource(job).create();

    List<Job> jobs = instance.getPeonJobs();

    Assertions.assertEquals(1, jobs.size());
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
        .addToLabels("druid.k8s.peons", "true")
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
        .addToLabels("druid.k8s.peons", "true")
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
        .endMetadata()
        .withNewStatus()
        .withActive(1)
        .endStatus()
        .build();

    Job deletableJob = new JobBuilder()
        .withNewMetadata()
        .withName(StringUtils.format("%s-deleteable", KUBERNETES_JOB_NAME))
        .addToLabels("druid.k8s.peons", "true")
        .endMetadata()
        .withNewStatus()
        .withCompletionTime(new Timestamp(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5)).toString())
        .endStatus()
        .build();

    Job undeletableJob = new JobBuilder()
        .withNewMetadata()
        .withName(StringUtils.format("%s-undeletable", KUBERNETES_JOB_NAME))
        .addToLabels("druid.k8s.peons", "true")
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

    Pod pod = instance.getPeonPodWithRetries(new K8sTaskId(ID).getK8sJobName());

    Assertions.assertNotNull(pod);
  }

  @Test
  void test_getPeonPodWithRetries_withoutPod_raisesKubernetesResourceNotFoundException()
  {
    Assertions.assertThrows(
        KubernetesResourceNotFoundException.class,
        () -> instance.getPeonPodWithRetries(clientApi.getClient(), new K8sTaskId(ID).getK8sJobName(), 1, 1),
        StringUtils.format("K8s pod with label: job-name=%s not found", ID)
    );
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

    Optional<LogWatch> maybeLogWatch = instance.getPeonLogWatcher(new K8sTaskId(ID));
    Assertions.assertTrue(maybeLogWatch.isPresent());
  }


  @Test
  void test_getPeonLogsWatcher_withoutJob_returnsEmptyOptional()
  {
    Optional<LogWatch> maybeLogWatch = instance.getPeonLogWatcher(new K8sTaskId(ID));
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

    Optional<LogWatch> maybeLogWatch = instance.getPeonLogWatcher(new K8sTaskId(ID));
    Assertions.assertFalse(maybeLogWatch.isPresent());
  }
}
