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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@EnableKubernetesMockClient(crud = true)
public class DruidKubernetesPeonClientTest
{

  private KubernetesClient client;
  private KubernetesMockServer server;

  @Test
  void testWaitingForAPodToGetReadyThatDoesntExist()
  {
    DruidKubernetesPeonClient client = new DruidKubernetesPeonClient(new TestKubernetesClient(this.client), "test",
                                                                     false
    );
    JobResponse jobResponse = client.waitForJobCompletion(new K8sTaskId("some-task"), 1, TimeUnit.SECONDS);
    Assertions.assertEquals(PeonPhase.FAILED, jobResponse.getPhase());
    Assertions.assertNull(jobResponse.getJob());
  }

  @Test
  void testWaitingForAPodToGetReadySuccess()
  {
    DruidKubernetesPeonClient peonClient = new DruidKubernetesPeonClient(new TestKubernetesClient(this.client), "test",
        false
    );
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName("sometask")
        .endMetadata()
        .withStatus(new JobStatusBuilder().withActive(null).withSucceeded(1).build())
        .build();
    client.batch().v1().jobs().inNamespace("test").create(job);
    JobResponse jobResponse = peonClient.waitForJobCompletion(new K8sTaskId("sometask"), 1, TimeUnit.SECONDS);
    Assertions.assertEquals(PeonPhase.SUCCEEDED, jobResponse.getPhase());
    Assertions.assertEquals(job.getStatus().getSucceeded(), jobResponse.getJob().getStatus().getSucceeded());
  }

  @Test
  void testTheFlow()
  {
    DruidKubernetesPeonClient peonClient = new DruidKubernetesPeonClient(new TestKubernetesClient(this.client), "test",
                                                                         false
    );
    List<Job> currentJobs = peonClient.listAllPeonJobs();
    Assertions.assertEquals(0, currentJobs.size());
    Job job = new JobBuilder()
        .withNewMetadata()
        .withName("job_name")
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .withNewSpec()
        .withTemplate(new PodTemplateSpec(new ObjectMeta(), K8sTestUtils.getDummyPodSpec()))
        .endSpec().build();
    client.batch().v1().jobs().inNamespace("test").create(job);
    currentJobs = peonClient.listAllPeonJobs();
    Assertions.assertEquals(1, currentJobs.size());
  }

  @Test
  void testListPeonPods()
  {
    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName("foo")
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .withSpec(K8sTestUtils.getDummyPodSpec())
        .build();
    client.pods().inNamespace("test").create(pod);
    DruidKubernetesPeonClient peonClient = new DruidKubernetesPeonClient(new TestKubernetesClient(this.client), "test",
                                                                         false
    );
    List<Pod> pods = peonClient.listPeonPods();
    Assertions.assertEquals(1, pods.size());
  }

  @Test
  void testCleanup() throws KubernetesResourceNotFoundException
  {
    DruidKubernetesPeonClient peonClient = new DruidKubernetesPeonClient(new TestKubernetesClient(this.client), "test",
                                                                         false
    );
    Job active = mockJob(true, new Timestamp(System.currentTimeMillis()));
    long tenMinutesAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10);
    Job dontKillYet = mockJob(false, new Timestamp(tenMinutesAgo));
    long oneHourAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(60);
    Job killThisOne = mockJob(false, new Timestamp(oneHourAgo));

    List<Job> jobs = Lists.newArrayList(active, dontKillYet, killThisOne);
    List<Job> toDelete = peonClient.getJobsToCleanup(jobs, 30, TimeUnit.MINUTES);
    Assertions.assertEquals(1, toDelete.size()); // should only cleanup one job
    Assertions.assertEquals(killThisOne, Iterables.getOnlyElement(toDelete)); // should only cleanup one job
  }

  @Test
  void testCleanupReturnValue() throws KubernetesResourceNotFoundException
  {
    DruidKubernetesPeonClient peonClient = new DruidKubernetesPeonClient(new TestKubernetesClient(this.client), "test",
        false
    );
    Assertions.assertFalse(peonClient.cleanUpJob(new K8sTaskId("sometask")));

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName("sometask")
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .withNewSpec()
        .withTemplate(new PodTemplateSpec(new ObjectMeta(), K8sTestUtils.getDummyPodSpec()))
        .endSpec().build();
    client.batch().v1().jobs().inNamespace("test").create(job);
    Assertions.assertTrue(peonClient.cleanUpJob(new K8sTaskId("sometask")));
  }

  @Test
  void watchingALogThatDoesntExist()
  {
    DruidKubernetesPeonClient peonClient = new DruidKubernetesPeonClient(
        new TestKubernetesClient(this.client), "test",
        false
    );
    Optional<InputStream> stream = peonClient.getPeonLogs(new K8sTaskId("foo"));
    Assertions.assertFalse(stream.isPresent());
  }

  @Test
  void testGetLogReaderForJob()
  {
    server.expect().get()
        .withPath("/apis/batch/v1/namespaces/test/jobs/foo")
        .andReturn(HttpURLConnection.HTTP_OK, new JobBuilder()
            .withNewMetadata()
            .withName("foo")
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
        .withPath("/api/v1/namespaces/test/pods?labelSelector=controller-uid%3Duid")
        .andReturn(HttpURLConnection.HTTP_OK, new PodListBuilder()
            .addNewItem()
            .withNewMetadata()
            .withName("foo")
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
        .withPath("/api/v1/namespaces/test/pods/foo/log?pretty=false&container=main")
        .andReturn(HttpURLConnection.HTTP_OK, "data")
        .once();

    DruidKubernetesPeonClient peonClient = new DruidKubernetesPeonClient(
        new TestKubernetesClient(client),
        "test",
        false
    );

    Optional<InputStream> logs = peonClient.getPeonLogs(new K8sTaskId("foo"));
    Assertions.assertTrue(logs.isPresent());
  }

  @Test
  void testGetLogReaderForJobWithoutPodReturnsEmptyOptional()
  {
    server.expect().get()
        .withPath("/apis/batch/v1/namespaces/test/jobs/foo")
        .andReturn(HttpURLConnection.HTTP_OK, new JobBuilder()
            .withNewMetadata()
            .withName("foo")
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

    DruidKubernetesPeonClient peonClient = new DruidKubernetesPeonClient(
        new TestKubernetesClient(client),
        "test",
        false
    );

    Optional<InputStream> logs = peonClient.getPeonLogs(new K8sTaskId("foo"));
    Assertions.assertFalse(logs.isPresent());
  }

  @Test
  void testGetLogReaderForJobWithoutJobReturnsEmptyOptional()
  {
    DruidKubernetesPeonClient peonClient = new DruidKubernetesPeonClient(
        new TestKubernetesClient(client),
        "test",
        false
    );

    Optional<InputStream> logs = peonClient.getPeonLogs(new K8sTaskId("foo"));
    Assertions.assertFalse(logs.isPresent());
  }

  private Job mockJob(boolean active, Timestamp timestamp)
  {
    Job job = mock(Job.class);
    JobStatus status = mock(JobStatus.class);
    if (active) {
      when(status.getActive()).thenReturn(1);
    } else {
      when(status.getActive()).thenReturn(null);
      when(status.getCompletionTime()).thenReturn(timestamp.toString());
    }
    when(job.getStatus()).thenReturn(status);
    return job;
  }
}
