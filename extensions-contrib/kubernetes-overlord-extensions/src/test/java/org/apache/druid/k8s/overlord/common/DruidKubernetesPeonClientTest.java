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
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@EnableKubernetesMockClient(crud = true)
public class DruidKubernetesPeonClientTest
{

  KubernetesClient client;

  @Test
  void testWaitingForAPodToGetReadyThatDoesntExist()
  {
    DruidKubernetesPeonClient client = new DruidKubernetesPeonClient(new TestKubernetesClient(this.client), "test",
                                                                     false
    );
    Assertions.assertThrows(KubernetesClientTimeoutException.class, () -> {
      client.waitForJobCompletion(new K8sTaskId("some-task"), 1, TimeUnit.SECONDS);
    });
  }

  @Test
  void testTheFlow()
  {
    DruidKubernetesPeonClient peonClient = new DruidKubernetesPeonClient(new TestKubernetesClient(this.client), "test",
                                                                         false
    );
    List<Job> currentJobs = peonClient.listAllPeonJobs();
    assertEquals(0, currentJobs.size());
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
    assertEquals(1, currentJobs.size());
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
    assertEquals(1, pods.size());
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
    assertEquals(1, toDelete.size()); // should only cleanup one job
    assertEquals(killThisOne, Iterables.getOnlyElement(toDelete)); // should only cleanup one job
  }

  @Test
  void watchingALogThatDoesntExist()
  {
    DruidKubernetesPeonClient peonClient = new DruidKubernetesPeonClient(
        new TestKubernetesClient(this.client), "test",
        false
    );
    Optional<InputStream> stream = peonClient.getPeonLogs(new K8sTaskId("foo"));
    assertFalse(stream.isPresent());

    String logs = peonClient.getJobLogs(new K8sTaskId("foo"));
    assertTrue(logs.startsWith("No logs found"));
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
