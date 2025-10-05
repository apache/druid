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
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Indexer;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

@EnableKubernetesMockClient(crud = true)
public class CachingKubernetesPeonClientTest
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
  private CachingKubernetesPeonClient instance;
  private StubServiceEmitter serviceEmitter;

  @BeforeEach
  public void setup()
  {
    serviceEmitter = new StubServiceEmitter("service", "host");
  }

  @Test
  void test_getPeonPod_withPodInCache_returnsPresentOptional()
  {
    // Create mocks
    SharedIndexInformer<Pod> podInformer = EasyMock.createMock(SharedIndexInformer.class);
    Indexer<Pod> indexer = EasyMock.createMock(Indexer.class);

    Pod pod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .addToLabels("job-name", KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    // Set up expectations
    EasyMock.expect(podInformer.getIndexer()).andReturn(indexer);
    EasyMock.expect(indexer.byIndex("byJobName", KUBERNETES_JOB_NAME))
            .andReturn(List.of(pod));

    EasyMock.replay(podInformer, indexer);

    clientApi = new TestKubernetesClient(this.client, podInformer, null);
    instance = new CachingKubernetesPeonClient(clientApi, NAMESPACE, false, serviceEmitter);

    Optional<Pod> result = instance.getPeonPod(KUBERNETES_JOB_NAME);

    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(POD_NAME, result.get().getMetadata().getName());

    EasyMock.verify(podInformer, indexer);
  }

  @Test
  void test_getPeonPod_withoutPodInCache_returnsAbsentOptional()
  {
    SharedIndexInformer<Pod> podInformer = EasyMock.createMock(SharedIndexInformer.class);
    Indexer<Pod> indexer = EasyMock.createMock(Indexer.class);

    EasyMock.expect(podInformer.getIndexer()).andReturn(indexer);
    EasyMock.expect(indexer.byIndex("byJobName", KUBERNETES_JOB_NAME))
            .andReturn(Collections.emptyList());

    EasyMock.replay(podInformer, indexer);

    clientApi = new TestKubernetesClient(this.client, podInformer, null);
    instance = new CachingKubernetesPeonClient(clientApi, NAMESPACE, false, serviceEmitter);

    Optional<Pod> result = instance.getPeonPod(KUBERNETES_JOB_NAME);

    Assertions.assertFalse(result.isPresent());

    EasyMock.verify(podInformer, indexer);
  }

  @Test
  void test_getPeonPod_withMultiplePodsForSameJob_returnsFirstOne()
  {
    SharedIndexInformer<Pod> podInformer = EasyMock.createMock(SharedIndexInformer.class);
    Indexer<Pod> indexer = EasyMock.createMock(Indexer.class);

    Pod pod1 = new PodBuilder()
        .withNewMetadata()
        .withName("pod-1")
        .addToLabels("job-name", KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    Pod pod2 = new PodBuilder()
        .withNewMetadata()
        .withName("pod-2")
        .addToLabels("job-name", KUBERNETES_JOB_NAME)
        .endMetadata()
        .build();

    EasyMock.expect(podInformer.getIndexer()).andReturn(indexer);
    EasyMock.expect(indexer.byIndex("byJobName", KUBERNETES_JOB_NAME))
            .andReturn(List.of(pod1, pod2));

    EasyMock.replay(podInformer, indexer);

    clientApi = new TestKubernetesClient(this.client, podInformer, null);
    instance = new CachingKubernetesPeonClient(clientApi, NAMESPACE, false, serviceEmitter);

    Optional<Pod> result = instance.getPeonPod(KUBERNETES_JOB_NAME);

    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals("pod-1", result.get().getMetadata().getName());

    EasyMock.verify(podInformer, indexer);
  }

  @Test
  void test_getPeonJobs_withoutOverlordNamespace_returnsAllJobsFromCache()
  {
    SharedIndexInformer<Job> jobInformer = EasyMock.createMock(SharedIndexInformer.class);
    Indexer<Job> indexer = EasyMock.createMock(Indexer.class);

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .endMetadata()
        .build();

    EasyMock.expect(jobInformer.getIndexer()).andReturn(indexer);
    EasyMock.expect(indexer.list()).andReturn(List.of(job));

    EasyMock.replay(jobInformer, indexer);

    clientApi = new TestKubernetesClient(this.client, null, jobInformer);
    instance = new CachingKubernetesPeonClient(clientApi, NAMESPACE, false, serviceEmitter);

    List<Job> jobs = instance.getPeonJobs();

    Assertions.assertEquals(1, jobs.size());
    Assertions.assertEquals(KUBERNETES_JOB_NAME, jobs.get(0).getMetadata().getName());

    EasyMock.verify(jobInformer, indexer);
  }

  @Test
  void test_getPeonJobs_withOverlordNamespace_returnsFilteredJobs()
  {
    SharedIndexInformer<Job> jobInformer = EasyMock.createMock(SharedIndexInformer.class);
    Indexer<Job> indexer = EasyMock.createMock(Indexer.class);

    Job job = new JobBuilder()
        .withNewMetadata()
        .withName(KUBERNETES_JOB_NAME)
        .addToLabels(DruidK8sConstants.LABEL_KEY, "true")
        .addToLabels(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, "overlord-ns")
        .endMetadata()
        .build();

    EasyMock.expect(jobInformer.getIndexer()).andReturn(indexer);
    EasyMock.expect(indexer.byIndex("byOverlordNamespace", "overlord-ns"))
            .andReturn(List.of(job));

    EasyMock.replay(jobInformer, indexer);

    clientApi = new TestKubernetesClient(this.client, null, jobInformer);
    instance = new CachingKubernetesPeonClient(clientApi, NAMESPACE, "overlord-ns", false, serviceEmitter);

    List<Job> jobs = instance.getPeonJobs();

    Assertions.assertEquals(1, jobs.size());
    Assertions.assertEquals(KUBERNETES_JOB_NAME, jobs.get(0).getMetadata().getName());

    EasyMock.verify(jobInformer, indexer);
  }

  @Test
  void test_getPeonJobs_whenCacheEmpty_returnsEmptyList()
  {
    SharedIndexInformer<Job> jobInformer = EasyMock.createMock(SharedIndexInformer.class);
    Indexer<Job> indexer = EasyMock.createMock(Indexer.class);

    EasyMock.expect(jobInformer.getIndexer()).andReturn(indexer);
    EasyMock.expect(indexer.list()).andReturn(Collections.emptyList());

    EasyMock.replay(jobInformer, indexer);

    clientApi = new TestKubernetesClient(this.client, null, jobInformer);
    instance = new CachingKubernetesPeonClient(clientApi, NAMESPACE, false, serviceEmitter);

    List<Job> jobs = instance.getPeonJobs();

    Assertions.assertEquals(0, jobs.size());

    EasyMock.verify(jobInformer, indexer);
  }
}
