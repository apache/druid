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

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class TestCachingKubernetesClient extends DruidKubernetesCachingClient
{
  private final KubernetesClient client;
  private final SharedIndexInformer<Pod> podInformer;
  private final SharedIndexInformer<Job> jobInformer;
  private final KubernetesResourceEventNotifier eventNotifier;
  private final CountDownLatch syncLatch;

  public TestCachingKubernetesClient(KubernetesClientApi clientApi, String namespace)
  {
    super(clientApi, namespace, 1000L);
    this.client = clientApi.getClient();
    this.eventNotifier = new KubernetesResourceEventNotifier();
    this.syncLatch = new CountDownLatch(2); // Wait for both informers

    // Set up pod informer with real event handlers
    this.podInformer = client.pods()
                             .inNamespace(namespace)
                             .inform(
                                 new ResourceEventHandler<Pod>()
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
                                 }, 1000L
                             );

    // Add pod indexer
    Map<String, Function<Pod, List<String>>> podIndexers = new HashMap<>();
    podIndexers.put(
        "byJobName", pod -> {
          if (pod.getMetadata() != null && pod.getMetadata().getLabels() != null) {
            String jobName = pod.getMetadata().getLabels().get("job-name");
            if (jobName != null) {
              return Collections.singletonList(jobName);
            }
          }
          return Collections.emptyList();
        }
    );
    podInformer.addIndexers(podIndexers);

    // Set up job informer with real event handlers
    this.jobInformer = client.batch()
                             .v1()
                             .jobs()
                             .inNamespace(namespace)
                             .withLabel(DruidK8sConstants.LABEL_KEY)
                             .inform(
                                 new ResourceEventHandler<Job>()
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
                                 }, 1000L
                             );

    // Add job indexers
    Map<String, Function<Job, List<String>>> jobIndexers = new HashMap<>();
    jobIndexers.put(
        "byJobName", job -> {
          if (job.getMetadata() != null && job.getMetadata().getName() != null) {
            return Collections.singletonList(job.getMetadata().getName());
          }
          return Collections.emptyList();
        }
    );
    jobIndexers.put(
        "byOverlordNamespace", job -> {
          if (job.getMetadata() != null && job.getMetadata().getLabels() != null) {
            String overlordNamespace = job.getMetadata().getLabels().get(DruidK8sConstants.OVERLORD_NAMESPACE_KEY);
            if (overlordNamespace != null) {
              return Collections.singletonList(overlordNamespace);
            }
          }
          return Collections.emptyList();
        }
    );
    jobInformer.addIndexers(jobIndexers);
  }

  public void start()
  {
    // Add ready callbacks to count down latch
    podInformer.addEventHandlerWithResyncPeriod(
        new ResourceEventHandler<Pod>()
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
        }, 1000L
    );

    jobInformer.addEventHandlerWithResyncPeriod(
        new ResourceEventHandler<Job>()
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
        }, 1000L
    );

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
  public <T> T readPodCache(SharedInformerCacheReadRequestExecutor<T, Pod> executor)
  {
    return executor.executeRequest(podInformer);
  }

  @Override
  public <T> T readJobCache(SharedInformerCacheReadRequestExecutor<T, Job> executor)
  {
    return executor.executeRequest(jobInformer);
  }
}
