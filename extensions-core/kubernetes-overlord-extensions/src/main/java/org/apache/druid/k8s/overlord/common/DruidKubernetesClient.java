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
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DruidKubernetesClient implements KubernetesClientApi
{
  private static final EmittingLogger log = new EmittingLogger(DruidKubernetesClient.class);

  private static final long INFORMER_RESYNC_PERIOD_MS = 30 * 1000L; // todo make this configurable by operator

  private final KubernetesClient kubernetesClient;
  private final SharedIndexInformer<Pod> podInformer;
  private final SharedIndexInformer<Job> jobInformer;

  public DruidKubernetesClient(
      DruidKubernetesHttpClientConfig httpClientConfig,
      Config kubernetesClientConfig,
      boolean enableCache
  )
  {
    this.kubernetesClient = new KubernetesClientBuilder()
        .withHttpClientFactory(new DruidKubernetesHttpClientFactory(httpClientConfig))
        .withConfig(kubernetesClientConfig)
        .build();
    if (enableCache) {
      this.podInformer = setupPodInformer(kubernetesClient.getNamespace());
      this.jobInformer = setupJobInformer(kubernetesClient.getNamespace());
    } else {
      this.podInformer = null;
      this.jobInformer = null;
    }
  }

  @Override
  public <T> T executeRequest(KubernetesExecutor<T> executor) throws KubernetesResourceNotFoundException
  {
    return executor.executeRequest(kubernetesClient);
  }

  @Override
  public <T> T executePodCacheRequest(KubernetesInformerExecutor<T, Pod> executor)
  {
    if (podInformer == null) {
      throw DruidException.defensive("Pod informer is not initialized, caching is disabled");
    }
    return executor.executeRequest(podInformer);
  }

  @Override
  public <T> T executeJobCacheRequest(KubernetesInformerExecutor<T, Job> executor)
  {
    if (jobInformer == null) {
      throw DruidException.defensive("Job informer is not initialized, caching is disabled");
    }
    return executor.executeRequest(jobInformer);
  }


  /**
   * This client automatically gets closed by the druid lifecycle, it should not be closed when used as it is
   * meant to be reused.
   *
   * @return re-useable KubernetesClient
   */
  @Override
  public KubernetesClient getClient()
  {
    return this.kubernetesClient;
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

  private SharedIndexInformer<Pod> setupPodInformer(String namespace)
  {
    SharedIndexInformer<Pod> podInformer =
        kubernetesClient.pods()
                        .inNamespace(namespace)
                        .inform(
                            new ResourceEventHandler<>()
                            {
                              @Override
                              public void onAdd(Pod pod)
                              {
                                log.info("Pod " + pod.getMetadata().getName() + " got added");
                              }

                              @Override
                              public void onUpdate(Pod oldPod, Pod newPod)
                              {
                                log.info("Pod " + oldPod.getMetadata().getName() + " got updated");
                              }

                              @Override
                              public void onDelete(Pod pod, boolean deletedFinalStateUnknown)
                              {
                                log.info("Pod " + pod.getMetadata().getName() + " got deleted");
                              }
                            }, INFORMER_RESYNC_PERIOD_MS
                        );

    Function<Pod, List<String>> jobNameIndexer = pod -> {
      if (pod.getMetadata() != null && pod.getMetadata().getLabels() != null) {
        String jobName = pod.getMetadata().getLabels().get("job-name");
        if (jobName != null) {
          return Collections.singletonList(jobName);
        }
      }
      return Collections.emptyList();
    };

    Map<String, Function<Pod, List<String>>> customPodIndexers = new HashMap<>();
    customPodIndexers.put("byJobName", jobNameIndexer);

    podInformer.addIndexers(customPodIndexers);
    return podInformer;
  }

  private SharedIndexInformer<Job> setupJobInformer(String namespace)
  {
    SharedIndexInformer<Job> jobInformer =
        kubernetesClient.batch()
                        .v1()
                        .jobs()
                        .inNamespace(namespace)
                        .withLabel(DruidK8sConstants.LABEL_KEY)
                        .inform(
                            new ResourceEventHandler<>()
                            {
                              @Override
                              public void onAdd(Job job)
                              {
                                log.info("Job " + job.getMetadata().getName() + " got added");
                              }

                              @Override
                              public void onUpdate(Job oldJob, Job newJob)
                              {
                                log.info("Job " + oldJob.getMetadata().getName() + " got updated");
                              }

                              @Override
                              public void onDelete(Job job, boolean deletedFinalStateUnknown)
                              {
                                log.info("Job " + job.getMetadata().getName() + " got deleted");
                              }
                            }, INFORMER_RESYNC_PERIOD_MS
                        );

    Function<Job, List<String>> overlordNamespaceIndexer = job -> {
      if (job.getMetadata() != null && job.getMetadata().getLabels() != null) {
        String overlordNamespace = job.getMetadata().getLabels().get(DruidK8sConstants.OVERLORD_NAMESPACE_KEY);
        if (overlordNamespace != null) {
          return Collections.singletonList(overlordNamespace);
        }
      }
      return Collections.emptyList();
    };

    Function<Job, List<String>> jobNameIndexer = job -> {
      if (job.getMetadata() != null && job.getMetadata().getName() != null) {
        return Collections.singletonList(job.getMetadata().getName());
      }
      return Collections.emptyList();
    };

    Map<String, Function<Job, List<String>>> customJobIndexers = new HashMap<>();
    customJobIndexers.put("byOverlordNamespace", overlordNamespaceIndexer);
    customJobIndexers.put("byJobName", jobNameIndexer);

    jobInformer.addIndexers(customJobIndexers);

    return jobInformer;
  }
}
