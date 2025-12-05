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

import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.k8s.overlord.common.httpclient.DruidKubernetesHttpClientFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DruidKubernetesClient implements KubernetesClientApi
{
  private static final EmittingLogger log = new EmittingLogger(DruidKubernetesClient.class);

  public static final String JOB_NAME_INDEX = "byJobName";
  public static final String OVERLORD_NAMESPACE_INDEX = "byOverlordNamespace";

  public static final String ENABLE_INFORMERS_KEY = "druid.k8s.informers.enabled";
  public static final String INFORMER_RESYNC_PERIOD_MS_KEY = "druid.k8s.informers.resyncPeriodMs";
  private static final long DEFAULT_INFORMER_RESYNC_PERIOD_MS = 300000L; // 5 minutes

  private final KubernetesClient kubernetesClient;
  private final SharedIndexInformer<Pod> podInformer;
  private final SharedIndexInformer<Job> jobInformer;
  private final KubernetesResourceEventNotifier eventNotifier;
  private final long informerResyncPeriodMillis;

  public DruidKubernetesClient(DruidKubernetesHttpClientFactory httpClientFactory, Config kubernetesClientConfig)
  {
    this.kubernetesClient = new KubernetesClientBuilder()
        .withHttpClientFactory(httpClientFactory)
        .withConfig(kubernetesClientConfig)
        .build();

    // It is required that the config declares whether informers are enabled or not
    Preconditions.checkNotNull(kubernetesClientConfig.getAdditionalProperties().get(ENABLE_INFORMERS_KEY),
                               "Kubernetes client config must contain property [%s]",
                               ENABLE_INFORMERS_KEY);

    informerResyncPeriodMillis = (long) kubernetesClientConfig
        .getAdditionalProperties().getOrDefault(INFORMER_RESYNC_PERIOD_MS_KEY, DEFAULT_INFORMER_RESYNC_PERIOD_MS);
    if ((boolean) kubernetesClientConfig.getAdditionalProperties().get(ENABLE_INFORMERS_KEY)) {
      this.eventNotifier = new KubernetesResourceEventNotifier();
      this.podInformer = setupPodInformer(kubernetesClient.getNamespace());
      this.jobInformer = setupJobInformer(kubernetesClient.getNamespace());
    } else {
      this.eventNotifier = null;
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
  public KubernetesResourceEventNotifier getEventNotifier()
  {
    if (eventNotifier == null) {
      throw DruidException.defensive("Event notifier is not initialized, caching is disabled");
    }
    return eventNotifier;
  }

  /**
   * Sets up a shared informer to watch and cache Pod resources in the specified namespace.
   * <p>
   * Registers event handlers for pod add/update/delete events and creates a custom index by job-name
   * for efficient pod lookup by job.
   * </p>
   */
  private SharedIndexInformer<Pod> setupPodInformer(String namespace)
  {
    SharedIndexInformer<Pod> podInformer =
        kubernetesClient.pods()
                        .inNamespace(namespace)
                        .withLabel(DruidK8sConstants.LABEL_KEY)
                        .inform(
                            new ResourceEventHandler<>()
                            {
                              @Override
                              public void onAdd(Pod pod)
                              {
                                log.debug("Pod[%s] got added", pod.getMetadata().getName());
                                notifyPodChange(pod);
                              }

                              @Override
                              public void onUpdate(Pod oldPod, Pod newPod)
                              {
                                log.debug("Pod[%s] got updated", oldPod.getMetadata().getName());
                                notifyPodChange(newPod);
                              }

                              @Override
                              public void onDelete(Pod pod, boolean deletedFinalStateUnknown)
                              {
                                log.debug("Pod[%s] got deleted", pod.getMetadata().getName());
                                notifyPodChange(pod);
                              }
                            }, informerResyncPeriodMillis
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
    customPodIndexers.put(JOB_NAME_INDEX, jobNameIndexer);

    podInformer.addIndexers(customPodIndexers);
    return podInformer;
  }

  /**
   * Sets up a shared informer to watch and cache Job resources in the specified namespace.
   * <p>
   * Registers event handlers for job add/update/delete events and creates custom indexes by job-name
   * and overlord-namespace for efficient job lookup and filtering.
   * </p>
   */
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
                                log.debug("Job[%s] got added", job.getMetadata().getName());
                                eventNotifier.notifyJobChange(job.getMetadata().getName(), job);
                              }

                              @Override
                              public void onUpdate(Job oldJob, Job newJob)
                              {
                                log.debug("Job[%s] got updated", newJob.getMetadata().getName());
                                eventNotifier.notifyJobChange(newJob.getMetadata().getName(), newJob);
                              }

                              @Override
                              public void onDelete(Job job, boolean deletedFinalStateUnknown)
                              {
                                log.debug("Job[%s] got deleted", job.getMetadata().getName());
                                eventNotifier.notifyJobChange(job.getMetadata().getName(), job);
                              }
                            }, informerResyncPeriodMillis
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
    customJobIndexers.put(OVERLORD_NAMESPACE_INDEX, overlordNamespaceIndexer);
    customJobIndexers.put(JOB_NAME_INDEX, jobNameIndexer);

    jobInformer.addIndexers(customJobIndexers);

    return jobInformer;
  }

  /**
   * Utility method to only notify pod changes for pods that are part of indexing jobs.
   */
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
  public long getInformerResyncPeriodMillis()
  {
    if (jobInformer == null || podInformer == null) {
      throw DruidException.defensive("Informers are not initialized, caching is disabled");
    }
    return informerResyncPeriodMillis;
  }
}
