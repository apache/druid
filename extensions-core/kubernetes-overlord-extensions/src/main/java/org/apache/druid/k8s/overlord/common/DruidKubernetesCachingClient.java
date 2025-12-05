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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class DruidKubernetesCachingClient
{
  /**
   * Event types for Kubernetes informer resource events.
   */
  public enum InformerEventType
  {
    ADD,
    UPDATE,
    DELETE
  }

  /**
   * Impl of ResourceEventHandler that simplifies event handling
   * by accepting a single lambda BiConsumer for all event types (add, update, delete).
   *
   * @param <T> The Kubernetes resource type (e.g., Pod, Job)
   */
  public static class InformerEventHandler<T> implements ResourceEventHandler<T>
  {
    private final BiConsumer<T, InformerEventType> eventConsumer;

    public InformerEventHandler(BiConsumer<T, InformerEventType> eventConsumer)
    {
      this.eventConsumer = eventConsumer;
    }

    @Override
    public void onAdd(T resource)
    {
      eventConsumer.accept(resource, InformerEventType.ADD);
    }

    @Override
    public void onUpdate(T oldResource, T newResource)
    {
      eventConsumer.accept(newResource, InformerEventType.UPDATE);
    }

    @Override
    public void onDelete(T resource, boolean deletedFinalStateUnknown)
    {
      eventConsumer.accept(resource, InformerEventType.DELETE);
    }
  }
  private static final EmittingLogger log = new EmittingLogger(DruidKubernetesCachingClient.class);

  public static final String JOB_NAME_INDEX = "byJobName";
  public static final String OVERLORD_NAMESPACE_INDEX = "byOverlordNamespace";

  private static final long DEFAULT_INFORMER_RESYNC_PERIOD_MS = 300000L; // 5 minutes

  private final KubernetesClientApi baseClient;
  private final SharedIndexInformer<Pod> podInformer;
  private final SharedIndexInformer<Job> jobInformer;
  private final KubernetesResourceEventNotifier eventNotifier;
  private final long informerResyncPeriodMillis;

  public DruidKubernetesCachingClient(
      KubernetesClientApi baseClient,
      String namespace,
      long informerResyncPeriodMillis
  )
  {
    this.baseClient = baseClient;
    this.informerResyncPeriodMillis = informerResyncPeriodMillis;
    this.eventNotifier = new KubernetesResourceEventNotifier();

    this.podInformer = setupPodInformer(namespace);
    this.jobInformer = setupJobInformer(namespace);
  }

  public KubernetesClientApi getBaseClient()
  {
    return baseClient;
  }

  // Delegate write operations to base client
  public <T> T executeRequest(KubernetesExecutor<T> executor) throws KubernetesResourceNotFoundException
  {
    return baseClient.executeRequest(executor);
  }

  public KubernetesClient getClient()
  {
    return baseClient.getClient();
  }

  public <T> T readPodCache(SharedInformerCacheReadRequestExecutor<T, Pod> executor)
  {
    if (podInformer == null) {
      throw DruidException.defensive("Pod informer is not initialized, caching is disabled");
    }
    return executor.executeRequest(podInformer);
  }

  public <T> T readJobCache(SharedInformerCacheReadRequestExecutor<T, Job> executor)
  {
    if (jobInformer == null) {
      throw DruidException.defensive("Job informer is not initialized, caching is disabled");
    }
    return executor.executeRequest(jobInformer);
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
        baseClient.getClient().pods()
                        .inNamespace(namespace)
                        .withLabel(DruidK8sConstants.LABEL_KEY)
                        .inform(
                            new InformerEventHandler<>(
                                (pod, eventType) -> {
                                  log.debug("Pod[%s] got %s", pod.getMetadata().getName(), eventType.name().toLowerCase());
                                  notifyPodChange(pod);
                                }
                            ), informerResyncPeriodMillis
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
        baseClient.getClient().batch()
                        .v1()
                        .jobs()
                        .inNamespace(namespace)
                        .withLabel(DruidK8sConstants.LABEL_KEY)
                        .inform(
                            new InformerEventHandler<>(
                                (job, eventType) -> {
                                  log.debug("Job[%s] got %s", job.getMetadata().getName(), eventType.name().toLowerCase());
                                  eventNotifier.notifyJobChange(job.getMetadata().getName(), job);
                                }
                            ), informerResyncPeriodMillis
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

  public CompletableFuture<Job> waitForJobChange(String jobName)
  {
    return eventNotifier.waitForJobChange(jobName);
  }

  public CompletableFuture<Pod> waitForPodChange(String jobName)
  {
    return eventNotifier.waitForPodChange(jobName);
  }

  public long getInformerResyncPeriodMillis()
  {
    return informerResyncPeriodMillis;
  }
}
