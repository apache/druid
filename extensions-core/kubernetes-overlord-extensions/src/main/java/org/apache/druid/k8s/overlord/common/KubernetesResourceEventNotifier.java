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
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages event notifications for Kubernetes resources (Jobs and Pods).
 * <p>
 * Allows tasks to wait for specific resource changes without polling, improving efficiency and responsiveness.
 * Crtical component of {@link CachingKubernetesPeonClient} functionality.
 * </p>
 */
public class KubernetesResourceEventNotifier
{
  private static final EmittingLogger log = new EmittingLogger(KubernetesResourceEventNotifier.class);

  private final ConcurrentHashMap<String, CompletableFuture<Job>> jobWatchers = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, CompletableFuture<Pod>> podWatchers = new ConcurrentHashMap<>();

  /**
   * Register to be notified when a job with the given name changes.
   * The returned future will complete when the job is added, updated, or deleted.
   *
   * @param jobName The name of the job to watch
   * @return A future that completes when the job changes
   */
  public CompletableFuture<Job> waitForJobChange(String jobName)
  {
    return jobWatchers.computeIfAbsent(jobName, k -> {
      log.debug("Creating new watcher for job [%s]", jobName);
      return new CompletableFuture<>();
    });

  }

  /**
   * Register to be notified when a pod for the given job name changes.
   * The returned future will complete when a pod with the job-name label changes.
   *
   * @param jobName The job-name label value to watch for
   * @return A future that completes when a matching pod changes
   */
  public CompletableFuture<Pod> waitForPodChange(String jobName)
  {
    return podWatchers.computeIfAbsent(jobName, k -> {
      log.debug("Creating new watcher for pod with job-name [%s]", jobName);
      return new CompletableFuture<>();
    });
  }

  /**
   * Notify all watchers that a job with the given name has changed and remove the watcher from the map.
   *
   * @param jobName The name of the job that changed
   */
  public void notifyJobChange(String jobName, Job job)
  {
    CompletableFuture<Job> future = jobWatchers.remove(jobName);
    if (future != null) {
      log.debug("Notifying watchers of job [%s] change", jobName);
      future.complete(job);
    }
  }

  /**
   * Notify all watchers that a pod for the given job name has changed and remove the watcher from the map.
   *
   * @param jobName The job-name label value that changed
   */
  public void notifyPodChange(String jobName, Pod pod)
  {
    CompletableFuture<Pod> future = podWatchers.remove(jobName);
    if (future != null) {
      log.debug("Notifying watchers of pod change for job-name [%s]", jobName);
      future.complete(pod);
    }

  }

  /**
   * Cancel all pending watchers. Used during shutdown.
   */
  public void cancelAll()
  {
    log.info("Cancelling all pending watchers");
    jobWatchers.values().forEach(f -> f.cancel(true));
    podWatchers.values().forEach(f -> f.cancel(true));
    jobWatchers.clear();
    podWatchers.clear();
  }
}
