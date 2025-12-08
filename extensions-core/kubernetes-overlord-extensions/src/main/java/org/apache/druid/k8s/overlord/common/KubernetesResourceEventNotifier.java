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
 * Critical component of {@link CachingKubernetesPeonClient} functionality.
 * </p>
 * <p>
 * This implementation assumes only one waiter per job/pod at a time. If a new waiter is registered for a job that
 * already has one, the previous waiter will be cancelled.
 * </p>
 */
public class KubernetesResourceEventNotifier
{
  private static final EmittingLogger log = new EmittingLogger(KubernetesResourceEventNotifier.class);

  private final ConcurrentHashMap<String, CompletableFuture<Job>> jobWatchers = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, CompletableFuture<Pod>> podWatchers = new ConcurrentHashMap<>();

  /**
   * Register to be notified when a job with the given name changes.
   * <p>
   * IMPORTANT: Callers must call {@link #cancelJobWatcher(String)} when done waiting to avoid resource leaks.
   *
   * @param jobName The name of the job to watch
   * @return A future that completes when the job changes
   */
  public CompletableFuture<Job> waitForJobChange(String jobName)
  {
    CompletableFuture<Job> future = new CompletableFuture<>();
    CompletableFuture<Job> previous = jobWatchers.put(jobName, future);

    if (previous != null && !previous.isDone()) {
      log.warn("Replacing active watcher for job [%s] - multiple waiters detected", jobName);
      previous.cancel(true);
    }

    log.debug("Registered watcher for job [%s]", jobName);
    return future;
  }

  /**
   * Register to be notified when a pod for the given job name changes.
   * <p>
   * IMPORTANT: Callers must call {@link #cancelPodWatcher(String)} when done waiting to avoid resource leaks.
   *
   * @param jobName The job-name label value to watch for
   * @return A future that completes when a matching pod changes
   */
  public CompletableFuture<Pod> waitForPodChange(String jobName)
  {
    CompletableFuture<Pod> future = new CompletableFuture<>();
    CompletableFuture<Pod> previous = podWatchers.put(jobName, future);

    if (previous != null && !previous.isDone()) {
      log.warn("Replacing active watcher for pod with job-name [%s] - multiple waiters detected", jobName);
      previous.cancel(true);
    }

    log.debug("Registered watcher for pod with job-name [%s]", jobName);
    return future;
  }

  /**
   * Cancel and remove a job watcher. Safe to call even if the future has already completed.
   *
   * @param jobName The name of the job to stop watching
   */
  public void cancelJobWatcher(String jobName)
  {
    CompletableFuture<Job> future = jobWatchers.remove(jobName);
    if (future != null && !future.isDone()) {
      log.debug("Cancelling watcher for job [%s]", jobName);
      future.cancel(true);
    }
  }

  /**
   * Cancel and remove a pod watcher. Safe to call even if the future has already completed.
   *
   * @param jobName The job-name label value to stop watching
   */
  public void cancelPodWatcher(String jobName)
  {
    CompletableFuture<Pod> future = podWatchers.remove(jobName);
    if (future != null && !future.isDone()) {
      log.debug("Cancelling watcher for pod with job-name [%s]", jobName);
      future.cancel(true);
    }
  }

  /**
   * Notify the waiter that a job with the given name has changed.
   * Completes the future and removes it from the map.
   *
   * @param jobName The name of the job that changed
   * @param job The job that changed
   */
  public void notifyJobChange(String jobName, Job job)
  {
    CompletableFuture<Job> future = jobWatchers.remove(jobName);
    if (future != null) {
      log.debug("Notifying watcher of job [%s] change", jobName);
      future.complete(job);
    }
  }

  /**
   * Notify the waiter that a pod for the given job name has changed.
   * Completes the future and removes it from the map.
   *
   * @param jobName The job-name label value that changed
   * @param pod The pod that changed
   */
  public void notifyPodChange(String jobName, Pod pod)
  {
    CompletableFuture<Pod> future = podWatchers.remove(jobName);
    if (future != null) {
      log.debug("Notifying watcher of pod change for job-name [%s]", jobName);
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
