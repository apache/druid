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
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.informers.cache.Store;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A KubernetesPeonClient implementation that uses shared informers to read Job and Pod state from a local cache.
 * <p>
 * This implementation greatly reduces load on the Kubernetes API server by centralizing watches and allowing
 * tasks to query cached resource state instead of making per-task API calls. Mutable operations (job creation,
 * deletion) still contact the API server directly.
 * </p>
 */
public class CachingKubernetesPeonClient extends KubernetesPeonClient
{
  protected static final EmittingLogger log = new EmittingLogger(CachingKubernetesPeonClient.class);

  private final DruidKubernetesCachingClient cachingClient;

  public CachingKubernetesPeonClient(
      DruidKubernetesCachingClient cachingClient,
      String namespace,
      String overlordNamespace,
      boolean debugJobs,
      ServiceEmitter emitter
  )
  {

    super(cachingClient.getBaseClient(), namespace, overlordNamespace == null ? "" : overlordNamespace, debugJobs, emitter);
    this.cachingClient = cachingClient;
  }

  @Override
  public JobResponse waitForPeonJobCompletion(K8sTaskId taskId, long howLong, TimeUnit unit)
  {
    final Duration timeout = Duration.millis(unit.toMillis(howLong));
    final Duration jobMustBeSeenWithin = Duration.millis(cachingClient.getInformerResyncPeriodMillis() * 2);
    final Stopwatch stopwatch = Stopwatch.createStarted();
    boolean jobSeenInCache = false;

    try {
      CompletableFuture<Job> jobFuture = null;
      while (stopwatch.hasNotElapsed(timeout) && (jobSeenInCache || stopwatch.hasNotElapsed(jobMustBeSeenWithin))) {
        if (jobFuture == null || jobFuture.isDone()) {
          // Register a future to watch the next change to this job
          jobFuture = cachingClient.waitForJobChange(taskId.getK8sJobName());
        }
        Optional<Job> maybeJob = getPeonJob(taskId.getK8sJobName());
        if (maybeJob.isPresent()) {
          jobSeenInCache = true;
          Job job = maybeJob.get();
          JobResponse currentResponse = determineJobResponse(job);
          if (currentResponse.getPhase() != PeonPhase.RUNNING) {
            return currentResponse;
          } else {
            log.debug("K8s job[%s] found in cache and is still running", taskId.getK8sJobName());
          }
        } else if (jobSeenInCache) {
          // Job was in cache before, but now it's gone - it was deleted and will never complete.
          log.warn("K8s Job[%s] was not found. It can happen if the task was canceled", taskId.getK8sJobName());
          return new JobResponse(null, PeonPhase.FAILED);
        } else {
          log.debug("K8s job[%s] not yet found in cache", taskId.getK8sJobName());
        }

        try {
          jobFuture.get(cachingClient.getInformerResyncPeriodMillis(), TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException | CancellationException e) {
          Throwable cause = e.getCause();
          if (cause instanceof CancellationException) {
            log.noStackTrace().warn("Job change watch for job[%s] was cancelled", taskId.getK8sJobName());
          } else {
            log.noStackTrace().warn(cause, "Exception while waiting for change notification of job[%s]", taskId.getK8sJobName());
          }
        }
        catch (TimeoutException e) {
          // No job change event notified within the timeout time. If there is more time, it will loop back and check the cache again.
          log.debug("Timeout waiting for change notification of job[%s].", taskId.getK8sJobName());
        }
        catch (InterruptedException e) {
          throw DruidException.defensive(e, "Interrupted waiting for job change notification for job[%s]", taskId.getK8sJobName());
        }
      }
    }
    finally {
      // Clean up: remove from map and cancel if still pending
      cachingClient.cancelJobWatcher(taskId.getK8sJobName());
    }

    log.warn("Timed out waiting for K8s job[%s] to complete", taskId.getK8sJobName());
    return new JobResponse(null, PeonPhase.FAILED);
  }

  @Override
  public List<Job> getPeonJobs()
  {
    if (overlordNamespace.isEmpty()) {
      return cachingClient.readJobCache(Store::list);
    } else {
      return cachingClient.readJobCache(
          indexer ->
              indexer.byIndex(DruidKubernetesCachingClient.OVERLORD_NAMESPACE_INDEX, overlordNamespace));
    }
  }

  @Override
  public Optional<Pod> getPeonPod(String jobName)
  {
    return cachingClient.readPodCache(indexer -> {
      List<Pod> pods = indexer.byIndex(DruidKubernetesCachingClient.JOB_NAME_INDEX, jobName);
      return pods.isEmpty() ? Optional.absent() : Optional.of(pods.get(0));
    });
  }

  public Optional<Job> getPeonJob(String jobName)
  {
    return cachingClient.readJobCache(indexer -> {
      List<Job> jobs = indexer.byIndex(DruidKubernetesCachingClient.JOB_NAME_INDEX, jobName);
      return jobs.isEmpty() ? Optional.absent() : Optional.of(jobs.get(0));
    });
  }

  @Override
  @Nullable
  protected Pod waitUntilPeonPodCreatedAndReady(String jobName, long howLong, TimeUnit timeUnit)
  {
    final Duration timeout = Duration.millis(timeUnit.toMillis(howLong));
    final Stopwatch stopwatch = Stopwatch.createStarted();

    try {
      CompletableFuture<Pod> podFuture = null;
      while (stopwatch.hasNotElapsed(timeout)) {
        if (podFuture == null || podFuture.isDone()) {
          // Register a future to watch the next change to this pod
          podFuture = cachingClient.waitForPodChange(jobName);
        }
        Optional<Pod> maybePod = getPeonPod(jobName);
        if (maybePod.isPresent()) {
          Pod pod = maybePod.get();
          String podName = pod.getMetadata() != null && pod.getMetadata().getName() != null
                    ? pod.getMetadata().getName()
                    : "unknown";
          if (isPodRunningOrComplete(pod)) {
            log.info("Pod[%s] for job[%s] is now in Running/Complete state", podName, jobName);
            return pod;
          } else {
            log.debug("Pod[%s] for job[%s] found in cache but not yet Running/Complete", podName, jobName);
          }
        } else {
          log.debug("Pod for job[%s] not yet found in cache", jobName);
        }

        try {
          podFuture.get(cachingClient.getInformerResyncPeriodMillis(), TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException | CancellationException e) {
          // This is unusual. Log warning but try to continue
          Throwable cause = e.getCause();
          if (cause instanceof CancellationException) {
            log.noStackTrace().warn("Pod change watch for job[%s] was cancelled", jobName);
          } else {
            log.noStackTrace().warn(cause, "Unexpected exception while waiting for pod change notification for job[%s]", jobName);
          }
        }
        catch (TimeoutException e) {
          // No pod change event notified within the timeout time. If there is more time, it will loop back and check the cache again.
          log.debug("Timeout waiting for change notification of pod for job[%s].", jobName);
        }
        catch (InterruptedException e) {
          throw DruidException.defensive(e, "Interrupted waiting for pod change notification for job[%s]", jobName);
        }
      }
    }
    finally {
      // Clean up: remove from map and cancel if still pending
      cachingClient.cancelPodWatcher(jobName);
    }
    log.warn("Timed out waiting for pod for job[%s] to be created and ready", jobName);
    return null;
  }

  /**
   * Check if the pod is in Running, Succeeded or Failed phase.
   */
  private boolean isPodRunningOrComplete(Pod pod)
  {
    // I could not find constants for Pod phases in fabric8, so hardcoding them here.
    // They are documented here: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase
    List<String> matchingPhases = List.of("Running", "Succeeded", "Failed");
    return pod.getStatus() != null && pod.getStatus().getPhase() != null &&
           matchingPhases.contains(pod.getStatus().getPhase());
  }

  /**
   * Determine the JobResponse based on the current state of the Job.
   */
  private JobResponse determineJobResponse(Job job)
  {
    if (job.getStatus() != null) {
      Integer active = job.getStatus().getActive();
      Integer succeeded = job.getStatus().getSucceeded();
      Integer failed = job.getStatus().getFailed();

      if ((active == null || active == 0) && (succeeded != null || failed != null)) {
        if (succeeded != null && succeeded > 0) {
          log.info("K8s job[%s] completed successfully", job.getMetadata().getName());
          return new JobResponse(job, PeonPhase.SUCCEEDED);
        } else {
          log.warn("K8s job[%s] failed with status %s", job.getMetadata().getName(), job.getStatus());
          return new JobResponse(job, PeonPhase.FAILED);
        }
      }
    }

    log.debug("K8s job[%s] is still active.", job.getMetadata().getName());
    return new JobResponse(job, PeonPhase.RUNNING);
  }
}
