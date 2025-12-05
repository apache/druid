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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
    Duration timeout = Duration.millis(unit.toMillis(howLong));
    Duration jobMustBeSeenWithin = Duration.millis(cachingClient.getInformerResyncPeriodMillis() * 2);
    Stopwatch stopwatch = Stopwatch.createStarted();
    boolean jobSeenInCache = false;

    // Set up to watch for job changes
    CompletableFuture<Job> jobFuture = cachingClient.getEventNotifier().waitForJobChange(taskId.getK8sJobName());

    // We will loop until the full timeout is reached if the job is seen in cache. If the job does not show up in the cache we will exit earlier.
    // In this loop we first check the cache to see if our job is there and complete. This avoids missing notifications that happened before we set up the watch.
    // If the job is not complete we wait for a notification of a job change or a timeout.
    // If it is a timeout, we loop back to check the cache again.
    // If it is a job change notification, we check the job state and exit if complete, or loop again if still running.
    do {
      try {
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
        }

        // We wake up every informer resync period to avoid event notifier misses.
        Job job = jobFuture.get(cachingClient.getInformerResyncPeriodMillis(), TimeUnit.MILLISECONDS);

        // Immediately set up to watch for the next change in case we need to wait again
        jobFuture = cachingClient.getEventNotifier().waitForJobChange(taskId.getK8sJobName());
        log.debug("Received job[%s] change notification", taskId.getK8sJobName());
        jobSeenInCache = true;

        if (job == null) {
          log.warn("K8s job[%s] was not found. It can happen if the task was canceled", taskId.getK8sJobName());
          return new JobResponse(null, PeonPhase.FAILED);
        }

        JobResponse currentResponse = determineJobResponse(job);
        if (currentResponse.getPhase() != PeonPhase.RUNNING) {
          return currentResponse;
        } else {
          log.debug("K8s job[%s] is still running", taskId.getK8sJobName());
        }
      }
      catch (TimeoutException e) {
        // A timeout here is not a problem, it forces us to loop around and check the cache again.
        // This prevents the case where we miss a notification and wait forever.
        log.debug("Timeout waiting for change notification of job[%s]. Waiting until full job timeout.", taskId.getK8sJobName());
      }
      catch (InterruptedException e) {
        throw DruidException.defensive(e, "Interrupted waiting for job change notification for job[%s]", taskId.getK8sJobName());
      }
      catch (Throwable e) {
        log.noStackTrace().warn(e, "Exception while waiting for change notification of job[%s]", taskId.getK8sJobName());
      }
    } while (stopwatch.hasNotElapsed(timeout) && (jobSeenInCache || stopwatch.hasNotElapsed(jobMustBeSeenWithin)));

    log.warn("Timed out waiting for K8s job[%s] to complete", taskId.getK8sJobName());
    return new JobResponse(null, PeonPhase.FAILED);
  }

  @Override
  public List<Job> getPeonJobs()
  {
    if (overlordNamespace.isEmpty()) {
      return cachingClient.executeJobCacheRequest(informer -> informer.getIndexer().list());
    } else {
      return cachingClient.executeJobCacheRequest(informer ->
                                                  informer.getIndexer()
                                                          .byIndex(DruidKubernetesCachingClient.OVERLORD_NAMESPACE_INDEX, overlordNamespace));
    }
  }

  @Override
  public Optional<Pod> getPeonPod(String jobName)
  {
    return cachingClient.executePodCacheRequest(informer -> {
      List<Pod> pods = informer.getIndexer().byIndex(DruidKubernetesCachingClient.JOB_NAME_INDEX, jobName);
      return pods.isEmpty() ? Optional.absent() : Optional.of(pods.get(0));
    });
  }

  public Optional<Job> getPeonJob(String jobName)
  {
    return cachingClient.executeJobCacheRequest(informer -> {
      List<Job> jobs = informer.getIndexer().byIndex(DruidKubernetesCachingClient.JOB_NAME_INDEX, jobName);
      return jobs.isEmpty() ? Optional.absent() : Optional.of(jobs.get(0));
    });
  }

  @Nullable
  protected Pod waitUntilPeonPodCreatedAndReady(String jobName, long howLong, TimeUnit timeUnit)
  {
    Duration timeout = Duration.millis(timeUnit.toMillis(howLong));
    Stopwatch stopwatch = Stopwatch.createStarted();
    String podName = "unknown";
    boolean podSeenInCache = false;

    // Set up to watch for pod changes
    CompletableFuture<Pod> podFuture = cachingClient.getEventNotifier().waitForPodChange(jobName);

    // We will loop until the specified timeout is reached, or we see the pod become ready, whichever comes first.
    // We eagerly check the cache first to avoid missing notifications that happened before we set up the watch.
    // If the pod is not ready we wait for a notification of a pod change or a timeout.
    // If it is a timeout, we loop back to check the cache again (if there is time)
    // If it is a pod change notification, we check the pod state and exit if ready, or loop again if still not ready.
    do {
      try {
        // First check to see if pod is already in cache and ready in case our completion future started after the update event fired
        Optional<Pod> maybePod = getPeonPod(jobName);
        if (maybePod.isPresent()) {
          podSeenInCache = true;
          Pod pod = maybePod.get();
          podName = pod.getMetadata().getName();

          if (isPodRunningOrComplete(pod)) {
            log.info("Pod[%s] for job[%s] is running or complete", podName, jobName);
            return pod;
          } else {
            log.debug("Pod[%s] for job[%s] exists but not ready yet", podName, jobName);
          }
        } else {
          log.info("Pod for job[%s] not created yet", jobName);
        }

        // We wake up every informer resync period to avoid event notifier misses.
        Pod pod = podFuture.get(cachingClient.getInformerResyncPeriodMillis(), TimeUnit.MILLISECONDS);

        podFuture = cachingClient.getEventNotifier().waitForPodChange(jobName);
        log.debug("Received pod[%s] change notification for job[%s]", podName, jobName);
        if (pod == null) {
          log.warn("Pod[%s] for job[%s] is null. This is unusual. Investigate Druid and k8s logs.", podName, jobName);
          return null;
        } else {
          podSeenInCache = true;
          podName = pod.getMetadata().getName();
          if (isPodRunningOrComplete(pod)) {
            log.info("Pod[%s] for job[%s] is running or complete", podName, jobName);
            return pod;
          } else {
            log.debug("Pod[%s] for job[%s] exists but not ready yet", podName, jobName);
          }
        }
      }
      catch (TimeoutException e) {
        // A timeout here is not a problem, it forces us to loop around and check the cache again.
        // This prevents the case where we miss a notification and wait forever.
        log.debug("Timeout waiting for pod change notification for job[%s], If full timeout has not been reached, the pod startup wait will continue", jobName);
      }
      catch (InterruptedException e) {
        throw DruidException.defensive(e, "Interrupted waiting for pod change notification for job[%s]", jobName);
      }
      catch (Throwable e) {
        log.warn("Unexpected exception[%s] waiting for pod change notification for job [%s]. Error message[%s]", e.getClass().getName(), jobName, e.getMessage());
      }
    } while (stopwatch.hasNotElapsed(timeout));

    if (podSeenInCache) {
      log.warn("Timeout waiting for pod[%s] for job[%s] to become ready after it was created", podName, jobName);
    } else {
      log.warn("Timeout waiting for pod for job[%s] to be created", jobName);
    }
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
