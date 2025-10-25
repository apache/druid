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
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

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
public class CachingKubernetesPeonClient extends AbstractKubernetesPeonClient
{
  protected static final EmittingLogger log = new EmittingLogger(CachingKubernetesPeonClient.class);

  public CachingKubernetesPeonClient(
      KubernetesClientApi clientApi,
      String namespace,
      String overlordNamespace,
      boolean debugJobs,
      ServiceEmitter emitter
  )
  {
    super(clientApi, namespace, overlordNamespace, debugJobs, emitter);
  }

  public CachingKubernetesPeonClient(
      KubernetesClientApi clientApi,
      String namespace,
      boolean debugJobs,
      ServiceEmitter emitter
  )
  {
    super(clientApi, namespace, "", debugJobs, emitter);
  }

  @Override
  public JobResponse waitForPeonJobCompletion(K8sTaskId taskId, long howLong, TimeUnit unit)
  {
    long timeoutMs = unit.toMillis(howLong);
    long startTime = System.currentTimeMillis();

    // Give the informer 2 resync periods to see the job. if it isn't seen by then, we assume the job was canceled.
    // This is to prevent us from waiting for entire max job runtime on a job that was canceled before it even started.
    long jobMustBeSeenBy = startTime + (clientApi.getInformerResyncPeriodMillis() * 2);
    boolean jobSeenInCache = false;

    CompletableFuture<Job> jobFuture = clientApi.getEventNotifier().waitForJobChange(taskId.getK8sJobName());
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
        Job job = jobFuture.get(clientApi.getInformerResyncPeriodMillis(), TimeUnit.MILLISECONDS);

        // Immediately set up to watch for the next change in case we need to wait again
        jobFuture = clientApi.getEventNotifier().waitForJobChange(taskId.getK8sJobName());
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
        log.debug("Timeout waiting for job change notification for job[%s], checking cache again", taskId.getK8sJobName());
      }
      catch (InterruptedException e) {
        throw DruidException.defensive(e, "Interrupted waiting for job change notification for job[%s]", taskId.getK8sJobName());
      }
      catch (Throwable e) {
        log.warn("Exception[%s] waiting for job change notification for job[%s]. Error message[%s]", e.getClass().getName(), taskId.getK8sJobName(), e.getMessage());
      }
    } while ((System.currentTimeMillis() - startTime < timeoutMs) && (jobSeenInCache || System.currentTimeMillis() < jobMustBeSeenBy));

    log.warn("Timed out waiting for K8s job[%s] to complete", taskId.getK8sJobName());
    return new JobResponse(null, PeonPhase.FAILED);
  }

  @Override
  public List<Job> getPeonJobs()
  {
    if (overlordNamespace.isEmpty()) {
      return clientApi.executeJobCacheRequest(informer -> informer.getIndexer().list());
    } else {
      return clientApi.executeJobCacheRequest(informer ->
                                                  informer.getIndexer()
                                                          .byIndex("byOverlordNamespace", overlordNamespace));
    }
  }

  @Override
  public Optional<Pod> getPeonPod(String jobName)
  {
    return clientApi.executePodCacheRequest(informer -> {
      List<Pod> pods = informer.getIndexer().byIndex("byJobName", jobName);
      return pods.isEmpty() ? Optional.absent() : Optional.of(pods.get(0));
    });
  }

  @Override
  public Optional<Job> getPeonJob(String jobName)
  {
    return clientApi.executeJobCacheRequest(informer -> {
      List<Job> jobs = informer.getIndexer().byIndex("byJobName", jobName);
      return jobs.isEmpty() ? Optional.absent() : Optional.of(jobs.get(0));
    });
  }

  @Override
  @Nullable
  protected Pod waitUntilPeonPodCreatedAndReady(String jobName, long howLong, TimeUnit timeUnit)
  {
    long timeoutMs = timeUnit.toMillis(howLong);
    long startTime = System.currentTimeMillis();

    String podName = "unknown";
    boolean podSeenInCache = false;
    CompletableFuture<Pod> podFuture = clientApi.getEventNotifier().waitForPodChange(jobName);
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
        Pod pod = podFuture.get(clientApi.getInformerResyncPeriodMillis(), TimeUnit.MILLISECONDS);

        podFuture = clientApi.getEventNotifier().waitForPodChange(jobName);
        log.debug("Received pod[%s] change notification for job[%s]", podName, jobName);
        if (pod == null) {
          throw DruidException.defensive("Pod[%s] for job[%s] is null. This is unusual. Investigate Druid and k8s logs.", podName, jobName);
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
        log.debug("Timeout waiting for pod change notification for job[%s], checking cache again", jobName);
      }
      catch (InterruptedException e) {
        throw DruidException.defensive(e, "Interrupted waiting for pod change notification for job[%s]", jobName);
      }
      catch (Throwable e) {
        log.warn("Unexpected exception[%s] waiting for pod change notification for job [%s]. Error message[%s]", e.getClass().getName(), jobName, e.getMessage());
      }
    } while (System.currentTimeMillis() - startTime < timeoutMs);

    // Timeout
    if (podSeenInCache) {
      log.warn("Timeout waiting for pod[%s] for job[%s] to become ready", podName, jobName);
      return null;
    } else {
      throw DruidException.defensive("Timeout waiting for pod for job[%s] to be created", jobName);
    }
  }

  /**
   * Check if the pod is in Running, Succeeded or Failed phase.
   */
  private boolean isPodRunningOrComplete(Pod pod)
  {
    // I could not find constants for Pod phases in fabric8, so hardcoding them here.
    // They are documented here: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase
    List<String> matchingPhases = List.of("Running", "Succeeded", "Failed");
    return pod.getStatus() != null &&
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
