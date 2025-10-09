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

/**
 * A KubernetesPeonClient implementation that uses cached informers to read Job and Pod state.
 * <p>
 * This reduces load on the Kubernetes API server by centralizing watches allowing tasks to query cached K8s resource
 * information.
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

    CompletableFuture<Job> jobFuture = clientApi.getEventNotifier().waitForJobChange(taskId.getK8sJobName());
    do {
      try {
        Optional<Job> maybeJob = getPeonJob(taskId.getK8sJobName());
        if (maybeJob.isPresent()) {
          Job job = maybeJob.get();
          JobResponse currentResponse = determineJobResponse(job);
          if (currentResponse.getPhase() != PeonPhase.RUNNING) {
            return currentResponse;
          }
        }
        Job job = jobFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
        // Immediately set up to watch for the next change in case we need to wait again
        jobFuture = clientApi.getEventNotifier().waitForJobChange(taskId.getK8sJobName());
        log.debug("Received job[%s] change notification", taskId.getK8sJobName());
        if (job == null) {
          log.warn("K8s job for the task[%s] was not found. It can happen if the task was canceled", taskId);
          return new JobResponse(null, PeonPhase.FAILED);
        }

        JobResponse currentResponse = determineJobResponse(job);
        if (currentResponse.getPhase() != PeonPhase.RUNNING) {
          return currentResponse;
        }
      }
      catch (Throwable e) {
        log.warn("Exception[%s] waiting for job change notification for job[%s]. Error message[%s]", e.getClass().getName(), taskId.getK8sJobName(), e.getMessage());
      }
    } while (System.currentTimeMillis() - startTime < timeoutMs);

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
          // Check if pod is ready (has IP)
          Pod pod = maybePod.get();
          if (isPodReady(pod)) {
            log.info("Pod[%s] for job[%s] is ready with IP[%s]", podName, jobName, pod.getStatus().getPodIP());
            return pod;
          } else {
            log.debug("Pod[%s] for job[%s] exists but not ready yet (no IP assigned)", podName, jobName);
          }
        }

        Pod pod = podFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
        podFuture = clientApi.getEventNotifier().waitForPodChange(jobName);
        log.debug("Received pod[%s] change notification for job[%s]", podName, jobName);
        if (pod == null) {
          throw DruidException.defensive("Pod[%s] for job[%s] is null. This is unusual. Investigate Druid and k8s logs.", podName, jobName);
        } else {
          podSeenInCache = true;
          if (isPodReady(pod)) {
            log.info("Pod[%s] for job[%s] is ready with IP[%s]", podName, jobName, pod.getStatus().getPodIP());
            return pod;
          } else {
            log.debug("Pod[%s] for job[%s] exists but not ready yet (no IP assigned)", podName, jobName);
          }
        }
      }
      catch (Throwable e) {
        log.warn("Exception[%s] waiting for pod change notification for job [%s]. Error message[%s]", e.getClass().getName(), jobName, e.getMessage());
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
   * Check if the pod is ready. For our purposes, this means it has been assigned an IP address.
   */
  private boolean isPodReady(Pod pod)
  {
    return pod.getStatus() != null && pod.getStatus().getPodIP() != null;
  }

  /**
   * Determine the JobResponse based on the current state of the Job.
   */
  private JobResponse determineJobResponse(Job job)
  {
    if (job.getStatus() != null &&
        (job.getStatus().getActive() == null ||  job.getStatus().getActive() == 0) &&
        (job.getStatus().getFailed() != null || job.getStatus().getSucceeded() != null)) {

      if (job.getStatus().getSucceeded() != null && job.getStatus().getSucceeded() > 0) {
        log.info("K8s job[%s] completed successfully", job.getMetadata().getName());
        return new JobResponse(job, PeonPhase.SUCCEEDED);
      } else {
        log.warn("K8s job[%s] failed with status %s", job.getMetadata().getName(), job.getStatus());
        return new JobResponse(job, PeonPhase.FAILED);
      }
    } else {
      log.debug("K8s job[%s] is still active.", job.getMetadata().getName());
      return new JobResponse(job, PeonPhase.RUNNING);
    }
  }
}
