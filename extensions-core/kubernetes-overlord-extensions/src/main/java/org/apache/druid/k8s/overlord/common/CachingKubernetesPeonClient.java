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

import java.util.List;
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
    long pollInterval = 5000;
    long jobAppearanceGracePeriodMs = 90000; // 90 seconds grace for job to appear in cache

    boolean jobSeenInCache = false;

    do {
      if (!clientApi.getJobInformer().hasSynced()) {
        // Checking before the informer has synced will likely result in a false negative.
        try {
          Thread.sleep(pollInterval);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        continue;
      }

      Job job;
      if (!overlordNamespace.isEmpty()) {
        job = clientApi.executeJobCacheRequest((informer) ->
                                                   informer.getIndexer()
                                                           .byIndex("byOverlordNamespace", overlordNamespace).stream()
                                                           .filter(j -> taskId.getK8sJobName()
                                                                              .equals(j.getMetadata().getName()))
                                                           .findFirst()
                                                           .orElse(null));
      } else {
        job = clientApi.executeJobCacheRequest(informer ->
                                                   informer.getIndexer()
                                                           .byIndex("byJobName", taskId.getK8sJobName())
                                                           .stream()
                                                           .findFirst()
                                                           .orElse(null));
      }

      if (job == null) {
        long elapsed = System.currentTimeMillis() - startTime;

        // Give grace period for job to appear in cache after creation
        if (!jobSeenInCache && elapsed < jobAppearanceGracePeriodMs) {
          log.debug("Job [%s] not yet in cache, waiting... (elapsed: %d ms)", taskId, elapsed);
          try {
            Thread.sleep(pollInterval);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
          continue;
        }

        // After grace period or if we've seen it before, job is truly missing
        log.warn("K8s job for the task [%s] was not found. It can happen if the task was canceled", taskId);
        return new JobResponse(null, PeonPhase.FAILED);
      }

      // Job found! Mark that we've seen it
      jobSeenInCache = true;

      // Check if job is complete
      if (job.getStatus().getActive() == null || job.getStatus().getActive() == 0) {
        if (job.getStatus().getSucceeded() > 0) {
          log.info("K8s job [%s] completed successfully", taskId);
          return new JobResponse(job, PeonPhase.SUCCEEDED);
        }
        log.warn("K8s job [%s] failed with status %s", taskId, job.getStatus());
        return new JobResponse(job, PeonPhase.FAILED);
      }

      // Job still running, wait and check again
      try {
        Thread.sleep(pollInterval);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    } while (System.currentTimeMillis() - startTime < timeoutMs);

    log.warn("Timed out waiting for K8s job [%s] to complete", taskId);
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
  protected Pod waitUntilPeonPodCreatedAndReady(String jobName, long howLong, TimeUnit timeUnit)
  {
    return clientApi.executePodCacheRequest(informer -> {
      long timeoutMs = timeUnit.toMillis(howLong);
      long startTime = System.currentTimeMillis();
      long pollInterval = 2000; // Poll every 2 seconds

      boolean podSeenInCache = false;
      String podName = null;

      do {
        if (!informer.hasSynced()) {
          // Wait for informer to sync
          try {
            Thread.sleep(pollInterval);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
          continue;
        }

        List<Pod> pods = informer.getIndexer().byIndex("byJobName", jobName);

        if (pods.isEmpty()) {
          // If we've seen the pod before, and now it's gone, it was deleted
          if (podSeenInCache) {
            log.warn("Pod for job[%s] disappeared after being seen in cache", jobName);
            return null;
          }
          // Otherwise keep waiting for it to appear
        } else {
          Pod currentPod = pods.get(0);
          podSeenInCache = true;
          podName = currentPod.getMetadata().getName();

          // Check if pod is ready (has IP)
          if (currentPod.getStatus() != null && currentPod.getStatus().getPodIP() != null) {
            log.info("Pod[%s] for job[%s] is ready with IP: %s", podName, jobName, currentPod.getStatus().getPodIP());
            return currentPod;
          }

          log.debug("Pod[%s] for job[%s] exists but not ready yet (no IP assigned)", podName, jobName);
        }

        // Wait before polling again
        long remainingTime = timeoutMs - (System.currentTimeMillis() - startTime);
        if (remainingTime <= 0) {
          break;
        }

        try {
          Thread.sleep(Math.min(pollInterval, remainingTime));
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      } while (System.currentTimeMillis() - startTime < timeoutMs);

      // Timeout
      if (podSeenInCache) {
        log.warn("Timeout waiting for pod[%s] for job[%s] to become ready", podName, jobName);
        return null;
      } else {
        throw DruidException.defensive("Timeout waiting for pod for job[%s] to be created", jobName);
      }
    });
  }

}
