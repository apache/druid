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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A KubernetesPeonClient implementation that directly queries the Kubernetes API server for Job and Pod state.
 * <p>
 * This implementation does not use caching and may put more load on the Kubernetes API server when many tasks
 * are running.
 * </p>
 */
public class DirectKubernetesPeonClient extends AbstractKubernetesPeonClient
{
  protected static final EmittingLogger log = new EmittingLogger(CachingKubernetesPeonClient.class);

  public DirectKubernetesPeonClient(
      KubernetesClientApi clientApi,
      String namespace,
      String overlordNamespace,
      boolean debugJobs,
      ServiceEmitter emitter
  )
  {
    super(clientApi, namespace, overlordNamespace, debugJobs, emitter);
  }

  public DirectKubernetesPeonClient(
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
    return clientApi.executeRequest(client -> {
      Job job = client.batch()
                      .v1()
                      .jobs()
                      .inNamespace(namespace)
                      .withName(taskId.getK8sJobName())
                      .waitUntilCondition(
                          x -> (x == null) || (x.getStatus() != null && x.getStatus().getActive() == null
                                               && (x.getStatus().getFailed() != null || x.getStatus().getSucceeded() != null)),
                          howLong,
                          unit
                      );
      if (job == null) {
        log.info("K8s job for the task [%s] was not found. It can happen if the task was canceled", taskId);
        return new JobResponse(null, PeonPhase.FAILED);
      }
      if (job.getStatus().getSucceeded() != null) {
        return new JobResponse(job, PeonPhase.SUCCEEDED);
      }
      log.warn("Task %s failed with status %s", taskId, job.getStatus());
      return new JobResponse(job, PeonPhase.FAILED);
    });
  }

  @Override
  public List<Job> getPeonJobs()
  {
    if (overlordNamespace.isEmpty()) {
      return clientApi.executeRequest(client -> client.batch()
                                                      .v1()
                                                      .jobs()
                                                      .inNamespace(namespace)
                                                      .withLabel(DruidK8sConstants.LABEL_KEY)
                                                      .list()
                                                      .getItems());
    } else {
      return clientApi.executeRequest(client -> client.batch()
                                                      .v1()
                                                      .jobs()
                                                      .inNamespace(namespace)
                                                      .withLabel(DruidK8sConstants.LABEL_KEY)
                                                      .withLabel(
                                                          DruidK8sConstants.OVERLORD_NAMESPACE_KEY,
                                                          overlordNamespace
                                                      )
                                                      .list()
                                                      .getItems());
    }
  }

  @Override
  public Optional<Pod> getPeonPod(String jobName)
  {
    return clientApi.executeRequest(client -> getPeonPod(client, jobName));
  }

  @Override
  public Optional<Job> getPeonJob(String jobName)
  {
    return clientApi.executeRequest(client -> getPeonJob(client, jobName));
  }

  @Nullable
  @Override
  protected Pod waitUntilPeonPodCreatedAndReady(String jobName, long howLong, TimeUnit timeUnit)
  {
    Pod pod = clientApi.executeRequest(client -> getPeonPodWithRetries(client, jobName, 5, RetryUtils.DEFAULT_MAX_TRIES));
    if (pod == null) {
      return null;
    }
    return clientApi.executeRequest(client -> waitForPodResultWithRetries(client, pod, howLong, timeUnit, 5, RetryUtils.DEFAULT_MAX_TRIES));

  }

  /**
   * Waits for a Kubernetes pod to reach a ready state with retry logic for transient connection pool exceptions.
   * <p>
   * This method waits for the specified pod to have a valid status with a pod IP assigned, indicating
   * it has been scheduled and is in a ready state. The method includes retry logic to handle transient
   * connection pool exceptions that may occur during the wait operation.
   * <p>
   * The method will wait up to the specified timeout for the pod to become ready, and retry the entire wait operation
   * if transient connection issues are encountered.
   *
   * @param client the Kubernetes client to use for pod operations
   * @param pod the pod to wait for
   * @param howLong the maximum time to wait for the pod to become ready
   * @param timeUnit the time unit for the wait timeout
   * @param quietTries number of initial retry attempts without logging warnings
   * @param maxTries maximum total number of retry attempts
   * @return the pod in its ready state, or null if the pod disappeared or wait operation failed
   * @throws DruidException if waiting fails after all retry attempts or encounters non-retryable errors
   */
  @VisibleForTesting
  Pod waitForPodResultWithRetries(KubernetesClient client, Pod pod, long howLong, TimeUnit timeUnit, int quietTries, int maxTries)
  {
    try {
      return RetryUtils.retry(
          () -> client.pods()
                      .inNamespace(namespace)
                      .withName(pod.getMetadata().getName())
                      .waitUntilCondition(
                          p -> {
                            if (p == null) {
                              return true;
                            }
                            return p.getStatus() != null && p.getStatus().getPodIP() != null;
                          }, howLong, timeUnit),
          this::isRetryableTransientConnectionPoolException, quietTries, maxTries);
    }
    catch (Exception e) {
      throw DruidException.defensive(e, "Error when waiting for pod[%s] to start", pod.getMetadata().getName());
    }
  }

  @VisibleForTesting
  Pod getPeonPodWithRetries(KubernetesClient client, String jobName, int quietTries, int maxTries)
  {
    try {
      return RetryUtils.retry(
          () -> {
            Optional<Pod> maybePod = getPeonPod(client, jobName);
            if (maybePod.isPresent()) {
              return maybePod.get();
            }

            // If the pod is missing, we can take a look at job events to discover potential problems with pod creation.
            List<Event> events = getPeonEvents(client, jobName);

            if (events.isEmpty()) {
              throw new KubernetesResourceNotFoundException("K8s pod with label[job-name=%s] not found", jobName);
            } else {
              Event latestEvent = events.get(events.size() - 1);
              throw new KubernetesResourceNotFoundException(
                  "Job[%s] failed to create pods. Message[%s]", jobName, latestEvent.getMessage());
            }
          },
          this::shouldRetryWaitForStartingPeonPod, quietTries, maxTries
      );
    }
    catch (KubernetesResourceNotFoundException e) {
      throw e;
    }
    catch (Exception e) {
      throw DruidException.defensive(e, "Error when looking for K8s pod with label[job-name=%s]", jobName);
    }
  }

  private Optional<Pod> getPeonPod(KubernetesClient client, String jobName)
  {
    List<Pod> pods = client.pods()
                           .inNamespace(namespace)
                           .withLabel("job-name", jobName)
                           .list()
                           .getItems();
    return pods.isEmpty() ? Optional.absent() : Optional.of(pods.get(0));
  }

  private Optional<Job> getPeonJob(KubernetesClient client, String jobName)
  {
    Job job = client.batch()
                    .v1()
                    .jobs()
                    .inNamespace(namespace)
                    .withName(jobName)
                    .get();
    return job == null ? Optional.absent() : Optional.of(job);
  }

  /**
   * Determines if this exception, specifically when containing Kubernetes job event messages, permits a retry attempt.
   * <p>
   * The method checks the exception message against a predefined list of Kubernetes event messages.
   * These substrings, found in {@link DruidK8sConstants#BLACKLISTED_PEON_POD_ERROR_MESSAGES},
   * represent Kubernetes event that indicate a retry for starting the Peon Pod would likely be futile.
   */
  private boolean shouldRetryWaitForStartingPeonPod(Throwable e)
  {
    if (isRetryableTransientConnectionPoolException(e)) {
      return true;
    }

    if (!(e instanceof KubernetesResourceNotFoundException)) {
      return false;
    }

    String errorMessage = e.getMessage();
    for (String blacklistedMessage : DruidK8sConstants.BLACKLISTED_PEON_POD_ERROR_MESSAGES) {
      if (errorMessage.contains(blacklistedMessage)) {
        return false;
      }
    }

    return true;
  }

  private List<Event> getPeonEvents(KubernetesClient client, String jobName)
  {
    ObjectReference objectReference = new ObjectReferenceBuilder()
        .withApiVersion("batch/v1")
        .withKind("Job")
        .withName(jobName)
        .withNamespace(this.namespace)
        .build();

    try {
      return client.v1()
                   .events()
                   .inNamespace(this.namespace)
                   .withInvolvedObject(objectReference)
                   .list()
                   .getItems();
    }
    catch (KubernetesClientException e) {
      log.warn("Failed to get events for job[%s]; %s", jobName, e.getMessage());
      return List.of();
    }
  }
}
