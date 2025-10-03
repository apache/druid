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
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.vertx.core.http.HttpClosedException;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KubernetesPeonClient
{
  private static final EmittingLogger log = new EmittingLogger(KubernetesPeonClient.class);

  private final KubernetesClientApi clientApi;
  private final String namespace;
  private final String overlordNamespace;
  private final boolean debugJobs;
  private final ServiceEmitter emitter;
  private final boolean useEventsAnalysisOnPodNotFound = false;

  public KubernetesPeonClient(
      KubernetesClientApi clientApi,
      String namespace,
      String overlordNamespace,
      boolean debugJobs,
      ServiceEmitter emitter
  )
  {
    this.clientApi = clientApi;
    this.namespace = namespace;
    this.overlordNamespace = overlordNamespace;
    this.debugJobs = debugJobs;
    this.emitter = emitter;
  }

  public KubernetesPeonClient(
      KubernetesClientApi clientApi,
      String namespace,
      boolean debugJobs,
      ServiceEmitter emitter
  )
  {
    this(clientApi, namespace, "", debugJobs, emitter);
  }

  public Pod launchPeonJobAndWaitForStart(Job job, Task task, long howLong, TimeUnit timeUnit) throws IllegalStateException
  {
    long start = System.currentTimeMillis();
    // launch job
    return clientApi.executeRequest(client -> {
      String jobName = job.getMetadata().getName();

      log.info("Submitting job[%s] for task[%s].", jobName, task.getId());
      createK8sJobWithRetries(job);
      log.info("Submitted job[%s] for task[%s]. Waiting for POD to launch.", jobName, task.getId());

      // Wait for the pod to be available
      Pod mainPod = getPeonPodWithRetries(jobName);
      log.info("Pod for job[%s] launched for task[%s]. Waiting for pod to be in running state.", jobName, task.getId());

      // Wait for the pod to be in state running, completed, or failed.
      Pod result = waitForPodResultWithRetries(mainPod, howLong, timeUnit);

      if (result == null) {
        throw new ISE("K8s pod for the task [%s] appeared and disappeared. It can happen if the task was canceled", task.getId());
      }
      log.info("Pod for job[%s] is in state [%s] for task[%s].", jobName, result.getStatus().getPhase(), task.getId());
      long duration = System.currentTimeMillis() - start;
      emitK8sPodMetrics(task, "k8s/peon/startup/time", duration);
      return result;
    });
  }

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
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        continue;
      }

      Job job = clientApi.executeJobCacheRequest((informer) ->
                                                     informer.getIndexer()
                                                             .byIndex("byOverlordNamespace", overlordNamespace).stream()
                                                             .filter(j -> taskId.getK8sJobName().equals(j.getMetadata().getName()))
                                                             .findFirst()
                                                             .orElse(null));

      if (job == null) {
        long elapsed = System.currentTimeMillis() - startTime;

        // Give grace period for job to appear in cache after creation
        if (!jobSeenInCache && elapsed < jobAppearanceGracePeriodMs) {
          log.debug("Job [%s] not yet in cache, waiting... (elapsed: %d ms)", taskId, elapsed);
          try {
            Thread.sleep(pollInterval);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
          continue;
        }

        // After grace period or if we've seen it before, job is truly missing
        log.info("K8s job for the task [%s] was not found. It can happen if the task was canceled", taskId);
        return new JobResponse(null, PeonPhase.FAILED);
      }

      // Job found! Mark that we've seen it
      jobSeenInCache = true;

      // Check if job is complete
      if (job.getStatus().getActive() == null || job.getStatus().getActive() == 0) {
        if (job.getStatus().getSucceeded() != null) {
          return new JobResponse(job, PeonPhase.SUCCEEDED);
        }
        log.warn("Task %s failed with status %s", taskId, job.getStatus());
        return new JobResponse(job, PeonPhase.FAILED);
      }

      // Job still running, wait and check again
      try {
        Thread.sleep(pollInterval);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    } while (System.currentTimeMillis() - startTime < timeoutMs);

    log.warn("Timed out waiting for job [%s] to complete", taskId);
    return new JobResponse(null, PeonPhase.FAILED);
  }

  public boolean deletePeonJob(K8sTaskId taskId)
  {
    if (!debugJobs) {
      Boolean result = clientApi.executeRequest(client -> !client.batch()
                                                                 .v1()
                                                                 .jobs()
                                                                 .inNamespace(namespace)
                                                                 .withName(taskId.getK8sJobName())
                                                                 .delete().isEmpty());
      if (result) {
        log.info("Cleaned up k8s job: %s", taskId);
      } else {
        log.info("K8s job does not exist: %s", taskId);
      }
      return result;
    } else {
      log.info("Not cleaning up job %s due to flag: debugJobs=true", taskId);
      return true;
    }
  }

  public Optional<LogWatch> getPeonLogWatcher(K8sTaskId taskId)
  {
    KubernetesClient k8sClient = clientApi.getClient();
    try {
      LogWatch logWatch = k8sClient.batch()
          .v1()
          .jobs()
          .inNamespace(namespace)
          .withName(taskId.getK8sJobName())
          .inContainer("main")
          .watchLog();
      if (logWatch == null) {
        return Optional.absent();
      }
      return Optional.of(logWatch);
    }
    catch (Exception e) {
      log.error(e, "Error watching logs from task: %s", taskId);
      return Optional.absent();
    }
  }

  public Optional<InputStream> getPeonLogs(K8sTaskId taskId)
  {
    KubernetesClient k8sClient = clientApi.getClient();
    try {
      InputStream logStream = k8sClient.batch()
                                   .v1()
                                   .jobs()
                                   .inNamespace(namespace)
                                   .withName(taskId.getK8sJobName())
                                   .inContainer("main")
                                   .getLogInputStream();
      if (logStream == null) {
        return Optional.absent();
      }
      return Optional.of(logStream);
    }
    catch (Exception e) {
      log.error(e, "Error streaming logs from task: %s", taskId);
      return Optional.absent();
    }
  }

  public List<Job> getPeonJobs(boolean useCache)
  {
    if (useCache) {
      if (!clientApi.getJobInformer().hasSynced()) {
        log.warn("K8s job informer cache not synced, getting jobs directly from k8s");
        useCache = false;
      }
    }

    return this.overlordNamespace.isEmpty()
           ? getPeonJobsWithoutOverlordNamespaceKeyLabels(useCache)
           : getPeonJobsWithOverlordNamespaceKeyLabels(useCache);
  }

  private List<Job> getPeonJobsWithoutOverlordNamespaceKeyLabels(boolean useCache)
  {
    if (useCache) {
      return clientApi.executeJobCacheRequest(informer -> informer.getIndexer().list());
    } else {
      return clientApi.executeRequest(client -> client.batch()
                                                      .v1()
                                                      .jobs()
                                                      .inNamespace(namespace)
                                                      .withLabel(DruidK8sConstants.LABEL_KEY)
                                                      .list()
                                                      .getItems());
    }
  }

  private List<Job> getPeonJobsWithOverlordNamespaceKeyLabels(boolean useCache)
  {
    if (useCache) {
      return clientApi.executeJobCacheRequest(informer -> informer.getIndexer()
                                                                  .byIndex("byOverlordNamespace", overlordNamespace));
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

  public int deleteCompletedPeonJobsOlderThan(long howFarBack, TimeUnit timeUnit)
  {
    AtomicInteger numDeleted = new AtomicInteger();
    return clientApi.executeRequest(client -> {
      List<Job> jobs = getJobsToCleanup(getPeonJobs(true), howFarBack, timeUnit);
      jobs.forEach(x -> {
        if (!client.batch()
                   .v1()
                   .jobs()
                   .inNamespace(namespace)
                   .withName(x.getMetadata().getName())
                   .delete().isEmpty()) {
          numDeleted.incrementAndGet();
        } else {
          log.error("Failed to delete job %s", x.getMetadata().getName());
        }
      });
      return numDeleted.get();
    });
  }

  private List<Job> getJobsToCleanup(List<Job> candidates, long howFarBack, TimeUnit timeUnit)
  {
    List<Job> toDelete = new ArrayList<>();
    long cutOff = System.currentTimeMillis() - timeUnit.toMillis(howFarBack);
    candidates.forEach(x -> {
      // jobs that are complete
      if (x.getStatus().getActive() == null) {
        Timestamp timestamp = Timestamp.valueOf(x.getStatus().getCompletionTime());
        if (timestamp.before(new Timestamp(cutOff))) {
          toDelete.add(x);
        }
      }
    });
    return toDelete;
  }

  public Optional<Pod> getPeonPod(String jobName)
  {
    return clientApi.executePodCacheRequest(informer -> getPeonPod(informer, jobName));
  }

  private Optional<Pod> getPeonPod(SharedIndexInformer informer, String jobName)
  {
    List<Pod> pods = informer.getIndexer().byIndex("byJobName", jobName);
    return pods.isEmpty() ? Optional.absent() : Optional.of(pods.get(0));
  }

  public Pod waitForPodResultWithRetries(final Pod pod, long howLong, TimeUnit timeUnit)
  {
    return clientApi.executePodCacheRequest(informer -> waitForPodResultWithRetries(informer, pod, howLong, timeUnit, 5, RetryUtils.DEFAULT_MAX_TRIES));
  }

  public Pod getPeonPodWithRetries(String jobName)
  {
    return clientApi.executePodCacheRequest(informer -> getPeonPodWithRetries(informer, jobName, 5, RetryUtils.DEFAULT_MAX_TRIES));
  }

  public void createK8sJobWithRetries(Job job)
  {
    clientApi.executeRequest(client -> {
      createK8sJobWithRetries(client, job, 5, RetryUtils.DEFAULT_MAX_TRIES);
      return null;
    });
  }

  /**
   * Creates a Kubernetes job with retry logic for transient connection pool exceptions.
   * <p>
   * This method attempts to create the specified job in Kubernetes with built-in retry logic
   * for transient connection pool issues. If the job already exists (HTTP 409 conflict),
   * the method returns successfully without throwing an exception, assuming the job was
   * already submitted by a previous request.
   * <p>
   * The retry logic only applies to transient connection pool exceptions. Other exceptions will cause the method to
   * fail immediately.
   *
   * @param client the Kubernetes client to use for job creation
   * @param job the Kubernetes job to create
   * @param quietTries number of initial retry attempts without logging warnings
   * @param maxTries maximum total number of retry attempts
   * @throws DruidException if job creation fails after all retry attempts or encounters non-retryable errors
   */
  @VisibleForTesting
  void createK8sJobWithRetries(KubernetesClient client, Job job, int quietTries, int maxTries)
  {
    try {
      RetryUtils.retry(
          () -> {
            try {
              client.batch()
                    .v1()
                    .jobs()
                    .inNamespace(namespace)
                    .resource(job)
                    .create();
              return null;
            }
            catch (KubernetesClientException e) {
              if (e.getCode() == 409) {
                // Job already exists, return successfully
                log.info("K8s job[%s] already exists, skipping creation", job.getMetadata().getName());
                return null;
              }
              throw e;
            }
          },
          this::isRetryableTransientConnectionPoolException, quietTries, maxTries
      );
    }
    catch (Exception e) {
      throw DruidException.defensive(e, "Error when creating K8s job[%s]", job.getMetadata().getName());
    }
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
   * @param pod        the pod to wait for
   * @param howLong    the maximum time to wait for the pod to become ready
   * @param timeUnit   the time unit for the wait timeout
   * @param quietTries number of initial retry attempts without logging warnings
   * @param maxTries   maximum total number of retry attempts
   * @return the pod in its ready state, or null if the pod disappeared or wait operation failed
   * @throws DruidException if waiting fails after all retry attempts or encounters non-retryable errors
   */
  @VisibleForTesting
  Pod waitForPodResultWithRetries(SharedIndexInformer<Pod> informer, Pod pod, long howLong, TimeUnit timeUnit, int quietTries, int maxTries)
  {
    try {
      return RetryUtils.retry(
          () -> waitForPodResultUsingCache(informer, pod, howLong, timeUnit),
          this::isRetryableTransientConnectionPoolException,
          quietTries,
          maxTries
      );
    } catch (Exception e) {
      throw DruidException.defensive(e, "Error when waiting for pod[%s] to start", pod.getMetadata().getName());
    }
  }

  /**
   * Retrieves the pod associated with a Kubernetes job with retry logic for transient failures.
   * <p>
   * This method searches for a pod with the specified job name label and includes retry logic
   * to handle both transient connection pool exceptions and cases where the pod may not be
   * immediately available after job creation. If no pod is found, the method examines job
   * events to provide detailed error information about pod creation failures.
   * <p>
   * The retry logic applies to:
   * <ul>
   *   <li>Transient connection pool exceptions</li>
   *   <li>Pod not found scenarios, except when blacklisted error messages from {@link DruidK8sConstants#BLACKLISTED_PEON_POD_ERROR_MESSAGES} are encountered</li>
   * </ul>
   *
   * @param informer the Kubernetes informer to use for pod and event operations
   * @param jobName the name of the job whose pod should be retrieved
   * @param quietTries number of initial retry attempts without logging warnings
   * @param maxTries maximum total number of retry attempts
   * @return the pod associated with the job
   * @throws KubernetesResourceNotFoundException if the pod cannot be found after all retry attempts
   * @throws DruidException if retrieval fails due to other errors
   */
  @VisibleForTesting
  Pod getPeonPodWithRetries(SharedIndexInformer<Pod> informer, String jobName, int quietTries, int maxTries)
  {
    try {
      return RetryUtils.retry(
          () -> {
            Optional<Pod> maybePod = getPeonPod(informer, jobName);
            if (maybePod.isPresent()) {
              return maybePod.get();
            }

            List<Event> events;
            if (useEventsAnalysisOnPodNotFound) {
              // If the pod is missing, we can take a look at job events to discover potential problems with pod creation.
              // This is an optional analysis step as it requires an additional API call and may not be desirable in all environments.
              events = clientApi.executeRequest((client) -> getPeonEvents(client, jobName));
            } else {
              events = List.of();
            }

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

  /**
   * Checks if the exception is a potentially transient connection pool exception.
   * <p>
   * This method checks if the exception is one of the known transient connection pool exceptions
   * and whether it contains a specific message substring, if applicable.
   * <p>
   * We have experienced connections in the pool being closed by the server-side but remaining in the pool. These issues
   * should be safe to retry in many cases.
   */
  private boolean isRetryableTransientConnectionPoolException(Throwable e)
  {
    if (e instanceof KubernetesClientException) {
      return e.getMessage() != null && e.getMessage().contains("Connection was closed");
    } else if (e instanceof HttpClosedException) {
      return true;
    }
    return false;
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

  private Pod waitForPodResultUsingCache(SharedIndexInformer<Pod> informer, Pod pod, long howLong, TimeUnit timeUnit)
  {
    log.info("Waiting for pod[%s] to be in running state using pod cache", pod.getMetadata().getName());
    long timeoutMs = timeUnit.toMillis(howLong);
    long startTime = System.currentTimeMillis();
    long pollInterval = 2000; // Poll every 2 seconds

    String podName = pod.getMetadata().getName();

    do {
      if (informer.hasSynced()) {
        // Wait for informer to sync
        try {
          Thread.sleep(pollInterval);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        continue;
      }

      Pod currentPod = informer.getIndexer().list().stream()
                               .filter(p -> podName.equals(p.getMetadata().getName()))
                               .findFirst()
                               .orElse(null);
      if (currentPod == null) {
        // Pod disappeared
        return null;
      }

      // Check if pod is ready (has IP)
      if (currentPod.getStatus() != null && currentPod.getStatus().getPodIP() != null) {
        return currentPod;
      }

      // Wait before polling again
      long remainingTime = timeoutMs - (System.currentTimeMillis() - startTime);
      if (remainingTime <= 0) {
        break;
      }

      try {
        Thread.sleep(Math.min(pollInterval, remainingTime));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    } while (System.currentTimeMillis() - startTime < timeoutMs);

    // Timeout - return null
    return null;
  }


  private void emitK8sPodMetrics(Task task, String metric, long durationMs)
  {
    ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);
    emitter.emit(metricBuilder.setMetric(metric, durationMs));
  }
}
