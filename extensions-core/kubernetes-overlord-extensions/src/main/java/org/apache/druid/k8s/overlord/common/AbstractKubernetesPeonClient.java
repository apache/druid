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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.vertx.core.http.HttpClosedException;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for Kubernetes peon clients, providing common functionality for mutable and log related operations
 */
public abstract class AbstractKubernetesPeonClient
{
  protected static final EmittingLogger log = new EmittingLogger(AbstractKubernetesPeonClient.class);

  protected final KubernetesClientApi clientApi;
  protected final String namespace;
  protected final String overlordNamespace;
  private final boolean debugJobs;
  private final ServiceEmitter emitter;

  AbstractKubernetesPeonClient(
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

  /**
   * Wait for the K8s job associated with the given taskId to complete, or until the given timeout is reached.
   * <p>
   * If the job completes, the {@link JobResponse} is returned in accordance with success or failure. If the timeout
   * is reached before job completion, a FAILED {@link JobResponse} is returned.
   * </p>
   *
   * @param taskId K8sTaskId representing the job to wait for
   * @param howLong maximum time to wait
   * @param unit time unit for the timeout
   * @return {@link JobResponse} indicating the result of the job
   */
  public abstract JobResponse waitForPeonJobCompletion(K8sTaskId taskId, long howLong, TimeUnit unit);

  /**
   * Get the list of all peon jobs in the namespace that this client is associated with.
   *
   * @return List of {@link Job} objects representing the peon jobs
   */
  public abstract List<Job> getPeonJobs();

  /**
   * Get the Pod associated with the given job name, if it exists
   *
   * @return an Optional containing the {@link Pod} if it exists, or absent if not found
   */
  public abstract Optional<Pod> getPeonPod(String jobName);

  /**
   * Get the Job with the given name, if it exists
   *
   * @return an Optional containing the {@link Job} if it exists, or absent if not found
   */
  public abstract Optional<Job> getPeonJob(String jobName);

  /**
   * Waits until a pod for the given job is created and ready to be monitored.
   * <p>
   * A pod can appear and dissapear in some cases, such as the task being canceled. In this case, null is returned and
   * the caller should handle accordingly.
   * </p>
   *
   * @param jobName  the name of the job whose pod we're waiting for
   * @param howLong  the maximum time to wait
   * @param timeUnit the time unit for the timeout
   * @return the {@link Pod} which was waited for or null if the pod appeared and dissapeared
   * @throws DruidException if the pod never appears within the timeout period
   */
  @Nullable
  protected abstract Pod waitUntilPeonPodCreatedAndReady(String jobName, long howLong, TimeUnit timeUnit);

  /**
   * Launches the given Kubernetes job for the specified task and waits for its associated pod to be created and ready.:w
   *
   * @param job {@link Job} being launched in k8s
   * @param task {@link Task} indexing task associated with the underlying job
   * @param howLong maximum time to wait for the pod to be created and ready to monitor
   * @param timeUnit time unit for the timeout
   * @return the {@link Pod} associated with the launched job once it is created and ready
   */
  public Pod launchPeonJobAndWaitForStart(Job job, Task task, long howLong, TimeUnit timeUnit)
  {
    long start = System.currentTimeMillis();

    // launch job
    String jobName = job.getMetadata().getName();
    log.info("Submitting job[%s] for task[%s].", jobName, task.getId());
    createK8sJobWithRetries(job);
    log.info("Submitted job[%s] for task[%s]. Waiting for POD to launch and be ready.", jobName, task.getId());

    // Wait for the Pod to be created and then reach ready state
    Pod result = waitUntilPeonPodCreatedAndReady(jobName, howLong, timeUnit);

    // Evaluate result of job launch
    if (result == null) {
      throw new ISE(
          "K8s pod for the task[%s] appeared and disappeared. It can happen if the task was canceled",
          task.getId()
      );
    }
    log.info("Pod for job[%s] is in state[%s] for task[%s].", jobName, result.getStatus().getPhase(), task.getId());
    long duration = System.currentTimeMillis() - start;
    emitK8sPodMetrics(task, "k8s/peon/startup/time", duration);
    return result;
  }

  /**
   * Deletes the Kubernetes job associated with the given taskId.
   * <p>
   * If the debugJobs flag is set to true, the job will not be deleted and a log message will be emitted instead.
   * </p>
   *
   * @return true if the job was deleted successfully or debugJobs is true, false if the job did not exist
   */
  public boolean deletePeonJob(K8sTaskId taskId)
  {
    if (!debugJobs) {
      Optional<Job> maybeJob = getPeonJob(taskId.getK8sJobName());
      if (!maybeJob.isPresent()) {
        log.info("Asked to delete a k8s job[%s] for task[%s] that does not exist?", taskId.getK8sJobName(), taskId.getOriginalTaskId());
        return false;
      }
      Job job = maybeJob.get();

      Boolean result = clientApi.executeRequest(client -> !client.batch()
                                                                                .v1()
                                                                                .jobs()
                                                                                .inNamespace(namespace)
                                                                                .resource(job)
                                                                                .delete().isEmpty());
      if (result) {
        log.info("Deleted k8s job[%s] for task[%s]", taskId.getK8sJobName(), taskId.getOriginalTaskId());
      } else {
        log.info("Asked to delete a k8s job[%s] for task[%s] that does not exist?", taskId.getK8sJobName(), taskId.getOriginalTaskId());
      }
      return result;
    } else {
      log.info("Not cleaning up job %s due to flag: debugJobs=true", taskId);
      return true;
    }
  }

  /**
   * Get a LogWatch for the peon pod associated with the given taskId. Create it if it does not already exist.
   * <p>
   * Any issues creating the LogWatch will be logged and an absent Optional will be returned.
   * </p>
   *
   * @return an Optional containing the {@link LogWatch} if it exists or was created.
   */
  public Optional<LogWatch> getPeonLogWatcher(K8sTaskId taskId)
  {
    Optional<Pod> maybePod = getPeonPod(taskId.getK8sJobName());
    if (!maybePod.isPresent()) {
      log.debug("Pod for job[%s] not found in cache, cannot watch logs", taskId.getK8sJobName());
      return Optional.absent();
    }

    Pod pod = maybePod.get();
    String podName = pod.getMetadata().getName();

    KubernetesClient k8sClient = clientApi.getClient();
    try {
      LogWatch logWatch = k8sClient.pods()
                                   .inNamespace(namespace)
                                   .resource(pod)
                                   .inContainer("main")
                                   .watchLog();
      if (logWatch == null) {
        return Optional.absent();
      }
      return Optional.of(logWatch);
    }
    catch (Exception e) {
      log.error(e, "Error watching logs from task: %s, pod: %s", taskId, podName);
      return Optional.absent();
    }
  }

  /**
   * Get an InputStream for the logs of the peon pod associated with the given taskId.
   * <p>
   * Any issues creating the InputStream will be logged and an absent Optional will be returned.
   * </p>
   *
   * @return an Optional containing the {@link InputStream} if the pod exists and logs could be streamed, or absent otherwise
   */
  public Optional<InputStream> getPeonLogs(K8sTaskId taskId)
  {
    Optional<Pod> maybePod = getPeonPod(taskId.getK8sJobName());
    if (!maybePod.isPresent()) {
      log.debug("Pod for job[%s] not found in cache, cannot stream logs", taskId.getK8sJobName());
      return Optional.absent();
    }

    Pod pod = maybePod.get();
    String podName = pod.getMetadata().getName();

    KubernetesClient k8sClient = clientApi.getClient();
    try {
      InputStream logStream = k8sClient.pods()
                                       .inNamespace(namespace)
                                       .resource(pod)
                                       .inContainer("main")
                                       .getLogInputStream();
      if (logStream == null) {
        return Optional.absent();
      }
      return Optional.of(logStream);
    }
    catch (Exception e) {
      log.error(e, "Error streaming logs for pod[%s] associated with task[%s]", podName, taskId.getOriginalTaskId());
      return Optional.absent();
    }
  }

  /**
   * Delete completed k8s jobs older than the specified time duration.
   *
   * @return the number of k8s jobs deleted
   */
  public int deleteCompletedPeonJobsOlderThan(long howFarBack, TimeUnit timeUnit)
  {
    AtomicInteger numDeleted = new AtomicInteger();
    return clientApi.executeRequest(client -> {
      List<Job> jobs = getJobsToCleanup(getPeonJobs(), howFarBack, timeUnit);
      jobs.forEach(job -> {
        if (!client.batch()
                   .v1()
                   .jobs()
                   .inNamespace(namespace)
                   .resource(job)
                   .delete().isEmpty()) {
          numDeleted.incrementAndGet();
        } else {
          log.error("Failed to delete k8s job[%s] during completed job cleanup", job.getMetadata().getName());
        }
      });
      return numDeleted.get();
    });
  }

  /**
   * Get the list of jobs to clean up based on their completion time.
   *
   * @return List of {@link Job} objects that are ready for cleanup
   */
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
   * @param client     the Kubernetes client to use for job creation
   * @param job        the Kubernetes job to create
   * @param quietTries number of initial retry attempts without logging warnings
   * @param maxTries   maximum total number of retry attempts
   * @throws DruidException if job creation fails after all retry attempts or encounters non-retryable errors
   */
  private void createK8sJobWithRetries(KubernetesClient client, Job job, int quietTries, int maxTries)
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
   * Checks if the exception is a potentially transient connection pool exception.
   * <p>
   * This method checks if the exception is one of the known transient connection pool exceptions
   * and whether it contains a specific message substring, if applicable.
   * <p>
   * We have experienced connections in the pool being closed by the server-side but remaining in the pool. These issues
   * should be safe to retry because even when making mutable calls to create jobs, the k8s control plane API has
   * gaurds in place preventind duplicate jobs with same job name.
   */
  protected boolean isRetryableTransientConnectionPoolException(Throwable e)
  {
    if (e instanceof KubernetesClientException) {
      return e.getMessage() != null && e.getMessage().contains("Connection was closed");
    } else if (e instanceof HttpClosedException) {
      return true;
    }
    return false;
  }

  private void emitK8sPodMetrics(Task task, String metric, long durationMs)
  {
    ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);
    emitter.emit(metricBuilder.setMetric(metric, durationMs));
  }
}
