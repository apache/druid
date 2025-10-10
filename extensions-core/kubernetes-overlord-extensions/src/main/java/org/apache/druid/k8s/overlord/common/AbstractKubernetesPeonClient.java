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
   * Wait for the K8s job associated with the given taskId to complete, or until the given timeout is reached
   *
   * @return JobResponse indicating the result of the job
   */
  public abstract JobResponse waitForPeonJobCompletion(K8sTaskId taskId, long howLong, TimeUnit unit);

  /**
   * Get the list of all peon jobs in the namespace associated with this client
   */
  public abstract List<Job> getPeonJobs();

  /**
   * Get the Pod associated with the given job name, if it exists
   *
   * @return an Optional containing the Pod if it exists, or absent if not found
   */
  public abstract Optional<Pod> getPeonPod(String jobName);

  /**
   * Get the Job with the given name, if it exists
   *
   * @return an Optional containing the Job if it exists, or absent if not found
   */
  public abstract Optional<Job> getPeonJob(String jobName);

  /**
   * Waits for a pod associated with a job to be created and reach ready state using the pod cache.
   * This method polls the informer cache until the pod appears and has a pod IP assigned.
   *
   * @param jobName  the name of the job whose pod we're waiting for
   * @param howLong  the maximum time to wait
   * @param timeUnit the time unit for the timeout
   * @return the pod in ready state, or null if the pod disappeared after being seen
   * @throws DruidException if the pod never appears within the timeout period
   */
  @Nullable
  protected abstract Pod waitUntilPeonPodCreatedAndReady(String jobName, long howLong, TimeUnit timeUnit);

  /**
   * Launches the given Job. Waits for the associated pod and job to be created and start running.
   *
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
          "K8s pod for the task [%s] appeared and disappeared. It can happen if the task was canceled",
          task.getId()
      );
    }
    log.info("Pod for job[%s] is in state [%s] for task[%s].", jobName, result.getStatus().getPhase(), task.getId());
    long duration = System.currentTimeMillis() - start;
    emitK8sPodMetrics(task, "k8s/peon/startup/time", duration);
    return result;
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

  public int deleteCompletedPeonJobsOlderThan(long howFarBack, TimeUnit timeUnit)
  {
    AtomicInteger numDeleted = new AtomicInteger();
    return clientApi.executeRequest(client -> {
      List<Job> jobs = getJobsToCleanup(getPeonJobs(), howFarBack, timeUnit);
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
   * Checks if the exception is a potentially transient connection pool exception.
   * <p>
   * This method checks if the exception is one of the known transient connection pool exceptions
   * and whether it contains a specific message substring, if applicable.
   * <p>
   * We have experienced connections in the pool being closed by the server-side but remaining in the pool. These issues
   * should be safe to retry in many cases.
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
