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

public class KubernetesPeonClient
{
  private static final EmittingLogger log = new EmittingLogger(KubernetesPeonClient.class);

  private final KubernetesClientApi clientApi;
  private final String namespace;
  private final String overlordNamespace;
  private final boolean debugJobs;
  private final ServiceEmitter emitter;

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
      client.batch()
            .v1()
            .jobs()
            .inNamespace(namespace)
            .resource(job)
            .create();
      log.info("Submitted job[%s] for task[%s]. Waiting for POD to launch.", jobName, task.getId());

      // wait until the pod is running or complete or failed, any of those is fine
      Pod mainPod = getPeonPodWithRetries(jobName);
      Pod result = client.pods()
                         .inNamespace(namespace)
                         .withName(mainPod.getMetadata().getName())
                         .waitUntilCondition(pod -> {
                           if (pod == null) {
                             return true;
                           }
                           return pod.getStatus() != null && pod.getStatus().getPodIP() != null;
                         }, howLong, timeUnit);
      
      if (result == null) {
        throw new ISE("K8s pod for the task [%s] appeared and disappeared. It can happen if the task was canceled", task.getId());
      }
      long duration = System.currentTimeMillis() - start;
      emitK8sPodMetrics(task, "k8s/peon/startup/time", duration);
      return result;
    });
  }

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

  public List<Job> getPeonJobs()
  {
    return this.overlordNamespace.isEmpty()
           ? getPeonJobsWithoutOverlordNamespaceKeyLabels()
           : getPeonJobsWithOverlordNamespaceKeyLabels();
  }

  private List<Job> getPeonJobsWithoutOverlordNamespaceKeyLabels()
  {
    return clientApi.executeRequest(client -> client.batch()
                                                    .v1()
                                                    .jobs()
                                                    .inNamespace(namespace)
                                                    .withLabel(DruidK8sConstants.LABEL_KEY)
                                                    .list()
                                                    .getItems());
  }

  private List<Job> getPeonJobsWithOverlordNamespaceKeyLabels()
  {
    return clientApi.executeRequest(client -> client.batch()
                                                    .v1()
                                                    .jobs()
                                                    .inNamespace(namespace)
                                                    .withLabel(DruidK8sConstants.LABEL_KEY)
                                                    .withLabel(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, overlordNamespace)
                                                    .list()
                                                    .getItems());
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

  public Optional<Pod> getPeonPod(String jobName)
  {
    return clientApi.executeRequest(client -> getPeonPod(client, jobName));
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

  public Pod getPeonPodWithRetries(String jobName)
  {
    return clientApi.executeRequest(client -> getPeonPodWithRetries(client, jobName, 5, RetryUtils.DEFAULT_MAX_TRIES));
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

            // If the pod is missing, the next best thing we can do is to take a look at what's happening to the job.
            // Use getLatestEventForJob to ensure we consider the actual most recent event.
            List<Event> events = getPeonEvents(client, jobName);

            if (events.isEmpty()) {
              throw new KubernetesResourceNotFoundException("K8s pod with label: job-name=" + jobName + " not found");
            } else {
              Event latestEvent = events.get(events.size() - 1);
              throw new KubernetesResourceNotFoundException(
                  "Job[%s] failed to create pods. Message[%s]", jobName, latestEvent.getMessage());
            }
          },
          this::shouldRetryStartingPeonPod, quietTries, maxTries
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
  private boolean shouldRetryStartingPeonPod(Throwable e)
  {
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

  private void emitK8sPodMetrics(Task task, String metric, long durationMs)
  {
    ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);
    emitter.emit(metricBuilder.setMetric(metric, durationMs));
  }
}
