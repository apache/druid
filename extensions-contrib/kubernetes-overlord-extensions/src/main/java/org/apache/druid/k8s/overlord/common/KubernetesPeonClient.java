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
import io.fabric8.kubernetes.client.dsl.LogWatch;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;

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
  private final boolean debugJobs;

  public KubernetesPeonClient(KubernetesClientApi clientApi, String namespace, boolean debugJobs)
  {
    this.clientApi = clientApi;
    this.namespace = namespace;
    this.debugJobs = debugJobs;
  }

  public Pod launchPeonJobAndWaitForStart(Job job, long howLong, TimeUnit timeUnit)
  {
    long start = System.currentTimeMillis();
    // launch job
    return clientApi.executeRequest(client -> {
      client.batch().v1().jobs().inNamespace(namespace).resource(job).create();
      K8sTaskId taskId = new K8sTaskId(job.getMetadata().getName());
      log.info("Successfully submitted job: %s ... waiting for job to launch", taskId);
      // wait until the pod is running or complete or failed, any of those is fine
      Pod mainPod = getPeonPodWithRetries(taskId);
      Pod result = client.pods().inNamespace(namespace).withName(mainPod.getMetadata().getName())
                         .waitUntilCondition(pod -> {
                           if (pod == null) {
                             return false;
                           }
                           return pod.getStatus() != null && pod.getStatus().getPodIP() != null;
                         }, howLong, timeUnit);
      long duration = System.currentTimeMillis() - start;
      log.info("Took task %s %d ms for pod to startup", taskId, duration);
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
                      .withName(taskId.getK8sTaskId())
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
                                                                 .withName(taskId.getK8sTaskId())
                                                                 .delete().isEmpty());
      if (result) {
        log.info("Cleaned up k8s task: %s", taskId);
      } else {
        log.info("K8s task does not exist: %s", taskId);
      }
      return result;
    } else {
      log.info("Not cleaning up task %s due to flag: debugJobs=true", taskId);
      return true;
    }
  }

  public Optional<InputStream> getPeonLogs(K8sTaskId taskId)
  {
    KubernetesClient k8sClient = clientApi.getClient();
    try {
      LogWatch logWatch = k8sClient.batch()
                                   .v1()
                                   .jobs()
                                   .inNamespace(namespace)
                                   .withName(taskId.getK8sTaskId())
                                   .inContainer("main")
                                   .watchLog();
      if (logWatch == null) {
        k8sClient.close();
        return Optional.absent();
      }
      return Optional.of(new LogWatchInputStream(k8sClient, logWatch));
    }
    catch (Exception e) {
      log.error(e, "Error streaming logs from task: %s", taskId);
      k8sClient.close();
      return Optional.absent();
    }
  }

  public List<Job> getPeonJobs()
  {
    return clientApi.executeRequest(client -> client.batch()
                                                    .v1()
                                                    .jobs()
                                                    .inNamespace(namespace)
                                                    .withLabel(DruidK8sConstants.LABEL_KEY)
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

  public Optional<Pod> getPeonPod(K8sTaskId taskId)
  {
    return clientApi.executeRequest(client -> getPeonPod(client, taskId));
  }

  private Optional<Pod> getPeonPod(KubernetesClient client, K8sTaskId taskId)
  {
    List<Pod> pods = client.pods()
        .inNamespace(namespace)
        .withLabel("job-name", taskId.getK8sTaskId())
        .list()
        .getItems();
    return pods.isEmpty() ? Optional.absent() : Optional.of(pods.get(0));
  }

  public Pod getPeonPodWithRetries(K8sTaskId taskId)
  {
    return clientApi.executeRequest(client -> getPeonPodWithRetries(client, taskId, 5, RetryUtils.DEFAULT_MAX_TRIES));
  }

  @VisibleForTesting
  Pod getPeonPodWithRetries(KubernetesClient client, K8sTaskId taskId, int quietTries, int maxTries)
  {
    String k8sTaskId = taskId.getK8sTaskId();
    try {
      return RetryUtils.retry(
          () -> {
            Optional<Pod> maybePod = getPeonPod(client, taskId);
            if (maybePod.isPresent()) {
              return maybePod.get();
            }
            throw new KubernetesResourceNotFoundException(
                "K8s pod with label: job-name="
                + k8sTaskId
                + " not found");
          },
          DruidK8sConstants.IS_TRANSIENT, quietTries, maxTries
      );
    }
    catch (Exception e) {
      throw new KubernetesResourceNotFoundException("K8s pod with label: job-name=" + k8sTaskId + " not found");
    }
  }
}
