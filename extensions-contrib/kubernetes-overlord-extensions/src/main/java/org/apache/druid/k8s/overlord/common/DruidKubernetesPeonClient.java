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
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DruidKubernetesPeonClient implements KubernetesPeonClient
{

  private static final EmittingLogger log = new EmittingLogger(DruidKubernetesPeonClient.class);

  private final KubernetesClientApi clientApi;
  private final String namespace;
  private final boolean debugJobs;

  public DruidKubernetesPeonClient(KubernetesClientApi clientApi, String namespace, boolean debugJobs)
  {
    this.clientApi = clientApi;
    this.namespace = namespace;
    this.debugJobs = debugJobs;
  }

  @Override
  public Optional<Job> jobExists(K8sTaskId taskId)
  {
    return clientApi.executeRequest(
        client -> {
          return Optional.fromNullable(
              client.batch()
                    .v1()
                    .jobs()
                    .inNamespace(namespace)
                    .withName(taskId.getK8sTaskId())
                    .get());
        }
    );
  }

  @Override
  public Pod launchJobAndWaitForStart(Job job, long howLong, TimeUnit timeUnit)
  {
    long start = System.currentTimeMillis();
    // launch job
    return clientApi.executeRequest(client -> {
      client.batch().v1().jobs().inNamespace(namespace).create(job);
      K8sTaskId taskId = new K8sTaskId(job.getMetadata().getName());
      log.info("Successfully submitted job: %s ... waiting for job to launch", taskId);
      // wait until the pod is running or complete or failed, any of those is fine
      Pod mainPod = getMainJobPod(taskId);
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

  @Override
  public JobResponse waitForJobCompletion(K8sTaskId taskId, long howLong, TimeUnit unit)
  {
    return clientApi.executeRequest(client -> {
      Job job = client.batch()
                      .v1()
                      .jobs()
                      .inNamespace(namespace)
                      .withName(taskId.getK8sTaskId())
                      .waitUntilCondition(
                          x -> x != null && x.getStatus() != null && x.getStatus().getActive() == null,
                          howLong,
                          unit
                      );
      if (job.getStatus().getSucceeded() != null) {
        return new JobResponse(job, PeonPhase.SUCCEEDED);
      }
      return new JobResponse(job, PeonPhase.FAILED);
    });
  }

  @Override
  public boolean cleanUpJob(K8sTaskId taskId)
  {
    if (!debugJobs) {
      Boolean result = clientApi.executeRequest(client -> client.batch()
                                                                .v1()
                                                                .jobs()
                                                                .inNamespace(namespace)
                                                                .withName(taskId.getK8sTaskId())
                                                                .delete());
      if (result) {
        log.info("Cleaned up k8s task: %s", taskId);
      } else {
        log.info("Failed to cleanup task: %s", taskId);
      }
      return result;
    } else {
      log.info("Not cleaning up task %s due to flag: debugJobs=true", taskId);
      return true;
    }

  }

  @Override
  public String getJobLogs(K8sTaskId taskId)
  {
    try {
      return clientApi.executeRequest(client -> client.batch()
                                                      .v1()
                                                      .jobs()
                                                      .inNamespace(namespace)
                                                      .withName(taskId.getK8sTaskId())
                                                      .inContainer("main")
                                                      .getLog(true));
    }
    catch (Exception e) {
      return "No logs found: " + e;
    }
  }

  @Override
  public Optional<InputStream> getPeonLogs(K8sTaskId taskId)
  {
    try {
      return clientApi.executeRequest(client -> {
        Reader reader = client.batch()
                              .v1()
                              .jobs()
                              .inNamespace(namespace)
                              .withName(taskId.getK8sTaskId())
                              .inContainer("main")
                              .getLogReader();
        if (reader == null) {
          return Optional.absent();
        }
        return Optional.of(new ReaderInputStream(reader, StandardCharsets.UTF_8));
      });
    }
    catch (Exception e) {
      log.error("Error streaming logs from task: %s", taskId);
      return Optional.absent();
    }
  }

  @Override
  public List<Job> listAllPeonJobs()
  {
    return clientApi.executeRequest(client -> client.batch()
                                                    .v1()
                                                    .jobs()
                                                    .inNamespace(namespace)
                                                    .withLabel(DruidK8sConstants.LABEL_KEY)
                                                    .list()
                                                    .getItems());
  }

  @Override
  public List<Pod> listPeonPods(Set<PeonPhase> phases)
  {
    return listPeonPods().stream()
                  .filter(x -> phases.contains(PeonPhase.getPhaseFor(x)))
                  .collect(Collectors.toList());
  }

  @Override
  public List<Pod> listPeonPods()
  {
    PodList podList = clientApi.executeRequest(client -> client.pods().inNamespace(namespace))
                               .withLabel(DruidK8sConstants.LABEL_KEY)
                               .list();
    return podList.getItems();
  }

  @Override
  public int cleanCompletedJobsOlderThan(long howFarBack, TimeUnit timeUnit)
  {
    AtomicInteger numDeleted = new AtomicInteger();
    return clientApi.executeRequest(client -> {
      List<Job> jobs = getJobsToCleanup(listAllPeonJobs(), howFarBack, timeUnit);
      jobs.forEach(x -> {
        if (client.batch().v1().jobs().inNamespace(namespace).withName(x.getMetadata().getName()).delete()) {
          numDeleted.incrementAndGet();
        }
      });
      return numDeleted.get();
    });
  }

  @Override
  public Pod getMainJobPod(K8sTaskId taskId)
  {
    return clientApi.executeRequest(client -> getMainJobPod(client, taskId));
  }


  @VisibleForTesting
  List<Job> getJobsToCleanup(List<Job> candidates, long howFarBack, TimeUnit timeUnit)
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

  Pod getMainJobPod(KubernetesClient client, K8sTaskId taskId)
  {
    String k8sTaskId = taskId.getK8sTaskId();
    try {
      return RetryUtils.retry(
          () -> {
            PodList list = client.pods().inNamespace(namespace).withLabel("job-name", k8sTaskId).list();
            if (list.getItems().size() > 0) {
              return list.getItems().get(0);
            }
            throw new KubernetesResourceNotFoundException(
                "K8s pod with label: job-name="
                + k8sTaskId
                + " not found");
          },
          DruidK8sConstants.IS_TRANSIENT, 5, RetryUtils.DEFAULT_MAX_TRIES
      );
    }
    catch (Exception e) {
      throw new KubernetesResourceNotFoundException("K8s pod with label: job-name=" + k8sTaskId + " not found");
    }
  }

}
