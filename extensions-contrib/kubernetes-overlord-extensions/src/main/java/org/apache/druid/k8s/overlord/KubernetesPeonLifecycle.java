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

package org.apache.druid.k8s.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;
import org.apache.druid.k8s.overlord.common.JobStatus;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.tasklogs.TaskLogs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class KubernetesPeonLifecycle
{
  private static final EmittingLogger log = new EmittingLogger(KubernetesPeonLifecycle.class);

  private RunnerTaskState taskState = RunnerTaskState.WAITING;

  private final K8sTaskId taskId;
  private final TaskLogs taskLogs;
  private final KubernetesPeonClient kubernetesClient;
  private final ObjectMapper mapper;

  protected KubernetesPeonLifecycle(
      Task task,
      KubernetesPeonClient kubernetesClient,
      TaskLogs taskLogs,
      ObjectMapper mapper
  )
  {
    this.taskId = new K8sTaskId(task);
    this.kubernetesClient = kubernetesClient;
    this.taskLogs = taskLogs;
    this.mapper = mapper;
  }

  /**
   * Run a Kubernetes Job
   *
   * @param job
   * @param launchTimeout
   * @param timeout
   * @return
   * @throws IllegalStateException
   */
  protected synchronized TaskStatus run(Job job, long launchTimeout, long timeout) throws IllegalStateException
  {
    try {
      Preconditions.checkState(
          RunnerTaskState.WAITING.equals(getRunnerTaskState()),
          "Task [%s] failed to run: invalid state transition [%s]->[%s]",
          taskId.getOriginalTaskId(),
          getRunnerTaskState(),
          RunnerTaskState.PENDING
      );

      taskState = RunnerTaskState.PENDING;

      kubernetesClient.launchPeonJobAndWaitForStart(
          job,
          launchTimeout,
          TimeUnit.MILLISECONDS
      );

      return join(timeout);
    }
    finally {
      taskState = RunnerTaskState.NONE;
    }
  }

  /**
   * Join existing Kubernetes Job
   *
   * @param timeout
   * @return
   * @throws IllegalStateException
   */
  protected synchronized TaskStatus join(long timeout) throws IllegalStateException
  {
    try {
      Preconditions.checkState(
          RunnerTaskState.WAITING.equals(getRunnerTaskState()) || RunnerTaskState.PENDING.equals(getRunnerTaskState()),
          "Task [%s] failed to run: invalid state transition [%s]->[%s]",
          taskId,
          getRunnerTaskState(),
          RunnerTaskState.RUNNING
      );

      taskState = RunnerTaskState.RUNNING;

      Optional<Job> maybeJob = kubernetesClient.getPeonJob(taskId);
      if (!maybeJob.isPresent()) {
        throw new ISE("Failed to join job [%s], does not exist", taskId);
      }

      if (JobStatus.isActive(maybeJob.get())) {
        kubernetesClient.waitForPeonJobCompletion(
            taskId,
            timeout,
            TimeUnit.MILLISECONDS
        );
      }

      saveLogs();

      return getTaskStatus();
    }
    finally {
      try {
        shutdown();
      }
      catch (Exception e) {
        log.warn(e, "Task [%s] shutdown failed", taskId);
      }

      taskState = RunnerTaskState.NONE;
    }
  }

  /**
   * Shutdown Kubernetes job and associated pods
   *
   * Behavior: Deletes Kubernetes job which a kill signal to the containers running in
   * the job's associated pod.
   *
   * Task state will be set by the thread running the run(...) or join(...) commands
   */
  protected void shutdown()
  {
    if (
        RunnerTaskState.PENDING.equals(getRunnerTaskState()) ||
        RunnerTaskState.RUNNING.equals(getRunnerTaskState())
    ) {
      kubernetesClient.deletePeonJob(taskId);
    }
  }

  /**
   * Stream logs from the Kubernetes pod running the peon process
   *
   * @return
   */
  protected Optional<InputStream> streamLogs()
  {
    if (!RunnerTaskState.RUNNING.equals(getRunnerTaskState())) {
      return Optional.absent();
    }
    return kubernetesClient.getPeonLogs(taskId);
  }

  /**
   * Get RunnerTaskState
   *
   * @return
   */
  protected RunnerTaskState getRunnerTaskState()
  {
    return taskState;
  }

  /**
   * Get task location for the Kubernetes pod running the peon process
   *
   * @return
   */
  protected TaskLocation getTaskLocation()
  {
    if (!RunnerTaskState.RUNNING.equals(getRunnerTaskState())) {
      return TaskLocation.unknown();
    }

    Optional<Pod> maybePod = kubernetesClient.getPeonPod(taskId);
    if (!maybePod.isPresent()) {
      return TaskLocation.unknown();
    }

    Pod pod = maybePod.get();
    PodStatus podStatus = pod.getStatus();

    if (podStatus == null || podStatus.getPodIP() == null) {
      return TaskLocation.unknown();
    }

    return TaskLocation.create(
        podStatus.getPodIP(),
        DruidK8sConstants.PORT,
        DruidK8sConstants.TLS_PORT,
        Boolean.parseBoolean(pod.getMetadata()
            .getAnnotations()
            .getOrDefault(DruidK8sConstants.TLS_ENABLED, "false")
        )
    );
  }

  private TaskStatus getTaskStatus()
  {
    try {
      Optional<InputStream> maybeTaskStatusStream = taskLogs.streamTaskStatus(taskId.getOriginalTaskId());
      if (maybeTaskStatusStream.isPresent()) {
        return mapper.readValue(
            IOUtils.toString(maybeTaskStatusStream.get(), StandardCharsets.UTF_8),
            TaskStatus.class
        );
      } else {
        return TaskStatus.failure(
            taskId.getOriginalTaskId(),
            StringUtils.format("Task [%s] failed, task status not found", taskId.getOriginalTaskId())
        );
      }
    }
    catch (IOException e) {
      return TaskStatus.failure(
          taskId.getOriginalTaskId(),
          StringUtils.format(
              "Task [%s] failed, caught exception when loading task status: %s",
              taskId.getOriginalTaskId(),
              e.getMessage()
          )
      );
    }
  }

  private void saveLogs()
  {
    try {
      Path file = Files.createTempFile(taskId.getOriginalTaskId(), "file");
      try {
        Optional<InputStream> maybeLogStream = streamLogs();
        if (maybeLogStream.isPresent()) {
          FileUtils.copyInputStreamToFile(maybeLogStream.get(), file.toFile());
        }
        taskLogs.pushTaskLog(taskId.getOriginalTaskId(), file.toFile());
      }
      catch (IOException e) {
        log.error(e, "Failed to stream logs for task [%s]", taskId.getOriginalTaskId());
      }
      finally {
        Files.deleteIfExists(file);
      }
    }
    catch (IOException e) {
      log.warn(e, "Failed to manage temporary log file for task [%s]", taskId.getOriginalTaskId());
    }
  }
}
