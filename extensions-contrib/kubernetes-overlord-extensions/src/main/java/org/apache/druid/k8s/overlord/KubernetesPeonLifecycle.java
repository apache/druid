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
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;
import org.apache.druid.k8s.overlord.common.JobResponse;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.tasklogs.TaskLogs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is a wrapper per Druid task responsible for managing the task lifecycle
 * once it has been transformed into a K8s Job and submitted by the KubernetesTaskRunner.
 *
 * This includes actually submitting the waiting Job to K8s,
 * waiting for the Job to report completion,
 * joining an already running Job and waiting for it to report completion,
 * shutting down a Job, including queued jobs that have not been submitted to K8s,
 * streaming task logs for a running job
 */
public class KubernetesPeonLifecycle
{
  private static final EmittingLogger log = new EmittingLogger(KubernetesPeonLifecycle.class);

  protected enum State
  {
    /** Lifecycle's state before {@link #run(Job, long, long)} or {@link #join(long)} is called. */
    NOT_STARTED,
    /** Lifecycle's state since {@link #run(Job, long, long)} is called. */
    PENDING,
    /** Lifecycle's state since {@link #join(long)} is called. */
    RUNNING,
    /** Lifecycle's state since the task has completed. */
    STOPPED
  }

  private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
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
          state.compareAndSet(State.NOT_STARTED, State.PENDING),
          "Task [%s] failed to run: invalid peon lifecycle state transition [%s]->[%s]",
          taskId.getOriginalTaskId(),
          state.get(),
          State.PENDING
      );

      kubernetesClient.launchPeonJobAndWaitForStart(
          job,
          launchTimeout,
          TimeUnit.MILLISECONDS
      );

      return join(timeout);
    }
    finally {
      state.set(State.STOPPED);
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
          (
              state.compareAndSet(State.NOT_STARTED, State.RUNNING) ||
              state.compareAndSet(State.PENDING, State.RUNNING)
          ),
          "Task [%s] failed to join: invalid peon lifecycle state transition [%s]->[%s]",
          taskId.getOriginalTaskId(),
          state.get(),
          State.RUNNING
      );

      JobResponse jobResponse = kubernetesClient.waitForPeonJobCompletion(
          taskId,
          timeout,
          TimeUnit.MILLISECONDS
      );

      saveLogs();

      return getTaskStatus(jobResponse.getJobDuration());
    }
    finally {
      try {
        shutdown();
      }
      catch (Exception e) {
        log.warn(e, "Task [%s] shutdown failed", taskId);
      }

      state.set(State.STOPPED);
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
    if (State.PENDING.equals(state.get()) || State.RUNNING.equals(state.get())) {
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
    if (!State.RUNNING.equals(state.get())) {
      return Optional.absent();
    }
    return kubernetesClient.getPeonLogs(taskId);
  }

  /**
   * Get peon lifecycle state
   *
   * @return
   */
  protected State getState()
  {
    return state.get();
  }

  /**
   * Get task location for the Kubernetes pod running the peon process
   *
   * @return
   */
  protected TaskLocation getTaskLocation()
  {
    if (!State.RUNNING.equals(state.get())) {
      log.debug("Can't get task location for non-running job. [%s]", taskId.getOriginalTaskId());
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

  private TaskStatus getTaskStatus(long duration)
  {
    TaskStatus taskStatus;
    try {
      Optional<InputStream> maybeTaskStatusStream = taskLogs.streamTaskStatus(taskId.getOriginalTaskId());
      if (maybeTaskStatusStream.isPresent()) {
        taskStatus = mapper.readValue(
            IOUtils.toString(maybeTaskStatusStream.get(), StandardCharsets.UTF_8),
            TaskStatus.class
        );
      } else {
        taskStatus = TaskStatus.failure(taskId.getOriginalTaskId(), "task status not found");
      }
    }
    catch (IOException e) {
      log.error(e, "Failed to load task status for task [%s]", taskId.getOriginalTaskId());
      taskStatus = TaskStatus.failure(
          taskId.getOriginalTaskId(),
          StringUtils.format("error loading status: %s", e.getMessage())
      );
    }

    return taskStatus.withDuration(duration);
  }

  private void saveLogs()
  {
    try {
      Path file = Files.createTempFile(taskId.getOriginalTaskId(), "log");
      try {
        Optional<InputStream> maybeLogStream = streamLogs();
        if (maybeLogStream.isPresent()) {
          FileUtils.copyInputStreamToFile(maybeLogStream.get(), file.toFile());
        } else {
          log.debug("Log stream not found for %s", taskId.getOriginalTaskId());
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
