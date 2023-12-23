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
import io.fabric8.kubernetes.client.dsl.LogWatch;
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
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
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
  @FunctionalInterface
  public interface TaskStateListener
  {
    void stateChanged(State state, String taskId);
  }

  private static final EmittingLogger log = new EmittingLogger(KubernetesPeonLifecycle.class);

  protected enum State
  {
    /** Lifecycle's state before {@link #run(Job, long, long, boolean)} or {@link #join(long)} is called. */
    NOT_STARTED,
    /** Lifecycle's state since {@link #run(Job, long, long, boolean)} is called. */
    PENDING,
    /** Lifecycle's state since {@link #join(long)} is called. */
    RUNNING,
    /** Lifecycle's state since the task has completed. */
    STOPPED
  }

  private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
  private final K8sTaskId taskId;
  private final TaskLogs taskLogs;
  private final Task task;
  private final KubernetesPeonClient kubernetesClient;
  private final ObjectMapper mapper;
  private final TaskStateListener stateListener;
  @MonotonicNonNull
  private LogWatch logWatch;

  private TaskLocation taskLocation;

  protected KubernetesPeonLifecycle(
      Task task,
      KubernetesPeonClient kubernetesClient,
      TaskLogs taskLogs,
      ObjectMapper mapper,
      TaskStateListener stateListener
  )
  {
    this.taskId = new K8sTaskId(task);
    this.task = task;
    this.kubernetesClient = kubernetesClient;
    this.taskLogs = taskLogs;
    this.mapper = mapper;
    this.stateListener = stateListener;
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
  protected synchronized TaskStatus run(Job job, long launchTimeout, long timeout, boolean useDeepStorageForTaskPayload) throws IllegalStateException, IOException
  {
    try {
      updateState(new State[]{State.NOT_STARTED}, State.PENDING);

      if (useDeepStorageForTaskPayload) {
        writeTaskPayload(task);
      }

      // In case something bad happens and run is called twice on this KubernetesPeonLifecycle, reset taskLocation.
      taskLocation = null;
      kubernetesClient.launchPeonJobAndWaitForStart(
          job,
          task,
          launchTimeout,
          TimeUnit.MILLISECONDS
      );

      return join(timeout);
    }
    catch (Exception e) {
      log.info("Failed to run task: %s", taskId.getOriginalTaskId());
      throw e;
    }
    finally {
      stopTask();
    }
  }

  private void writeTaskPayload(Task task) throws IOException
  {
    Path file = null;
    try {
      file = Files.createTempFile(taskId.getOriginalTaskId(), "task.json");
      FileUtils.writeStringToFile(file.toFile(), mapper.writeValueAsString(task), Charset.defaultCharset());
      taskLogs.pushTaskPayload(task.getId(), file.toFile());
    }
    catch (Exception e) {
      log.error("Failed to write task payload for task: %s", taskId.getOriginalTaskId());
      throw new RuntimeException(e);
    }
    finally {
      if (file != null) {
        Files.deleteIfExists(file);
      }
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
      updateState(new State[]{State.NOT_STARTED, State.PENDING}, State.RUNNING);

      JobResponse jobResponse = kubernetesClient.waitForPeonJobCompletion(
          taskId,
          timeout,
          TimeUnit.MILLISECONDS
      );

      return getTaskStatus(jobResponse.getJobDuration());
    }
    finally {
      try {
        saveLogs();
      }
      catch (Exception e) {
        log.warn(e, "Log processing failed for task [%s]", taskId);
      }

      stopTask();
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
    if (State.PENDING.equals(state.get()) || State.RUNNING.equals(state.get()) || State.STOPPED.equals(state.get())) {
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
    if (State.PENDING.equals(state.get()) || State.NOT_STARTED.equals(state.get())) {
      log.debug("Can't get task location for non-running job. [%s]", taskId.getOriginalTaskId());
      return TaskLocation.unknown();
    }

    /* It's okay to cache this because podIP only changes on pod restart, and we have to set restartPolicy to Never
    since Druid doesn't support retrying tasks from a external system (K8s). We can explore adding a fabric8 watcher
    if we decide we need to change this later.
    **/
    if (taskLocation == null) {
      Optional<Pod> maybePod = kubernetesClient.getPeonPod(taskId.getK8sJobName());
      if (!maybePod.isPresent()) {
        return TaskLocation.unknown();
      }

      Pod pod = maybePod.get();
      PodStatus podStatus = pod.getStatus();

      if (podStatus == null || podStatus.getPodIP() == null) {
        return TaskLocation.unknown();
      }
      taskLocation = TaskLocation.create(
          podStatus.getPodIP(),
          DruidK8sConstants.PORT,
          DruidK8sConstants.TLS_PORT,
          Boolean.parseBoolean(pod.getMetadata().getAnnotations().getOrDefault(DruidK8sConstants.TLS_ENABLED, "false")),
          pod.getMetadata() != null ? pod.getMetadata().getName() : ""
      );
    }

    return taskLocation;
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
        log.info(
            "Peon for task [%s] did not push its task status. Check k8s logs and events for the pod to see what happened.",
            taskId
        );
        taskStatus = TaskStatus.failure(taskId.getOriginalTaskId(), "Peon did not report status successfully.");
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

  protected void startWatchingLogs()
  {
    if (logWatch != null) {
      log.debug("There is already a log watcher for %s", taskId.getOriginalTaskId());
      return;
    }
    try {
      Optional<LogWatch> maybeLogWatch = kubernetesClient.getPeonLogWatcher(taskId);
      if (maybeLogWatch.isPresent()) {
        logWatch = maybeLogWatch.get();
      }
    }
    catch (Exception e) {
      log.error(e, "Error watching logs from task: %s", taskId);
    }
  }

  protected void saveLogs()
  {
    try {
      Path file = Files.createTempFile(taskId.getOriginalTaskId(), "log");
      try {
        startWatchingLogs();
        if (logWatch != null) {
          FileUtils.copyInputStreamToFile(logWatch.getOutput(), file.toFile());
        } else {
          log.debug("Log stream not found for %s", taskId.getOriginalTaskId());
          FileUtils.writeStringToFile(
              file.toFile(),
              StringUtils.format(
                  "Peon for task [%s] did not report any logs. Check k8s metrics and events for the pod to see what happened.",
                  taskId
              ),
              Charset.defaultCharset()
          );

        }
        taskLogs.pushTaskLog(taskId.getOriginalTaskId(), file.toFile());
      }
      catch (IOException e) {
        log.error(e, "Failed to stream logs for task [%s]", taskId.getOriginalTaskId());
      }
      finally {
        if (logWatch != null) {
          logWatch.close();
        }
        Files.deleteIfExists(file);
      }
    }
    catch (IOException e) {
      log.warn(e, "Failed to manage temporary log file for task [%s]", taskId.getOriginalTaskId());
    }
  }

  private void stopTask()
  {
    if (!State.STOPPED.equals(state.get())) {
      updateState(new State[]{State.NOT_STARTED, State.PENDING, State.RUNNING}, State.STOPPED);
    }
  }

  private void updateState(State[] acceptedStates, State targetState)
  {
    Preconditions.checkState(
        Arrays.stream(acceptedStates).anyMatch(s -> state.compareAndSet(s, targetState)),
        "Task [%s] failed to run: invalid peon lifecycle state transition [%s]->[%s]",
        taskId.getOriginalTaskId(),
        state.get(),
        targetState
    );
    stateListener.stateChanged(state.get(), taskId.getOriginalTaskId());
  }
}
