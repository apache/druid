/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.guice.annotations.Global;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.indexing.overlord.resources.ShutdownCleanlyResource;
import io.druid.indexing.overlord.resources.TaskLogResource;
import io.druid.indexing.overlord.resources.TierRunningCheckResource;
import io.druid.indexing.overlord.routing.TaskStatusReporter;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.DruidNode;
import io.druid.tasklogs.TaskLogStreamer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Should only be instanced on the overlord. Instancing it anywhere else requires TaskStorage to be functioning from that
 * node
 */
public abstract class AbstractTierRemoteTaskRunner implements TaskRunner, TaskLogStreamer, TaskStatusReporter
{
  private static final EmittingLogger log = new EmittingLogger(AbstractTierRemoteTaskRunner.class);
  private final ConcurrentMap<String, StatefulTaskRunnerWorkItem> knownTasks = new ConcurrentHashMap<>();
  private final TierTaskDiscovery tierTaskDiscovery;
  private final HttpClient httpClient;
  private final TaskStorage taskStorage;
  private final ListeningScheduledExecutorService cleanupExec;
  private final ListenableScheduledFuture<?> cleanupFuture;
  private final AtomicBoolean started = new AtomicBoolean(false);

  @Inject
  public AbstractTierRemoteTaskRunner(
      TierTaskDiscovery tierTaskDiscovery,
      @Global HttpClient httpClient,
      TaskStorage taskStorage,
      ScheduledExecutorFactory executorFactory
  )
  {
    this.cleanupExec = MoreExecutors.listeningDecorator(executorFactory.create(1, "AbstractTierRemoteTaskCleanup--%s"));
    this.tierTaskDiscovery = tierTaskDiscovery;
    this.httpClient = httpClient;
    this.taskStorage = taskStorage;
    cleanupFuture = cleanupExec.scheduleAtFixedRate(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              synchronized (started) {
                if (!started.get()) {
                  log.debug("Not yet started");
                  return;
                }
                for (StatefulTaskRunnerWorkItem workItem : knownTasks.values()) {
                  if (workItem.lastSeen.get() > 300_000 // TODO: configurable
                      && StatefulTaskRunnerWorkItem.State.isTerminal(workItem.state.get())) {
                    // If its done and has not been updated in 5 minutes, remove it from the tasks
                    if (!knownTasks.remove(workItem.getTaskId(), workItem)) {
                      log.makeAlert("Task [%s] could not be removed", workItem.getTaskId()).emit();
                    }
                  }
                }
              }
            }
            catch (Exception e) {
              log.error(e, "Unhandled exception in cleanup");
            }
          }
        },
        0, // launch and probably skip once. Useful for unit tests
        1,
        TimeUnit.MINUTES
    );
    Futures.addCallback(cleanupFuture, new FutureCallback<Object>()
    {
      @Override
      public void onSuccess(@Nullable Object result)
      {
        log.debug("Cleanup future is done");
      }

      @Override
      public void onFailure(Throwable t)
      {
        if (cleanupFuture.isCancelled()) {
          log.debug("Cleanup is cancelled");
        } else {
          log.error(t, "Cleanup had an error");
        }
      }
    });
  }

  @Override
  public Optional<ByteSource> streamTaskLog(String taskid, long offset) throws IOException
  {
    final StatefulTaskRunnerWorkItem workItem = knownTasks.get(taskid);
    DruidNode node = workItem == null ? null : workItem.node.get();
    if (node == null) {
      final Optional<DruidNode> maybeNode = tierTaskDiscovery.getNodeForTask(taskid);
      if (!maybeNode.isPresent()) {
        log.debug("No node discovered for task id [%s]", taskid);
        return Optional.absent();
      }
      node = maybeNode.get();
    }
    if (node == null) {
      log.debug("No node for task id [%s]", taskid);
      return Optional.absent();
    }
    final URL url = TaskLogResource.buildURL(node, offset);
    return Optional.<ByteSource>of(
        new ByteSource()
        {
          @Override
          public InputStream openStream() throws IOException
          {
            try {
              return httpClient.go(
                  new Request(HttpMethod.GET, url),
                  new InputStreamResponseHandler(),
                  Duration.millis(120_000) // TODO: make this configurable
              ).get();
            }
            catch (InterruptedException e) {
              log.info(e, "Interrupted while fetching logs from [%s]", url);
              Thread.currentThread().interrupt();
              throw Throwables.propagate(e);
            }
            catch (ExecutionException e) {
              log.warn(e, "Error getting data from [%s]", url);
              final Throwable cause = e.getCause();
              Throwables.propagateIfInstanceOf(cause, IOException.class);
              throw Throwables.propagate(cause);
            }
          }
        }
    );
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    return ImmutableList.of();
  }

  /**
   * Handle task state in the process of running it. The actual run request is handled through the `launch` method
   *
   * @param task task to run
   *
   * @return A future which will return the value TaskStatus. Any exceptions encountered during `launch` will be exposed
   * as exceptions in the future, including if the launch was interrupted by InterruptedException.
   */
  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    final SettableFuture<TaskStatus> future = SettableFuture.create();
    Futures.addCallback(
        future, new FutureCallback<TaskStatus>()
        {
          @Override
          public void onSuccess(TaskStatus status)
          {
            final StatefulTaskRunnerWorkItem priorItem = knownTasks.get(task.getId());
            if (priorItem == null) {
              log.makeAlert("Task [%s] was pruned before it could be reported with status [%s]", task.getId(), status)
                 .emit();
              return;
            }
            priorItem.seen();
            if (status.isSuccess()) {
              priorItem.state.set(StatefulTaskRunnerWorkItem.State.SUCCESS);
            } else if (status.isFailure()) {
              priorItem.state.set(StatefulTaskRunnerWorkItem.State.FAILED);
            }
          }

          @Override
          public void onFailure(Throwable throwable)
          {
            if (future.isCancelled()) {
              log.info("Future for [%s] was cancelled", task.getId());
              return;
            }
            final StatefulTaskRunnerWorkItem priorItem = knownTasks.get(task.getId());
            if (priorItem == null) {
              log.warn("Task [%s] was pruned before it could be reported as unknown", task.getId());
              return;
            }
            priorItem.seen();
            final StatefulTaskRunnerWorkItem.State state = priorItem.state.get();
            if (StatefulTaskRunnerWorkItem.State.FAILED.equals(state)
                || StatefulTaskRunnerWorkItem.State.SUCCESS.equals(state)) {
              log.debug("Task [%s] already done, skipping setting state from failed future", task.getId());
            } else {
              log.debug("Setting state to UNKNOWN for failed future task [%s]", task.getId());
              if (!priorItem.state.compareAndSet(state, StatefulTaskRunnerWorkItem.State.UNKNOWN)) {
                log.warn(
                    "state for task [%s] was changed during failure update to UNKNOWN. Expected [%s] found [%s]",
                    task.getId(),
                    state,
                    priorItem.state.get()
                );
              }
            }
          }
        }
    );
    final StatefulTaskRunnerWorkItem workItem = new StatefulTaskRunnerWorkItem(
        task.getId(),
        future,
        DateTime.now(),
        DateTime.now()
    );
    workItem.state.set(StatefulTaskRunnerWorkItem.State.STAGED);
    final StatefulTaskRunnerWorkItem priorItem = knownTasks.putIfAbsent(task.getId(), workItem);
    if (priorItem != null) {
      return priorItem.getResult();
    }
    try {
      launch(future, task);
    }
    catch (Exception e) {
      if (!future.isDone()) {
        future.setException(e);
      } else {
        log.warn(e, "Task [%s] future already done, ignoring error");
      }
    }
    return future;
  }

  /**
   * Launch the task on the framework
   *
   * @param future The future which will be used
   * @param task   The task to launch
   */
  protected abstract void launch(SettableFuture future, Task task);

  /**
   * Kill the task. May be overridden by any particular implementation.
   *
   * @param node The DruidNode (host:port) of the druid JVM running a TASK.
   */
  protected ListenableFuture<?> kill(DruidNode node)
  {
    return killRemoteTask(httpClient, node);
  }

  @Override
  public void shutdown(String taskid)
  {
    StatefulTaskRunnerWorkItem workItem = knownTasks.get(taskid);
    if (workItem == null) {
      refreshTaskIds();
      final SettableFuture<TaskStatus> future = SettableFuture.create();
      future.set(TaskStatus.failure(taskid));
      final StatefulTaskRunnerWorkItem statefulTaskRunnerWorkItem = new StatefulTaskRunnerWorkItem(
          taskid,
          future,
          DateTime.now(),
          DateTime.now()
      );
      workItem = knownTasks.putIfAbsent(taskid, statefulTaskRunnerWorkItem);
      if (workItem == null) {
        log.info("Task [%s] not found, setting work item as failed");
        return;
      }
    }

    final DruidNode node = workItem.node.get();

    if (node == null) {
      log.error("Task [%s] has a work item but is not reporting itself, Failing task. Will kill if it reports itself");
      ((SettableFuture<TaskStatus>) workItem.getResult()).set(TaskStatus.failure(taskid));
      return;
    }
    try {
      kill(node).get();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
  {
    return Collections2.filter(
        knownTasks.values(),
        new Predicate<StatefulTaskRunnerWorkItem>()
        {
          @Override
          public boolean apply(StatefulTaskRunnerWorkItem input)
          {
            return StatefulTaskRunnerWorkItem.State.STARTED.equals(input.state.get());
          }
        }
    );
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    return Collections2.filter(
        knownTasks.values(),
        new Predicate<StatefulTaskRunnerWorkItem>()
        {
          @Override
          public boolean apply(StatefulTaskRunnerWorkItem input)
          {
            return StatefulTaskRunnerWorkItem.State.STAGED.equals(input.state.get());
          }
        }
    );
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    return knownTasks.values();
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.absent();
  }

  @Override
  public boolean reportStatus(final TaskStatus status)
  {
    boolean checkStorageStatus = false; // If we expect this status to match the final version in storage
    final String taskId = status.getId();
    StatefulTaskRunnerWorkItem workItem = knownTasks.get(taskId);
    if (workItem == null && status.isComplete()) {
      final SettableFuture<TaskStatus> future = SettableFuture.create();
      if (!future.set(status)) {
        throw new ISE("Somehow someone set the future first");
      }
      workItem = new StatefulTaskRunnerWorkItem(taskId, future, DateTime.now(), DateTime.now());
      workItem = knownTasks.putIfAbsent(taskId, workItem);
      if (workItem == null) {
        checkStorageStatus = true;
      }
      // In the weird case where workItem encounters a race condition and is set, we will treat it properly below
    }

    // Either workItem was present to begin with or it was added in a racy way
    if (workItem != null) {
      workItem.seen();
      final StatefulTaskRunnerWorkItem.State workItemState = workItem.state.get();
      if (!workItemState.isTerminal()) {
        StatefulTaskRunnerWorkItem.State newState = StatefulTaskRunnerWorkItem.State.of(status.getStatusCode());
        if (workItem.state.compareAndSet(workItemState, newState)) {
          log.debug("Set task [%s] to state [%s]", taskId, newState);
        } else {
          log.warn(
              "Update of task [%s] to state [%s] failed because it was in state [%s]",
              taskId,
              newState,
              workItem.state.get()
          ); // Example : terminal status is reported in a racy way
        }
      }
    }
    if (workItem != null && status.isComplete()) {
      final ListenableFuture<TaskStatus> workFuture = workItem.getResult();
      if (workFuture instanceof SettableFuture) {
        final SettableFuture<TaskStatus> settableFuture = (SettableFuture<TaskStatus>) workFuture;
        /**
         * This is actually a bit dangerous. We're relying on the callbacks in TaskQueue to handle the future,
         * but don't make any guarantees the future callbacks fire or complete as expected. As such it is possible
         * to kill the overlord between the time the future is set and the result is properly stored in the task
         * storage. We check at the end to make sure the status is expected.
         */
        if (!settableFuture.set(status)) {
          final TaskStatus priorStatus;
          try {
            priorStatus = settableFuture.get();
          }
          catch (InterruptedException | ExecutionException e) {
            throw new ISE(e, "How did this happen!? Expected the result for task [%s] to already be set", taskId);
          }
          if (!status.equals(priorStatus)) {
            // Alert here or just error log?
            log.error("Received task status of [%s] but already set to [%s]", status, priorStatus);
          } else {
            checkStorageStatus = true;
          }
          if (priorStatus.isFailure()) {
            log.info("Already failed task reported status of [%s]", taskId);
          }
        } else {
          log.debug("Task status for [%s] could not be set. Checking storage value", taskId);
          checkStorageStatus = true;
        }
      } else {
        throw new ISE("Future for task ID [%s] is not a SettableFuture: [%s]", taskId, workItem.getResult());
      }
    }

    if (!checkStorageStatus) {
      // We heard the report, but it didn't match and we don't care. We logged it.
      return true;
    }

    try {
      // TODO: configurable
      final AtomicInteger retriesRemaining = new AtomicInteger(10);
      // We use RetryUtils for exponential backoff, but don't want it to throw ISE when retries are exceeded.
      return RetryUtils.retry(
          new Callable<Boolean>()
          {
            @Override
            public Boolean call() throws Exception
            {
              if (retriesRemaining.decrementAndGet() <= 0) {
                return false;
              }
              final Optional<TaskStatus> setStatus = taskStorage.getStatus(taskId);
              if (setStatus.isPresent()) {
                final TaskStatus setTaskStatus = setStatus.get();
                if (setTaskStatus.getStatusCode().equals(status.getStatusCode())) {
                  return true;
                }
                if (setTaskStatus.isComplete()) {
                  log.makeAlert(
                      "Task [%s] tried to set terminal status [%s] but was already [%s]",
                      taskId,
                      status,
                      setTaskStatus
                  ).emit();
                  return true;
                } else {
                  // Storage of task did not match and was not terminal in storage.
                  return false;
                }
              } else {
                throw new NotYetFoundException();
              }
            }
          },
          new Predicate<Throwable>()
          {
            @Override
            public boolean apply(Throwable throwable)
            {
              return throwable instanceof NotYetFoundException;
            }
          },
          retriesRemaining.get() + 1
      );
    }
    catch (InterruptedException e) {
      log.debug(e, "Reporting Interrupted");
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    catch (Exception e) {
      log.error(e, "Error reporting status [%s]", status);
      throw Throwables.propagate(e);
    }
  }

  protected void refreshTaskIds()
  {
    final Map<String, DruidNode> taskMap = tierTaskDiscovery.getTasks();
    final Set<String> lostTasks = Sets.difference(knownTasks.keySet(), taskMap.keySet());
    for (String lostTask : lostTasks) {
      final StatefulTaskRunnerWorkItem workItem = knownTasks.get(lostTask);
      if (workItem != null) {
        final StatefulTaskRunnerWorkItem.State state = workItem.state.get();
        if (StatefulTaskRunnerWorkItem.State.STARTED.equals(state)) {
          log.warn("Lost task [%s]", lostTask);
          if (!workItem.state.compareAndSet(state, StatefulTaskRunnerWorkItem.State.UNKNOWN)) {
            log.warn("Could not update task [%s] to UNKNOWN because it is [%s]", lostTask, workItem.state.get());
          }
        }
      }
    }
    for (Map.Entry<String, DruidNode> entry : taskMap.entrySet()) {
      StatefulTaskRunnerWorkItem workItem = knownTasks.get(entry.getKey());
      if (workItem == null) {
        // Maybe put in a new entry
        final SettableFuture<TaskStatus> future = SettableFuture.create();
        workItem = new StatefulTaskRunnerWorkItem(entry.getKey(), future, null, null);
        final StatefulTaskRunnerWorkItem prior = knownTasks.putIfAbsent(entry.getKey(), workItem);
        if (prior != null) {
          log.debug("Found prior work item for task id [%s]", prior.getTaskId());
          workItem = prior;
        }
      }
      final StatefulTaskRunnerWorkItem.State state = workItem.state.get();
      if (StatefulTaskRunnerWorkItem.State.STAGED.equals(state)
          || StatefulTaskRunnerWorkItem.State.UNKNOWN.equals(state)) {
        if (!workItem.state.compareAndSet(state, StatefulTaskRunnerWorkItem.State.STARTED)) {
          log.warn(
              "Someone set state for task [%s] to [%s] before I could set it to STARTED from [%s]",
              entry.getKey(),
              workItem.state.get(),
              state
          );
        }
      } else if (!StatefulTaskRunnerWorkItem.State.STARTED.equals(state)) {
        log.error("Task [%s] is reporting strange state [%s]", entry.getKey(), state);
      }
      workItem.node.set(entry.getValue());
      workItem.seen();
    }
  }

  protected ListenableFuture<?> refreshTaskStatus()
  {
    final List<ListenableFuture<Boolean>> futures = new ArrayList<>();
    for (final Map.Entry<String, StatefulTaskRunnerWorkItem> entry : knownTasks.entrySet()) {
      final StatefulTaskRunnerWorkItem workItem = entry.getValue();
      final URL url;
      try {
        final DruidNode node = workItem.node.get();
        if (node == null) {
          log.warn("Task [%s] has no node", entry.getKey());
          continue;
        }
        url = new URL("http", node.getHost(), node.getPort(), TierRunningCheckResource.PATH);
      }
      catch (MalformedURLException e) {
        log.warn(e, "Error checking task [%s]", entry.getKey());
        final SettableFuture<Boolean> future = SettableFuture.create();
        future.setException(e);
        futures.add(future);
        continue;
      }
      final Request request = new Request(HttpMethod.GET, url);
      final ListenableFuture<Boolean> future = httpClient.go(
          request,
          new HttpResponseHandler<Boolean, Boolean>()
          {
            @Override
            public ClientResponse<Boolean> handleResponse(HttpResponse response)
            {
              if (response.getStatus().getCode() == HttpResponseStatus.OK.getCode()) {
                return ClientResponse.finished(true);
              } else {
                log.warn("Error in checking for status of task [%s]: [%s]", entry.getKey(), response.getStatus());
                return ClientResponse.finished(false);
              }
            }

            @Override
            public ClientResponse<Boolean> handleChunk(
                ClientResponse<Boolean> clientResponse, HttpChunk chunk
            )
            {
              log.trace("Received a chunk from [%s]", url);
              return clientResponse;
            }

            @Override
            public ClientResponse<Boolean> done(ClientResponse<Boolean> clientResponse)
            {
              final StatefulTaskRunnerWorkItem.State state = workItem.state.get();
              if (state.isTerminal()) {
                // Maybe it is shutting down?
                log.warn("Task [%s] still being polled even though it is thought to be [%s]", entry.getKey(), state);
              } else if (clientResponse.getObj()) {
                log.debug("Task [%s] running", entry.getKey());
                // Example: failure is reported asynchronously
                if (!workItem.state.compareAndSet(state, StatefulTaskRunnerWorkItem.State.STARTED)) {
                  log.warn(
                      "Failed to update STARTED state of task [%s]. Expected [%s] found [%s]",
                      entry.getKey(),
                      state,
                      workItem.state.get()
                  );
                }
              } else {
                // Example: failure is reported asynchronously
                if (!workItem.state.compareAndSet(state, StatefulTaskRunnerWorkItem.State.UNKNOWN)) {
                  log.warn(
                      "Failed to update UNKNOWN state of task [%s]. Expected [%s] found [%s]",
                      entry.getKey(),
                      state,
                      workItem.state.get()
                  );
                }
              }
              return clientResponse;
            }

            @Override
            public void exceptionCaught(ClientResponse<Boolean> clientResponse, Throwable e)
            {
              final StatefulTaskRunnerWorkItem.State state = entry.getValue().state.get();
              if (StatefulTaskRunnerWorkItem.State.STARTED.equals(state)) {
                log.error(e, "Error in processing [%s] setting [%s] to unknown", url, entry.getKey());
                if (!entry.getValue().state.compareAndSet(state, StatefulTaskRunnerWorkItem.State.UNKNOWN)) {
                  log.warn(
                      "Task [%s] could not update to UNKNOWN. Expected [%s] found [%s]",
                      entry.getKey(),
                      state,
                      entry.getValue().state.get()
                  );
                }
              } else {
                log.error(e, "Error processing [%s], leaving [%s] as [%s]", url, entry.getKey(), state);
              }
            }
          }
      );
      Futures.addCallback(
          future, new FutureCallback<Boolean>()
          {
            @Override
            public void onSuccess(Boolean result)
            {
              log.debug("[%s] resulted in [%s]", url, result);
            }

            @Override
            public void onFailure(Throwable t)
            {
              log.error(t, "Error getting status from [%s]", url);
            }
          }
      );
      futures.add(future);
    }
    return Futures.allAsList(futures);
  }

  public static ListenableFuture<?> killRemoteTask(HttpClient httpClient, DruidNode node)
  {
    final URL url;
    try {
      url = new URL("http", node.getHost(), node.getPort(), ShutdownCleanlyResource.PATH);
    }
    catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
    return httpClient.go(
        new Request(HttpMethod.POST, url),
        new HttpResponseHandler<Void, Void>()
        {
          @Override
          public ClientResponse<Void> handleResponse(HttpResponse response)
          {
            HttpResponseStatus status = response.getStatus();
            log.debug(
                "Received status code %d [%s] for shutdown request",
                status.getCode(),
                status.getReasonPhrase()
            );
            if (status.getCode() != HttpResponseStatus.ACCEPTED.getCode()) {
              final String msg = String.format(
                  "Bad status code. Received [%d]:[%s] from url [%s]",
                  status.getCode(),
                  status.getReasonPhrase(),
                  url
              );
              throw new RuntimeException(msg);
            }
            return ClientResponse.finished(null);
          }

          @Override
          public ClientResponse<Void> handleChunk(
              ClientResponse<Void> clientResponse, HttpChunk chunk
          )
          {
            log.debug("Received chunk... why?");
            return clientResponse;
          }

          @Override
          public ClientResponse<Void> done(ClientResponse<Void> clientResponse)
          {
            return clientResponse;
          }

          @Override
          public void exceptionCaught(ClientResponse<Void> clientResponse, Throwable e)
          {
            log.error(e, "Error in command execution");
          }
        }
    );
  }

  @Override
  @LifecycleStart
  public void start()
  {
    synchronized (started) {
      if (!started.compareAndSet(false, true)) {
        throw new ISE("Already started");
      }
      refreshTaskIds();
      refreshTaskStatus();
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    synchronized (started) {
      started.set(false);
      knownTasks.clear();
      cleanupExec.shutdownNow();
    }
  }


  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    throw new UnsupportedOperationException("Not Yet Supported");
  }
}

class StatefulTaskRunnerWorkItem extends TaskRunnerWorkItem
{
  final AtomicReference<State> state = new AtomicReference<>(State.UNKNOWN);
  final AtomicLong lastSeen = new AtomicLong(System.currentTimeMillis());
  final AtomicReference<DruidNode> node = new AtomicReference<>(null);

  public StatefulTaskRunnerWorkItem(
      String taskId,
      SettableFuture<TaskStatus> result,
      DateTime createdTime,
      DateTime queueInsertionTime
  )
  {
    super(taskId, result, createdTime, queueInsertionTime);
  }

  @Override
  public TaskLocation getLocation()
  {
    throw new UnsupportedOperationException("TODO: this");
  }

  enum State
  {
    UNKNOWN, STAGED, STARTED, FAILED, SUCCESS;

    public static boolean isTerminal(State state)
    {
      return SUCCESS.equals(state) || FAILED.equals(state);
    }

    public static State of(TaskStatus.Status status)
    {
      switch (status) {
        case RUNNING:
          return STARTED;
        case SUCCESS:
          return SUCCESS;
        case FAILED:
          return FAILED;
        default:
          return UNKNOWN;
      }
    }

    public boolean isTerminal()
    {
      return isTerminal(this);
    }
  }

  public void seen()
  {
    lastSeen.set(System.currentTimeMillis());
  }
}

class NotYetFoundException extends Exception
{
}
