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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;
import io.druid.concurrent.Execs;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.tasklogs.LogUtils;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.indexing.overlord.config.TierLocalTaskRunnerConfig;
import io.druid.indexing.overlord.resources.DeadhandResource;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.DruidNode;
import io.druid.tasklogs.TaskLogPusher;
import io.druid.tasklogs.TaskLogStreamer;
import org.apache.commons.io.FileUtils;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Duration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The TierLocalTaskRunner is a gateway between general cluster task information and forked stuff locally on a
 * single machine. It communicates existence via  PORT_FILE_NAME in the task attempt directory.
 * Using the PORT_FILE_NAME the TierLocalTaskRunner can identify peons on the local machine.
 * The TierLocalTaskRunner is able to restart without affecting the state of the peons.
 * The Peons run some special magic in io.druid.indexing.overlord.TierModule to expose an ability to shutdown the VMs
 * cleanly.
 */
public class TierLocalTaskRunner implements TaskRunner, TaskLogStreamer
{
  private enum ForkingTaskRunnerState
  {
    INITIALIZING,
    STARTING,
    STARTED,
    STOPPING,
    STOPPED
  }

  public static final String TASKID_PROPERTY = "io.druid.indexing.worker.taskid";
  public static final String PORT_FILE_NAME = "task.port";
  public static final String LOG_FILE_NAME = "task.log";
  public static final String TASK_FILE_NAME = "task.json";
  public static final String STATUS_FILE_NAME = "status.json";
  public static final String DEADHAND_FILE_NAME = "delete_me_to_terminate";
  private static final EmittingLogger log = new EmittingLogger(TierLocalTaskRunner.class);
  protected static final String CHILD_PROPERTY_PREFIX = "druid.indexer.fork.property.";
  private static final int MAX_DELETE_RETRIES = 3; // How many times should we try to delete the attempt dir on cleanup
  private static final String LONGEST_POSITIVE_INTEGER = String.format("%d", Integer.MAX_VALUE);

  private final TierLocalTaskRunnerConfig config;
  private final TaskConfig taskConfig;
  private final Properties props;
  private final TaskLogPusher taskLogPusher;
  private final DruidNode node;
  private final ObjectMapper jsonMapper;
  private final PortFinder portFinder;
  private final HttpClient httpClient;
  private final ServiceAnnouncer serviceAnnouncer;
  private final ConcurrentMap<String, ForkingTaskRunnerWorkItem> tasks = new ConcurrentHashMap<>();
  private final AtomicReference<ForkingTaskRunnerState> state = new AtomicReference<>(ForkingTaskRunnerState.INITIALIZING);
  protected final ListeningExecutorService exec; // protected for unit tests
  protected final ScheduledExecutorService heartbeatExec;
  protected final AtomicReference<Future<?>> heartbeatFuture = new AtomicReference<>(null);
  private final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();
  private final Object startStopLock = new Object();

  @Inject
  public TierLocalTaskRunner(
      final TierLocalTaskRunnerConfig config,
      final TaskConfig taskConfig,
      final WorkerConfig workerConfig,
      final Properties props,
      final TaskLogPusher taskLogPusher,
      final ObjectMapper jsonMapper,
      final @Self DruidNode node,
      final @Global HttpClient httpClient,
      final ServiceAnnouncer serviceAnnouncer
  )
  {
    this.config = config;
    this.taskConfig = taskConfig;
    this.props = props;
    this.taskLogPusher = taskLogPusher;
    this.jsonMapper = jsonMapper;
    this.node = node;
    this.portFinder = new PortFinder(config.getStartPort());
    this.httpClient = httpClient;
    this.serviceAnnouncer = serviceAnnouncer;
    this.exec = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            workerConfig.getCapacity(),
            "ForkingTaskWatcherExecutor-%d"
        )
    );
    this.heartbeatExec = Executors.newScheduledThreadPool(
        workerConfig.getCapacity() + 1, // +1 for the overall heartbeat submitter
        Execs.makeThreadFactory("localHeartbeatEmitter-%s")
    );
    if (config.getHeartbeatLocalNetworkTimeout() < config.getDelayBetweenHeartbeatBatches() * 3) {
      log.warn(
          "Heartbeats to peon may overlap. heartbeatLocalNetworkTimeout [%d] too high, or maxHeartbeatRetries [%d] too high, or heartbeatTimeLimit [%d] too low",
          config.getHeartbeatLocalNetworkTimeout(),
          config.getMaxHeartbeatRetries(),
          config.getHeartbeatTimeLimit()
      );
    }
  }

  /**
   * "Attach" to a given task ID. This returns a future which will wait for the given task to end and return the result.
   * This method assumes a task is already running, and attempts to wait for it to finish.
   * This will also create an asynchronous task to do the cleanup and reporting of the task.
   *
   * @param taskId      The task ID of interest
   * @param leaderLatch A leader latch to wait on to determine if this attach instance is the winner in races.
   * @param exec        The executor service to submit tasks to
   *
   * @return A ListenableFuture which will wait on taskId to complete then return its status
   */
  private ListenableFuture<TaskStatus> attach(
      final String taskId,
      final CountDownLatch leaderLatch,
      ListeningExecutorService exec
  )
  {
    final AtomicReference<ForkingTaskRunnerWorkItem> workItemAtomicReference = new AtomicReference<>(null);
    final ListenableFuture<TaskStatus> future = exec.submit(
        new Callable<TaskStatus>()
        {
          @Override
          public TaskStatus call() throws Exception
          {
            if (leaderLatch != null) {
              leaderLatch.await();
            }
            final ForkingTaskRunnerWorkItem workItem = tasks.get(taskId);
            if (workItem == null) {
              throw new NullPointerException(String.format("Task [%s] not found", taskId));
            }
            workItemAtomicReference.set(workItem);
            final ProcessHolder processHolder = workItem.processHolder.get();
            if (processHolder == null) {
              throw new NullPointerException(String.format("Task [%s] has no process holder, cannot attach!", taskId));
            }

            processHolder.awaitShutdown(Long.MAX_VALUE);

            // Give status file a little time to update before we call it failed
            final File statusFile = new File(
                getTaskAttemptDir(processHolder.taskId, processHolder.attemptId),
                STATUS_FILE_NAME
            );
            // Handle race condition on task exiting and updating file. Wait for modifications in the output directory
            if (!statusFile.exists() || statusFile.length() == 0) {
              final Path statusPathParent = statusFile.toPath().getParent();
              long startTime = System.currentTimeMillis();
              try (WatchService watchService = statusPathParent.getFileSystem().newWatchService()) {
                statusPathParent.register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY
                );
                while (!statusFile.exists() || statusFile.length() == 0) {
                  final WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
                  if (key != null) {
                    log.debug("Watch events [%s] for task [%s]", key, taskId);
                  }
                  if (System.currentTimeMillis() - startTime > config.getSoftShutdownTimeLimit()) {
                    break; // error case will be handled below
                  }
                }
              }
            }

            if (statusFile.exists() && statusFile.length() > 0) {
              final TaskStatus status = jsonMapper.readValue(statusFile, TaskStatus.class);
              log.info("Task [%s] exited with status [%s]", processHolder.taskId, status);
              return TaskStatus.fromCode(processHolder.taskId, status.getStatusCode());
            } else {
              log.warn("Unable to find status file at [%s]. Reporting as failed", statusFile);
              return TaskStatus.failure(processHolder.taskId);
            }
          }
        }
    );
    Futures.addCallback(
        future,
        new FutureCallback<TaskStatus>()
        {
          @Override
          public void onSuccess(TaskStatus result)
          {
            // Success of retrieving task status, not success of task
            final ForkingTaskRunnerWorkItem workItem = workItemAtomicReference.get();
            final ProcessHolder processHolder = workItem.processHolder.get();
            uploadLogAndCleanDir(taskId, processHolder.attemptId);
            portFinder.markPortUnused(processHolder.port);
            if (!tasks.remove(taskId, workItem)) {
              log.error("Task state corrupted, work items did not match for [%s] when cleaning up", taskId);
            }
          }

          @Override
          public void onFailure(Throwable t)
          {
            final ForkingTaskRunnerWorkItem workItem = workItemAtomicReference.get();
            if (workItem == null) {
              if (t instanceof CancellationException) {
                log.debug("Task [%s] did not have work item set. Probably didn't win leader election", taskId);
              } else {
                log.error(t, "Error in attaching to task [%s]", taskId);
              }
              return;
            }
            final ProcessHolder processHolder = workItem.processHolder.get();
            if (processHolder == null) {
              log.error("Task [%s] has no process holder, cannot attach!", taskId);
              return;
            }
            try {
              portFinder.markPortUnused(processHolder.port);
              if (t instanceof InterruptedException) {
                log.info("Task watcher for [%s] was interrupted", processHolder);
              } else {
                log.error(t, "Task watcher for [%s] had an error on attaching", processHolder);
              }
            }
            finally {
              if (!tasks.remove(processHolder.taskId, workItem)) {
                log.warn("work item didn't match entry in tasks for [%s]", processHolder.taskId);
              }
            }
          }
        },
        exec
    );
    return future;
  }

  private void uploadLogAndCleanDir(String taskId, String attemptId)
  {
    final File taskAttemptDir = getTaskAttemptDir(taskId, attemptId);
    final File taskDir = getTaskDir(taskId);
    final File logFile = getLogFile(taskId, attemptId);
    try {
      taskLogPusher.pushTaskLog(taskId, logFile);
      int remainingTries = MAX_DELETE_RETRIES;
      while (taskAttemptDir.exists() && remainingTries-- > 0) {
        try {
          FileUtils.deleteDirectory(taskAttemptDir);
          log.debug("Cleaned up [%s]", taskAttemptDir);
        }
        // IOException on race condition on deleting dir, IAE if dir is eliminated between exists check and deleteDirectory's exists check
        catch (IOException | IllegalArgumentException ex) {
          log.debug(ex, "Error cleaning up files at [%s]", taskAttemptDir);
        }
      }
      if (taskAttemptDir.exists()) {
        log.error("Could not cleanup directory [%s]", taskAttemptDir);
      }
      final File lockFile = new File(taskDir, "lock");
      if (lockFile.exists() && !lockFile.delete()) {
        if (lockFile.exists()) {
          log.warn("Could not clean out lock file in [%s]", taskDir);
        }
      }
      if (!taskDir.delete()) {
        log.debug("Could not clear task directory [%s]", taskDir);
      }
    }
    catch (IOException ex) {
      log.error(ex, "Error pushing log file [%s]", logFile);
    }
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    // No special case here, any old tasks are already added to the known tasks at startup
    return ImmutableList.of();
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    throw new UnsupportedOperationException("Not yet supported");
  }

  // General workflow:
  // 1. Create a future which waits for leader election before proceeding
  // 2. Check for leadership by ConcurrentMap putIfAbsent
  // 3. If leader, allow future to continue, else cancel future (which is waiting on latch)
  // 4. Future creates process
  // 5. Future runs process
  // 6. Future calls attach() task in order to wait for task completion.
  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    final CountDownLatch leaderLatch = new CountDownLatch(1);

    // Submit a new task which will launch the job, then wait on an attach to the job
    final ListenableFuture<TaskStatus> startingFuture = exec.submit(
        new Callable<TaskStatus>()
        {
          @Override
          public TaskStatus call()
          {
            try {
              leaderLatch.await();
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw Throwables.propagate(e);
            }
            final int childPort = portFinder.findUnusedPort();
            final File attemptDir = getNewTaskAttemptDir(task.getId());
            final String attemptUUID = attemptDir.getName();
            final ProcessHolder processHolder = new ProcessHolder(task.getId(), attemptUUID, node.getHost(), childPort);
            final Path attemptPath = attemptDir.toPath();

            log.debug("Created directory [%s] for task [%s]", attemptDir, task.getId());

            try {
              final File taskFile = new File(attemptDir, TASK_FILE_NAME);
              final File statusFile = new File(attemptDir, STATUS_FILE_NAME);
              final File logFile = new File(attemptDir, LOG_FILE_NAME);
              final File deadhandFile = new File(attemptDir, DEADHAND_FILE_NAME);

              if (!taskFile.exists() && !taskFile.createNewFile()) {
                throw new IOException(String.format("Could not create file [%s]", taskFile));
              }
              if (!statusFile.exists() && !statusFile.createNewFile()) {
                throw new IOException(String.format("Could not create file [%s]", statusFile));
              }
              if (!logFile.exists() && !logFile.createNewFile()) {
                throw new IOException(String.format("Could not create file [%s]", logFile));
              }
              if (!deadhandFile.exists() && !deadhandFile.createNewFile()) {
                throw new IOException(String.format("Could not create file [%s]", deadhandFile));
              }

              // time to adjust process holders
              final ForkingTaskRunnerWorkItem taskWorkItem = tasks.get(task.getId());

              if (taskWorkItem == null) {
                log.makeAlert("WTF?! TaskInfo disappeared!").addData("task", task.getId()).emit();
                throw new ISE("TaskInfo disappeared for task[%s]!", task.getId());
              }

              if (taskWorkItem.shutdown.get()) {
                throw new IllegalStateException("Task has been shut down!");
              }

              if (taskWorkItem.processHolder.get() != null) {
                // Fail early, there is also a last second check later on
                log.makeAlert("WTF?! TaskInfo already has a processHolder")
                   .addData("task", task.getId())
                   .emit();
                throw new ISE("TaskInfo already has processHolder for task[%s]!", task.getId());
              }

              final List<String> command = Lists.newArrayList();
              final String childHost = node.getHost();
              final String taskClasspath;
              if (task.getClasspathPrefix() != null && !task.getClasspathPrefix().isEmpty()) {
                taskClasspath = Joiner.on(File.pathSeparator).join(
                    task.getClasspathPrefix(),
                    config.getClasspath()
                );
              } else {
                taskClasspath = config.getClasspath();
              }

              command.add(config.getJavaCommand());
              command.add("-cp");
              command.add(taskClasspath);

              Iterables.addAll(
                  command,
                  new QuotableWhiteSpaceSplitter(config.getJavaOpts())
              );
              Iterables.addAll(
                  command,
                  config.getJavaOptsArray()
              );

              for (String propName : props.stringPropertyNames()) {
                for (String allowedPrefix : config.getAllowedPrefixes()) {
                  if (propName.startsWith(allowedPrefix)) {
                    command.add(
                        String.format(
                            "-D%s=%s",
                            propName,
                            props.getProperty(propName)
                        )
                    );
                  }
                }
              }

              // Override child JVM specific properties
              for (String propName : props.stringPropertyNames()) {
                if (propName.startsWith(CHILD_PROPERTY_PREFIX)) {
                  command.add(
                      String.format(
                          "-D%s=%s",
                          propName.substring(CHILD_PROPERTY_PREFIX.length()),
                          props.getProperty(propName)
                      )
                  );
                }
              }

              command.add(String.format("-Ddruid.host=%s", childHost));
              command.add(String.format("-Ddruid.port=%d", childPort));

              command.add(String.format("-D" + TASKID_PROPERTY + "=%s", task.getId()));

              command.add("io.druid.cli.Main");
              command.add("tier");
              command.add("fork");

              String nodeType = task.getNodeType();
              if (nodeType != null) {
                command.add("--nodeType");
                command.add(nodeType);
              }

              // Needed for legacy CliPeon support
              command.add(taskFile.getAbsolutePath());
              command.add(statusFile.getAbsolutePath());

              jsonMapper.writeValue(taskFile, task);
              try (WatchService watchService = attemptPath.getFileSystem().newWatchService()) {
                attemptPath.register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY
                );
                log.info("Running command: %s", Joiner.on(" ").join(command));
                // Process can continue running in the background. We monitor via files rather than Process
                final Process process = new ProcessBuilder(command)
                    .redirectError(logFile)
                    .redirectOutput(logFile)
                    .directory(attemptDir)
                    .start();
                if (!taskWorkItem.processHolder.compareAndSet(null, processHolder)) {
                  final String msg = String.format(
                      "WTF!? Expected empty process holder and found [%s]",
                      taskWorkItem.processHolder.get()
                  );
                  log.makeAlert("%s", msg).emit();
                  throw new ISE("%s", msg);
                }

                log.info("Logging task %s output to: %s", task.getId(), logFile);
                // Wait for files to be modified by task starting
                log.debug("Waiting for task [%s] to start", processHolder);
                watchService.take();// Should only be modified by task
                log.debug("Waiting for task [%s] to finish", processHolder);

                return attach(task.getId(), leaderLatch, MoreExecutors.sameThreadExecutor()).get();
              }
            }
            catch (InterruptedException e) {
              log.info("Interrupted while waiting for task [%s]", processHolder);
              Thread.currentThread().interrupt();
              throw Throwables.propagate(e);
            }
            catch (ExecutionException e) {
              final Throwable eCause = e.getCause();
              if (eCause instanceof InterruptedException) {
                log.info(e, "Attaching interrupted for [%s]", processHolder);
                Thread.currentThread().interrupt();
              } else {
                log.info(e, "Exception during execution of attach for [%s]", processHolder);
              }
              throw Throwables.propagate(e);
            }
            catch (Throwable t) {
              log.info(t, "Exception caught during forking of [%s]", processHolder);
              throw Throwables.propagate(t);
            }
          }
        }
    );
    try {
      final ForkingTaskRunnerWorkItem workItem = new ForkingTaskRunnerWorkItem(task.getId(), startingFuture);
      // Leader election for task id
      final ForkingTaskRunnerWorkItem leaderItem = tasks.putIfAbsent(task.getId(), workItem);
      if (leaderItem != null) {
        if (!startingFuture.cancel(true)) {
          log.makeAlert("Task [%s] had a race condition and couldn't stop!", task.getId()).emit();
        }
        log.warn("Already have task id [%s], returning prior task instead", task.getId());
        return leaderItem.getResult();
      } else {
        return workItem.getResult();
      }
    }
    finally {
      leaderLatch.countDown();
    }
  }


  // This assumes that no task directories can be created except in the ForkingTaskRunner,
  // And that the ForkingTaskRunner has exclusive ownership of the directory structure
  @Override
  @LifecycleStart
  public void start()
  {
    synchronized (startStopLock) {
      if (!state.compareAndSet(ForkingTaskRunnerState.INITIALIZING, ForkingTaskRunnerState.STARTING)) {
        throw new ISE(
            "Invalid state for starting: Expected [%s] found [%s]",
            ForkingTaskRunnerState.INITIALIZING,
            state.get()
        );
      }

      populateMissingTasksFromDir();
      serviceAnnouncer.announce(node);
      final Future<?> heartbeatFuture = heartbeatExec.scheduleAtFixedRate(
          new Runnable()
          {
            @Override
            public void run()
            {
              for (final ForkingTaskRunnerWorkItem workItem : tasks.values()) {
                if (workItem.shutdown.get()) {
                  continue;
                }
                final ProcessHolder holder = workItem.processHolder.get();
                if (holder == null) {
                  continue;
                }
                final int port = holder.port;
                if (port == 0) {
                  continue;
                }

                heartbeatExec.submit(
                    new Runnable()
                    {
                      @Override
                      public void run()
                      {
                        try {
                          final URL url = new URL(
                              "http",
                              "localhost",
                              port,
                              DeadhandResource.DEADHAND_PATH
                          );
                          log.trace("Starting deadhand probe to [%s]", url);
                          httpClient.go(
                              new Request(HttpMethod.POST, url),
                              new HttpResponseHandler<Object, Object>()
                              {
                                @Override
                                public ClientResponse<Object> handleResponse(HttpResponse response)
                                {
                                  log.debug("heartbeat response for port [%d]: [%s]", port, response.getStatus());
                                  return ClientResponse.finished(null);
                                }

                                @Override
                                public ClientResponse<Object> handleChunk(
                                    ClientResponse<Object> clientResponse, HttpChunk chunk
                                )
                                {
                                  log.trace("chunk response for heartbeat on port [%d]", port);
                                  return clientResponse;
                                }

                                @Override
                                public ClientResponse<Object> done(ClientResponse<Object> clientResponse)
                                {
                                  log.trace("done with heartbeat for port [%d]", port);
                                  return clientResponse;
                                }

                                @Override
                                public void exceptionCaught(ClientResponse<Object> clientResponse, Throwable e)
                                {
                                  log.error(e, "Error in url [%s]", url);
                                }
                              },
                              Duration.millis(config.getHeartbeatLocalNetworkTimeout())
                          ).get(); // So we don't clog the pipeline
                        }
                        catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw Throwables.propagate(e);
                        }
                        catch (Exception e) {
                          log.warn(e, "Error in submitting heartbeat on port [%d]", port);
                        }
                      }
                    }
                ); // We don't care about failures
              }
            }
          },
          0,
          config.getDelayBetweenHeartbeatBatches(),
          TimeUnit.MILLISECONDS
      );
      if (!this.heartbeatFuture.compareAndSet(null, heartbeatFuture)) {
        if (!heartbeatFuture.cancel(true)) {
          log.makeAlert("Error canceling duplicate heartbeat emitter").emit();
        }
        throw new ISE("Heartbeat future was not null");
      }
      if (!state.compareAndSet(ForkingTaskRunnerState.STARTING, ForkingTaskRunnerState.STARTED)) {
        throw new ISE(
            "Someone is screwing around with my start state! Expected [%s] found [%s]",
            ForkingTaskRunnerState.STARTING,
            state.get()
        );
      }
      log.info("Started");
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    synchronized (startStopLock) {
      final ForkingTaskRunnerState s = state.get();
      if (ForkingTaskRunnerState.STOPPED.equals(s) || ForkingTaskRunnerState.STOPPING.equals(s)) {
        log.debug("Already stopped, ignoring");
        return;
      }
      if (!state.compareAndSet(ForkingTaskRunnerState.STARTED, ForkingTaskRunnerState.STOPPING)) {
        if (ForkingTaskRunnerState.STOPPING.equals(s) || ForkingTaskRunnerState.STOPPED.equals(s)) {
          log.info("Stop called multiple times. Ignoring stop request");
          return;
        } else {
          throw new ISE("Invalid state to stop. Expected [%s] found [%s]", ForkingTaskRunnerState.STARTED, s);
        }
      }
      serviceAnnouncer.unannounce(node);
      exec.shutdown();
      final Future<?> heartbeatFuture = this.heartbeatFuture.get();
      if (heartbeatFuture != null) {
        heartbeatFuture.cancel(false);
      }
      if (!(this.heartbeatFuture.compareAndSet(heartbeatFuture, null))) {
        log.error("Illegal state, heartbeat changed during stop()");
      }
      heartbeatExec.shutdown();
      if (!state.compareAndSet(ForkingTaskRunnerState.STOPPING, ForkingTaskRunnerState.STOPPED)) {
        throw new ISE(
            "Someone is screwing with my shutdown state! Expected [%s] found [%s]",
            ForkingTaskRunnerState.STOPPING,
            state.get()
        );
      }
      log.info("Stopped");
    }
  }

  @Override
  public void shutdown(final String taskid)
  {
    final ForkingTaskRunnerWorkItem taskInfo = tasks.get(taskid);

    if (taskInfo == null) {
      log.info("Ignoring request to cancel unknown task: %s", taskid);
      return;
    }

    if (!taskInfo.shutdown.compareAndSet(false, true)) {
      log.warn("Someone already shut down task [%s]. Ignoring request", taskid);
      return;
    }

    final ProcessHolder processHolder = taskInfo.processHolder.get();
    if (processHolder == null) {
      log.wtf("Task has no process holder!?");
      return;
    }
    // Check to see if foreign process needs to be killed
    if (processHolder.deadhandFile.exists()) {
      if (!processHolder.deadhandFile.delete() && processHolder.deadhandFile.exists()) {
        throw new RE("Could not remove file at [%s]", processHolder.deadhandFile);
      }
      log.info("Attempting shutdown via deletion of [%s]", processHolder.deadhandFile);
    }
    try {
      try {
        processHolder.awaitShutdown(config.getSoftShutdownTimeLimit());
      }
      catch (TimeoutException e) {
        log.info(
            "Timed out waiting for clean shutdown on task [%s]. Forcing shutdown...",
            taskInfo.processHolder.get()
        );
        if (!forceKill(processHolder)) {
          if (processHolder.taskPortFile.exists()) {
            throw new RuntimeException("Unable to shutdown task!");
          } else {
            log.info("Task shutdown on its own");
          }
        }
      }
      taskInfo.getResult().get();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    catch (ExecutionException e) {
      if (e.getCause() instanceof InterruptedException) {
        log.info("Interrupted while waiting for shutdown");
        return;
      }
      throw Throwables.propagate(e);
    }
  }

  /**
   * Unix (and maybe Oracle VM) specific killer for processes
   *
   * @param processHolder The process holder of interest
   *
   * @return True if the task was killed via this method, false otherwise.
   *
   * @throws InterruptedException If the waiting on system `kill` commands is interrupted.
   */
  private boolean forceKill(final ProcessHolder processHolder) throws InterruptedException
  {
    final String portString = Integer.toString(processHolder.port);
    final List<VirtualMachineDescriptor> vms = ImmutableList.copyOf(
        Collections2.filter(
            VirtualMachine.list(),
            new Predicate<VirtualMachineDescriptor>()
            {
              @Override
              public boolean apply(VirtualMachineDescriptor input)
              {
                try {
                  return portString.equals(
                      input.provider()
                           .attachVirtualMachine(input)
                           .getSystemProperties()
                           .getProperty("druid.port")
                  );
                }
                catch (IOException | AttachNotSupportedException e) {
                  log.warn(e, "Could not read property from vm");
                  return false;
                }
              }
            }
        )
    );
    if (vms.isEmpty()) {
      log.warn("Could not find vm for taskid [%s] using port [%d]!", processHolder.taskId, processHolder.port);
      return false;
    }
    final VirtualMachineDescriptor vmd = vms.get(0);
    try {
      final int pid = Integer.parseInt(vmd.id());
      log.info("Forcing kill of task [%s] on pid [%d]", processHolder.taskId, pid);
    }
    catch (NumberFormatException e) {
      log.error("Could not find pid for task [%s]. VM id [%s] is not an integer", processHolder.taskId, vmd.id());
      return false;
    }
    final File tmpFile;
    try {
      tmpFile = File.createTempFile("kill_output", ".tmp");
    }
    catch (IOException e) {
      log.error(e, "Could not create output file to kill task [%s] on port [%d]", processHolder.taskId, vmd.id());
      return false;
    }
    try {
      Process killingProcess = new ProcessBuilder(ImmutableList.of("kill", "-15", vmd.id()))
          .redirectOutput(tmpFile)
          .redirectError(tmpFile)
          .start();
      int retval = killingProcess.waitFor();
      if (retval == 0) {
        processHolder.awaitShutdown(config.getSoftShutdownTimeLimit());
        return true;
      }
      try (InputStream inputStream = new FileInputStream(tmpFile)) {
        Scanner scanner = new Scanner(inputStream).useDelimiter("\\A");
        log.error(
            "Term of pid [%s] did not succeed with code [%d]: [%s]",
            vmd.id(),
            retval,
            scanner.hasNext() ? scanner.next() : "null"
        );
      }
      tmpFile.delete();
      tmpFile.createNewFile();
      killingProcess = new ProcessBuilder(ImmutableList.of("kill", "-9", vmd.id()))
          .redirectOutput(tmpFile)
          .redirectError(tmpFile)
          .start();
      retval = killingProcess.waitFor();
      if (retval == 0) {
        processHolder.awaitShutdown(config.getSoftShutdownTimeLimit());
        return true;
      }
      try (InputStream inputStream = new FileInputStream(tmpFile)) {
        Scanner scanner = new Scanner(inputStream).useDelimiter("\\A");
        log.error(
            "Kill of pid [%s] did not succeed with code [%d]: [%s]",
            vmd.id(),
            retval,
            scanner.hasNext() ? scanner.next() : "null"
        );
      }
      return false;
    }
    catch (IOException | TimeoutException e) {
      throw Throwables.propagate(e);
    }
    finally {
      tmpFile.delete();
    }
  }

  @Override
  public Collection<TaskRunnerWorkItem> getRunningTasks()
  {
    return Collections2.transform(
        Collections2.filter(
            tasks.values(),
            new Predicate<ForkingTaskRunnerWorkItem>()
            {
              @Override
              public boolean apply(ForkingTaskRunnerWorkItem input)
              {
                return input.processHolder.get() != null;
              }
            }
        ),
        new Function<ForkingTaskRunnerWorkItem, TaskRunnerWorkItem>()
        {
          @Override
          public TaskRunnerWorkItem apply(ForkingTaskRunnerWorkItem input)
          {
            return input;
          }
        }
    );
  }

  @Override
  public Collection<TaskRunnerWorkItem> getPendingTasks()
  {
    return Collections2.transform(
        Collections2.filter(
            tasks.values(),
            new Predicate<ForkingTaskRunnerWorkItem>()
            {
              @Override
              public boolean apply(ForkingTaskRunnerWorkItem input)
              {
                return input.processHolder.get() == null;
              }
            }
        ),
        new Function<ForkingTaskRunnerWorkItem, TaskRunnerWorkItem>()
        {
          @Override
          public TaskRunnerWorkItem apply(ForkingTaskRunnerWorkItem input)
          {
            return input;
          }
        }
    );
  }

  @Override
  public Collection<TaskRunnerWorkItem> getKnownTasks()
  {
    return ImmutableList.<TaskRunnerWorkItem>copyOf(tasks.values());
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.absent();
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskid, final long offset)
  {
    final ForkingTaskRunnerWorkItem taskWorkItem = tasks.get(taskid);

    if (taskWorkItem == null) {
      return Optional.absent();
    }

    final ProcessHolder processHolder = taskWorkItem.processHolder.get();

    if (processHolder == null) {
      return Optional.absent();
    }

    final File logFile = getLogFile(processHolder);

    if (!logFile.exists()) {
      return Optional.absent();
    }

    return Optional.<ByteSource>of(
        new ByteSource()
        {
          @Override
          public InputStream openStream() throws IOException
          {
            return LogUtils.streamFile(logFile, offset);
          }
        }
    );
  }

  private Collection<File> getAttemptDirs()
  {
    final File baseDir = taskConfig.getBaseTaskDir();
    final File[] taskDirFileArray = baseDir.listFiles();
    final Collection<File> taskDirFileList =
        taskDirFileArray == null ?
        ImmutableList.<File>of() :
        Collections2.filter(
            Arrays.asList(
                taskDirFileArray
            ), new Predicate<File>()
            {
              @Override
              public boolean apply(File input)
              {
                return input.exists() && input.isDirectory();
              }
            }
        );
    if (taskDirFileList.isEmpty()) {
      log.info("No task dirs found in [%s]", baseDir);
      return ImmutableList.of();
    }
    return taskDirFileList;
  }

  private void populateMissingTasksFromDir()
  {
    if (!ForkingTaskRunnerState.STARTING.equals(state.get())) {
      // This might be safe to do, but this method assumes this is not the case
      throw new ISE("Cannot populate tasks from dirs once ForkingTaskRunner has been started");
    }

    final Collection<File> taskDirFileList = getAttemptDirs();
    if (taskDirFileList.isEmpty()) {
      log.info("No prior task attempts found");
      return;
    }
    log.debug("Looking for files in %s", taskDirFileList);
    final ListeningExecutorService lookingExecutor = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            taskDirFileList.size(),
            "localTaskDiscovery-%s"
        )
    );
    final ArrayList<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(taskDirFileList.size());
    // For the task directories, look for attempt directories
    for (final File potentialTaskDir : taskDirFileList) {
      futures.add(
          lookingExecutor.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  final File[] taskAttemptDirFileArray = potentialTaskDir.listFiles();
                  final Collection<File> taskAttemptDirFileList = // Only directories containing a non-zero TASK_FILE_NAME file
                      taskAttemptDirFileArray == null ?
                      ImmutableList.<File>of() :
                      Collections2.filter(
                          Arrays.asList(taskAttemptDirFileArray),
                          new Predicate<File>()
                          {
                            @Override
                            public boolean apply(File input)
                            {
                              return input.isDirectory() && input.listFiles(
                                  new FilenameFilter()
                                  {
                                    @Override
                                    public boolean accept(File dir, String name)
                                    {
                                      return TASK_FILE_NAME.equals(name)
                                             && new File(dir, TASK_FILE_NAME).length()
                                                > 0;
                                    }
                                  }
                              ) != null; // Did we find any?
                            }
                          }
                      );
                  if (taskAttemptDirFileList.isEmpty()) {
                    log.info(
                        "Directory [%s] has no viable task attempts, attempting to cleanup if empty",
                        potentialTaskDir
                    );
                    if (!potentialTaskDir.delete()) {
                      log.warn("Could not clean up [%s]", potentialTaskDir);
                    }
                    return;
                  } else {
                    log.debug("Found viable task in [%s]", potentialTaskDir);
                  }
                  // Find latest attempt in directory
                  long last_attempt = 0;
                  File latestAttemptDir = null;
                  for (File taskAttemptDir : taskAttemptDirFileList) {
                    if (!taskAttemptDir.isDirectory()) {
                      log.debug("Skipping non-directory [%s]", taskAttemptDir);
                      continue;
                    }
                    try {
                      final long check_attempt = Long.parseLong(taskAttemptDir.getName());
                      if (check_attempt > last_attempt) {
                        latestAttemptDir = getTaskAttemptDir(taskAttemptDir.getName(), check_attempt);
                        last_attempt = check_attempt;
                      }
                      if (latestAttemptDir == null) {
                        latestAttemptDir = taskAttemptDir;
                      } else {
                        if (latestAttemptDir.lastModified() < taskAttemptDir.lastModified()) {
                          latestAttemptDir = taskAttemptDir;
                        }
                      }
                    }
                    catch (NumberFormatException e) {
                      log.info(e, "Skipping unparsable directory [%s]", taskAttemptDir);
                    }
                  }
                  if (latestAttemptDir == null) {
                    log.warn("Didn't find any viable attempts among %s", taskAttemptDirFileList);
                    return;
                  }

                  //------------------------------------------------------------------ Load up data from suspected good attempt dirs

                  // We already checked earlier that this exists and is non zero
                  final File taskFile = new File(latestAttemptDir, TASK_FILE_NAME);
                  final Task task;
                  try {
                    task = jsonMapper.readValue(taskFile, Task.class);
                  }
                  catch (IOException e) {
                    log.makeAlert(e, "Corrupted task file at [%s]", taskFile).emit();
                    return;
                  }

                  final File statusFile = new File(latestAttemptDir, STATUS_FILE_NAME);

                  if (!statusFile.exists()) {
                    // Shouldn't be missing unless there's corruption somehow.
                    log.makeAlert("Status file [%s] is missing ", statusFile).emit();
                    return;
                  }

                  final File portFile = new File(latestAttemptDir, PORT_FILE_NAME);
                  Integer port = null;
                  try {
                    port = getPort(portFile, task.getId());
                  }
                  catch (IOException e) {
                    log.makeAlert(
                        e,
                        "Error reading port file [%s] for task [%s] in dir [%s]",
                        portFile,
                        task.getId(),
                        latestAttemptDir
                    ).emit();
                    return;
                  }
                  if (port == null) {
                    // At this point there should be one of two scenarios:
                    // A) The peon is still starting up and hasn't written the port file yet
                    // B) The peon has exited between ForkingTaskRunner instances
                    // We'll handle A first
                    final Path portPathParent = portFile.toPath().getParent();
                    try (WatchService watchService = portPathParent.getFileSystem().newWatchService()) {
                      final long start = System.currentTimeMillis();
                      while (port == null) {
                        port = getPort(portFile, task.getId());
                        watchService.poll(1, TimeUnit.SECONDS);
                        if (System.currentTimeMillis() - start > config.getSoftShutdownTimeLimit()) {
                          log.info(
                              "Timeout exceeded while waiting for task [%s] to publish its port information",
                              task.getId()
                          );
                          break;
                        }
                      }
                    }
                    catch (IOException e) {
                      log.makeAlert(
                          e,
                          "Error reading port file [%s] for task [%s] in dir [%s]",
                          portFile,
                          task.getId(),
                          latestAttemptDir
                      ).emit();
                      return; // Skip so it can be investigated. On IOException cleanup probably won't help anyways
                    }
                    catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw Throwables.propagate(e);
                    }
                  }

                  if (port == null) {
                    // We think the task really is dead
                    log.debug("Found no port file for task [%s]. Uploading log and cleaning", task.getId());
                    final CountDownLatch doneLatch = new CountDownLatch(1);
                    try {
                      final ListenableFuture<TaskStatus> future;
                      final ForkingTaskRunnerWorkItem workItem;
                      try {
                        future = attach(task.getId(), doneLatch, exec);
                        workItem = new ForkingTaskRunnerWorkItem(task.getId(), future);
                        workItem.processHolder = new AtomicReference<>(
                            new ProcessHolder(
                                task.getId(),
                                latestAttemptDir.getName(),
                                node.getHost(),
                                0
                            )
                        );
                        tasks.put(task.getId(), workItem);
                      }
                      finally {
                        doneLatch.countDown();
                      }
                      future.get();
                      workItem.processHolder.get().awaitShutdown(100L);
                    }
                    catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw Throwables.propagate(e);
                    }
                    catch (TimeoutException | ExecutionException e) {
                      log.makeAlert(e, "Could upload data for task [%s] which finished between runs", task.getId())
                         .emit();
                    }
                    return;
                  }
                  final ProcessHolder processHolder = new ProcessHolder(
                      task.getId(),
                      latestAttemptDir.getName(),
                      node.getHost(),
                      port
                  );
                  final CountDownLatch leaderLatch = new CountDownLatch(1);
                  final ForkingTaskRunnerWorkItem workItem = new ForkingTaskRunnerWorkItem(
                      task.getId(),
                      attach(task.getId(), leaderLatch, exec)
                  );
                  try {
                    workItem.processHolder.set(processHolder);
                    if (tasks.putIfAbsent(task.getId(), workItem) != null) {
                      log.warn("Task [%s] already exists!", task.getId());
                      workItem.getResult().cancel(true);
                    } else {
                      log.info("Found task [%s] in progress", processHolder);
                    }
                  }
                  finally {
                    leaderLatch.countDown();
                  }
                }
              }
          )
      );
    }
    try {
      // Exceptions that are recoverable are handled within the Runnable
      // Anything that makes it up this high really iss a problem
      Futures.allAsList(futures).get();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
    finally {
      lookingExecutor.shutdown();
    }
    log.info("Finished parsing potential tasks in %s", taskDirFileList);
  }

  /**
   * Returns the port number of the given task in the particular attempt directory.
   * The PORT_FILE_NAME is the intended indicator for the JVM instance itself.
   * Only the PEON is in charge of writing or deleting this file.
   *
   * @param portFile The file where the port number should be located as a UTF-8 string
   * @param taskId   The taskId for this request (used for logging)
   *
   * @return The port in use by the attempt or null if it is empty/not found
   *
   * @throws IOException on error in the underlying file (either corrupt file at the FS level or has nonsense contents)
   */
  private static Integer getPort(File portFile, String taskId) throws IOException
  {
    log.debug("Checking port file [%s] for taskId [%s] in [%s]", portFile, taskId, portFile.getParent());
    Integer port = null;
    if (!portFile.exists()) {
      return null;
    }
    try (FileChannel portFileChannel = FileChannel.open(
        portFile.toPath(),
        StandardOpenOption.READ,
        StandardOpenOption.WRITE // Required for lock
    )) {
      final ByteBuffer buffer;
      final FileLock fileLock = portFileChannel.lock(); // To make sure the peon is done writing before we try to read
      try {
        final long fileSize = portFileChannel.size();
        if (fileSize > LONGEST_POSITIVE_INTEGER.length()) {
          // Probably should never happen
          throw new IOException(
              String.format(
                  "port file [%s] for task [%s] is HUGE %d bytes",
                  portFile,
                  taskId,
                  fileSize
              )
          );
        }
        buffer = ByteBuffer.allocate((int) fileSize);
        for (int totalRead = 0, thisRead; totalRead < fileSize; totalRead += thisRead) {
          thisRead = portFileChannel.read(buffer);
        }
        buffer.rewind();
      }
      finally {
        fileLock.release();
      }
      final String portString = StringUtils.fromUtf8(buffer.array());
      port = Integer.parseInt(portString);
    }
    catch (FileNotFoundException e) {
      log.info(e, "Task [%s] attempt [%s] has no port file", taskId, portFile.getParent());
      return null;
    }
    catch (IOException | NumberFormatException e) {
      if (portFile.exists()) {
        // Something went wrong during write of value from peon's side
        log.makeAlert(e, "Port file [%s] for task [%s] is corrupt", portFile, taskId).emit();
        throw new IOException(String.format("Corrupt port file [%s]", portFile), e);
      } else {
        // Exited during read
        log.info(e, "Task [%s] attempt [%s] exited during read", taskId, portFile.getParent());
      }
      return null;
    }
    return port;
  }

  private File getTaskDir(String taskId)
  {
    return new File(taskConfig.getBaseTaskDir(), taskId);
  }

  private File getNewTaskAttemptDir(String taskId)
  {
    final File taskDir = getTaskDir(taskId);
    if (!taskDir.exists()) {
      if (!taskDir.mkdirs() && !taskDir.exists()) {
        throw new RuntimeException(new IOException(String.format("Unable to create file at [%s]", taskDir)));
      }
    }
    if (!taskDir.isDirectory()) {
      throw new RuntimeException(new IOException(String.format("[%s] not a directory", taskDir)));
    }
    final File[] files = taskDir.listFiles();
    long attempt_num = 0;
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          try {
            long attempt = Long.parseLong(file.getName());
            if (attempt > attempt_num) {
              attempt_num = attempt;
            }
          }
          catch (NumberFormatException e) {
            log.debug(e, "couldn't parse directory [%s]", file);
          }
        }
      }
    }
    File file;
    long attempt = attempt_num + 1;
    do {
      file = getTaskAttemptDir(taskId, attempt++);
    } while (!file.mkdirs());
    return file;
  }

  private File getTaskAttemptDir(String taskId, long attempt_num)
  {
    Preconditions.checkArgument(attempt_num < 10_000, "attempt_num < 10_000");
    return new File(getTaskDir(taskId), String.format("%04d", attempt_num));
  }

  private File getTaskAttemptDir(String taskId, String attemptId)
  {
    return new File(getTaskDir(taskId), attemptId);
  }

  private File getLogFile(String taskId, String attemptId)
  {
    return new File(getTaskAttemptDir(taskId, attemptId), LOG_FILE_NAME);
  }

  private File getLogFile(ProcessHolder processHolder)
  {
    return getLogFile(processHolder.taskId, processHolder.attemptId);
  }

  private static class ForkingTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    private AtomicBoolean shutdown = new AtomicBoolean(false);
    private AtomicReference<ProcessHolder> processHolder = new AtomicReference<>(null);

    private ForkingTaskRunnerWorkItem(
        String taskId,
        ListenableFuture<TaskStatus> statusFuture
    )
    {
      super(taskId, statusFuture);
    }

    @Override
    public TaskLocation getLocation()
    {
      final ProcessHolder processHolder = this.processHolder.get();
      if (processHolder == null) {
        return TaskLocation.unknown();
      }
      return TaskLocation.create(processHolder.localhost, processHolder.port);
    }
  }

  private class ProcessHolder
  {
    private final String taskId;
    private final String attemptId;
    private final int port;
    private final String localhost;
    private final File attemptDir;
    private final File taskPortFile;
    private final File deadhandFile;

    private ProcessHolder(String taskId, String attemptId, String localhost, int port)
    {
      this.taskId = taskId;
      this.attemptId = attemptId;
      this.localhost = localhost;
      this.port = port;
      attemptDir = getTaskAttemptDir(taskId, attemptId);
      taskPortFile = new File(attemptDir, PORT_FILE_NAME);
      deadhandFile = new File(attemptDir, DEADHAND_FILE_NAME);
    }

    public void awaitShutdown(long timeoutMS) throws InterruptedException, TimeoutException
    {
      final long startTime = System.currentTimeMillis();
      final Path taskPath = taskPortFile.toPath();
      boolean retry = true;
      while (retry) {
        try (WatchService watchService = taskPath.getFileSystem().newWatchService()) {
          taskPath.getParent().register(watchService, StandardWatchEventKinds.ENTRY_DELETE);
          while (taskPortFile.exists()) {
            final long delta = System.currentTimeMillis() - startTime;
            if (timeoutMS <= delta) {
              throw new TimeoutException("Waiting for the right delete event");
            }
            // Ignore result, we use a check for the file explicitly in the loop
            watchService.poll(100, TimeUnit.MILLISECONDS);
          }
          retry = false;
        }
        catch (IOException e) {
          log.warn(e, "Exception in watch service");
          if (System.currentTimeMillis() - startTime > timeoutMS) {
            final TimeoutException timeoutException = new TimeoutException("Watch service error");
            timeoutException.addSuppressed(e);
            throw timeoutException;
          }
        }
      }
    }

    @Override
    public String toString()
    {
      return "ProcessHolder{" +
             "taskId='" + taskId + '\'' +
             ", attemptId='" + attemptId + '\'' +
             ", port=" + port +
             '}';
    }
  }

  // If start() has finished
  public boolean isStarted(boolean block)
  {
    final ForkingTaskRunnerState state;
    if (block) {
      synchronized (startStopLock) {
        state = this.state.get();
      }
    } else {
      state = this.state.get();
    }
    return ForkingTaskRunnerState.STARTED.equals(state);
  }

  // If stop() has finished
  public boolean isStopped()
  {
    return ForkingTaskRunnerState.STOPPED.equals(state.get());
  }
}
