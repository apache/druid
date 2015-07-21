/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
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
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.tasklogs.LogUtils;
import io.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.server.DruidNode;
import io.druid.tasklogs.TaskLogPusher;
import io.druid.tasklogs.TaskLogStreamer;
import org.apache.commons.io.FileUtils;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs tasks in separate processes using the "internal peon" verb.
 * <p/>
 * The peon is responsible for creating this file when it starts, and deleting it during shutdown. It is a simple way to signify if the task is still running.
 * <p/>
 * It also allows searching through the running JVMs for one that has a port set to this value to find the JVM instance.
 */
public class ForkingTaskRunner implements TaskRunner, TaskLogStreamer
{
  public static final String TASKID_PROPERTY = "io.druid.indexing.worker.taskid";
  private static final String PORT_FILE_NAME = "task.port";
  private static final String LOG_FILE_NAME = "task.log";
  private static final String TASK_FILE_NAME = "task.json";
  private static final String STATUS_FILE_NAME = "status.json";
  private static final EmittingLogger log = new EmittingLogger(ForkingTaskRunner.class);
  private static final String CHILD_PROPERTY_PREFIX = "druid.indexer.fork.property.";
  private static final Splitter whiteSpaceSplitter = Splitter.on(CharMatcher.WHITESPACE).omitEmptyStrings();
  private static final int MAX_DELETE_RETRIES = 3; // How many times should we try to delete the attempt dir on cleanup

  private final ForkingTaskRunnerConfig config;
  private final TaskConfig taskConfig;
  private final WorkerConfig workerConfig;
  private final Properties props;
  private final TaskLogPusher taskLogPusher;
  private final DruidNode node;
  private final ObjectMapper jsonMapper;
  private final PortFinder portFinder;
  private final HttpClient httpClient;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final ConcurrentMap<String, ForkingTaskRunnerWorkItem> tasks = new ConcurrentHashMap<>();
  protected final ListeningExecutorService exec; // protected for unit tests

  @Inject
  public ForkingTaskRunner(
      ForkingTaskRunnerConfig config,
      TaskConfig taskConfig,
      WorkerConfig workerConfig,
      Properties props,
      TaskLogPusher taskLogPusher,
      ObjectMapper jsonMapper,
      @Self DruidNode node,
      @Global HttpClient httpClient
  )
  {
    this.config = config;
    this.taskConfig = taskConfig;
    this.workerConfig = workerConfig;
    this.props = props;
    this.taskLogPusher = taskLogPusher;
    this.jsonMapper = jsonMapper;
    this.node = node;
    this.portFinder = new PortFinder(config.getStartPort());
    this.httpClient = httpClient;
    this.exec = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            workerConfig.getCapacity(),
            "ForkingTaskWatcherExecutor-%d"
        )
    );
  }

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
            final File statusFile = new File(
                getTaskAttemptDir(processHolder.taskId, processHolder.attemptId),
                STATUS_FILE_NAME
            );
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
              if(t instanceof InterruptedException) {
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
    // Success of retrieving TaskStatus, not success of task
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
      if (!taskDir.delete()) {
        log.debug("Could not clear task directory [%s]", taskDir);
      }
    }
    catch (IOException ex) {
      log.error(ex, "Error pushing log file [%s]", logFile);
    }
  }

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
            final ProcessHolder processHolder = new ProcessHolder(task.getId(), attemptUUID, childPort);
            final Path attemptPath = attemptDir.toPath();

            try {
              final File taskFile = new File(attemptDir, TASK_FILE_NAME);
              final File statusFile = new File(attemptDir, STATUS_FILE_NAME);
              final File logFile = new File(attemptDir, LOG_FILE_NAME);
              final File portFile = new File(attemptDir, PORT_FILE_NAME);

              if (!taskFile.exists() && !taskFile.createNewFile()) {
                throw new IOException(String.format("Could not create file [%s]", taskFile));
              }
              if (!statusFile.exists() && !statusFile.createNewFile()) {
                throw new IOException(String.format("Could not create file [%s]", statusFile));
              }
              if (!logFile.exists() && !logFile.createNewFile()) {
                throw new IOException(String.format("Could not create file [%s]", logFile));
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

              Iterables.addAll(command, whiteSpaceSplitter.split(config.getJavaOpts()));

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
              command.add("internal");
              command.add("peon");
              command.add(taskFile.toString());
              command.add(statusFile.toString());
              command.add(portFile.toString());
              String nodeType = task.getNodeType();
              if (nodeType != null) {
                command.add("--nodeType");
                command.add(nodeType);
              }

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
            catch(InterruptedException e)
            {
              log.info("Interrupted while waiting for task to start [%s]", processHolder);
              Thread.currentThread().interrupt();
              throw Throwables.propagate(e);
            }
            catch(ExecutionException e){
              final Throwable eCause = e.getCause();
              if(eCause instanceof InterruptedException){
                log.info(e, "Attach interrupted for [%s]", processHolder);
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
        startingFuture.cancel(true);
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
  @LifecycleStart
  public synchronized void start()
  {
    if (stopped.get()) {
      throw new ISE("Already stopped!");
    }

    populateMissingTasksFromDir();

    if (!started.compareAndSet(false, true)) {
      throw new ISE("Already started");
    }
  }

  @LifecycleStop
  public synchronized void stop()
  {
    if (!started.get()) {
      throw new ISE("Not started");
    }
    if (!stopped.compareAndSet(false, true)) {
      throw new ISE("Already stopped");
    }
    exec.shutdown();
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
    if (processHolder.taskPortFile.exists()) {
      try {
        log.info("Killing task [%s] attempt [%s]", taskid, processHolder.attemptId);
        final URL url = new URL("http", "localhost", processHolder.port, "/shutdown");
        httpClient.go(
            new Request(HttpMethod.DELETE, url),
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
                log.info("Received chunk... why?");
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
        ).get();
        try {
          processHolder.awaitShutdown(config.getSoftShutdownTimelimit());
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
      catch (MalformedURLException | ExecutionException e) {
        throw Throwables.propagate(e);
      }
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
    final String portString = String.format("%d", processHolder.port);
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
    try {
      Process killingProcess = new ProcessBuilder(ImmutableList.of("kill", "-15", vmd.id()))
          .redirectOutput(ProcessBuilder.Redirect.PIPE)
          .redirectError(ProcessBuilder.Redirect.PIPE)
          .start();
      int retval = killingProcess.waitFor();
      if (retval == 0) {
        processHolder.awaitShutdown(config.getSoftShutdownTimelimit());
        return true;
      }
      try (InputStream inputStream = killingProcess.getInputStream()) {
        Scanner scanner = new Scanner(inputStream).useDelimiter("\\A");
        log.error(
            "Term of pid [%s] did not succeed with code [%d]: [%s]",
            vmd.id(),
            retval,
            scanner.hasNext() ? scanner.next() : "null"
        );
      }
      killingProcess = new ProcessBuilder(ImmutableList.of("kill", "-9", vmd.id()))
          .redirectOutput(ProcessBuilder.Redirect.PIPE)
          .redirectError(ProcessBuilder.Redirect.PIPE)
          .start();
      retval = killingProcess.waitFor();
      if (retval == 0) {
        processHolder.awaitShutdown(config.getSoftShutdownTimelimit());
        return true;
      }
      try (InputStream inputStream = killingProcess.getInputStream()) {
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
  public Collection<ZkWorker> getWorkers()
  {
    return ImmutableList.of();
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

  private void populateMissingTasksFromDir()
  {
    if (started.get() || stopped.get()) {
      // This might be safe to do, but this method assumes this is not the case
      throw new ISE("Cannot populate tasks from dirs once ForkingTaskRunner has been started");
    }
    //------------------------------------------------------------------ Find attempt directories
    final File baseDir = taskConfig.getBaseTaskDir();
    final File[] taskDirFileArray = baseDir.listFiles();
    final Collection<File> taskDirFileList = // Only directories
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
      return;
    }

    // For the task directories, look for attempt directories
    for (File potentialTaskDir : taskDirFileList) {
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
        log.info("Directory [%s] has no viable task attempts, attempting to cleanup if empty", potentialTaskDir);
        if (!potentialTaskDir.delete()) {
          log.warn("Could not clean up [%s]", potentialTaskDir);
        }
        continue;
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
        }
        catch (NumberFormatException e) {
          log.debug(e, "Skipping unparsable directory [%s]", taskAttemptDir);
        }
        if (latestAttemptDir == null) {
          latestAttemptDir = taskAttemptDir;
        } else {
          if (latestAttemptDir.lastModified() < taskAttemptDir.lastModified()) {
            latestAttemptDir = taskAttemptDir;
          }
        }
      }
      if (latestAttemptDir == null) {
        log.wtf("I had directories in [%s] but now I don't... memory corruption?", potentialTaskDir);
        continue;
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
        continue;
      }

      final File statusFile = new File(latestAttemptDir, STATUS_FILE_NAME);

      if (!statusFile.exists()) {
        // Shouldn't be missing unless there's corruption somehow.
        log.makeAlert("Status file [%s] is missing ", statusFile).emit();
        continue;
      }

      final File portFile = new File(latestAttemptDir, PORT_FILE_NAME);
      Integer port = null;
      if (portFile.exists()) {
        // Ooookkkkk, so here's where it gets interesting. The PORT_FILE_NAME is the intended indicator for the
        // JVM instance itself. Only the PEON is in charge of writing or deleting this file.
        // As such, there are a number of error cases about concurrent modifications that are taken into account here.
        // For example, a task is forked but then the forking task runner dies or is stopped. What do you do if the
        // Forking Task Runner and peon are starting up at the same time?
        try (FileChannel portFileChannel = FileChannel.open(
            portFile.toPath(),
            StandardOpenOption.READ,
            StandardOpenOption.WRITE // Required for lock
        )) {
          final ByteBuffer buffer;
          final FileLock fileLock = portFileChannel.lock(); // To make sure the peon is done writing before we try to read
          try {
            if (portFileChannel.size() > Integer.MAX_VALUE) {
              // Probably should never happen
              log.makeAlert(
                  "port file [%s] for task [%s] is HUGE %d bytes",
                  portFile,
                  task.getId(),
                  portFileChannel.size()
              ).emit();
              continue;
            }
            buffer = ByteBuffer.allocate((int) portFileChannel.size());
            portFileChannel.read(buffer);
            buffer.rewind();
          }
          finally {
            fileLock.release();
          }
          final String portString = Charsets.UTF_8.newDecoder().decode(buffer).toString();
          port = Integer.parseInt(portString);
        }
        catch (FileNotFoundException e) {
          log.info(e, "Task [%s] attempt [%s] exited during check", task.getId(), latestAttemptDir.getName());
          port = null;
        }
        catch (IOException | NumberFormatException e) {
          if (portFile.exists()) {
            // Something went wrong during write of value from peon's side
            log.makeAlert(e, "Port file [%s] for task [%s] is corrupt", portFile, task.getId()).emit();
            continue;
          }
          // Exited during read
          log.info(e, "Task [%s] attempt [%s] exited during read", task.getId(), latestAttemptDir.getName());
          port = null;
        }
      }
      if (port == null) {
        // At this point there should be one of two scenarios:
        // A) The peon has exited between ForkingTaskRunner instances
        // B) The peon is still starting up and hasn't written the port file yet
        log.debug("Found no port file for task [%d]. Uploading log and cleaning", task.getId());
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
          log.makeAlert(e, "Could upload data for task [%s] which finished between runs", task.getId()).emit();
        }
        continue;
      } else {
        portFinder.markPortUsed(port);
      }
      final ProcessHolder processHolder = new ProcessHolder(task.getId(), latestAttemptDir.getName(), port);
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

  private File getTaskDir(String taskId)
  {
    return new File(taskConfig.getBaseTaskDir(), taskId);
  }

  private File getNewTaskAttemptDir(String taskId)
  {
    final File taskDir = getTaskDir(taskId);
    if (!taskDir.exists()) {
      taskDir.mkdirs();
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
  }

  private class ProcessHolder
  {
    private final String taskId;
    private final String attemptId;
    private final int port;
    private final File taskPortFile;

    private ProcessHolder(String taskId, String attemptId, int port)
    {
      this.taskId = taskId;
      this.attemptId = attemptId;
      this.port = port;
      taskPortFile = new File(getTaskAttemptDir(taskId, attemptId), PORT_FILE_NAME);
    }

    public void awaitShutdown(long timeoutMS) throws InterruptedException, TimeoutException
    {
      final long startTime = System.currentTimeMillis();
      final Path taskPath = taskPortFile.toPath();
      try (WatchService watchService = taskPath.getFileSystem().newWatchService()) {
        taskPath.getParent().register(watchService, StandardWatchEventKinds.ENTRY_DELETE);
        while (taskPortFile.exists()) {
          final long delta = System.currentTimeMillis() - startTime;
          if (timeoutMS <= delta) {
            throw new TimeoutException("Waiting for the right delete event");
          }
          if (watchService.poll(timeoutMS - delta, TimeUnit.MILLISECONDS) == null) {
            throw new TimeoutException("Waiting for delete event to register");
          }
        }
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
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

  public boolean isStarted()
  {
    return started.get();
  }
}
