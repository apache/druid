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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.tasklogs.LogUtils;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.MonitorsConfig;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Runs tasks in separate processes using the "internal peon" verb.
 */
public class ForkingTaskRunner
    extends BaseRestorableTaskRunner<ForkingTaskRunner.ForkingTaskRunnerWorkItem>
    implements TaskLogStreamer
{
  private static final EmittingLogger LOGGER = new EmittingLogger(ForkingTaskRunner.class);
  private static final String CHILD_PROPERTY_PREFIX = "druid.indexer.fork.property.";
  private final ForkingTaskRunnerConfig config;
  private final Properties props;
  private final TaskLogPusher taskLogPusher;
  private final DruidNode node;
  private final ListeningExecutorService exec;
  private final PortFinder portFinder;

  private volatile boolean stopping = false;

  @Inject
  public ForkingTaskRunner(
      ForkingTaskRunnerConfig config,
      TaskConfig taskConfig,
      WorkerConfig workerConfig,
      Properties props,
      TaskLogPusher taskLogPusher,
      ObjectMapper jsonMapper,
      @Self DruidNode node
  )
  {
    super(jsonMapper, taskConfig);
    this.config = config;
    this.props = props;
    this.taskLogPusher = taskLogPusher;
    this.node = node;
    this.portFinder = new PortFinder(config.getStartPort(), config.getEndPort(), config.getPorts());
    this.exec = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(workerConfig.getCapacity(), "forking-task-runner-%d")
    );
  }

  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    synchronized (tasks) {
      tasks.computeIfAbsent(
          task.getId(), k ->
          new ForkingTaskRunnerWorkItem(
            task,
            exec.submit(
              new Callable<TaskStatus>() {
                @Override
                public TaskStatus call()
                {
                  final String attemptUUID = UUID.randomUUID().toString();
                  final File taskDir = taskConfig.getTaskDir(task.getId());
                  final File attemptDir = new File(taskDir, attemptUUID);

                  final ProcessHolder processHolder;
                  final String childHost = node.getHost();
                  int childPort = -1;
                  int tlsChildPort = -1;

                  if (node.isEnablePlaintextPort()) {
                    childPort = portFinder.findUnusedPort();
                  }

                  if (node.isEnableTlsPort()) {
                    tlsChildPort = portFinder.findUnusedPort();
                  }

                  final TaskLocation taskLocation = TaskLocation.create(childHost, childPort, tlsChildPort);

                  try {
                    final Closer closer = Closer.create();
                    try {
                      if (!attemptDir.mkdirs()) {
                        throw new IOE("Could not create directories: %s", attemptDir);
                      }

                      final File taskFile = new File(taskDir, "task.json");
                      final File statusFile = new File(attemptDir, "status.json");
                      final File logFile = new File(taskDir, "log");
                      final File reportsFile = new File(attemptDir, "report.json");

                      // time to adjust process holders
                      synchronized (tasks) {
                        final ForkingTaskRunnerWorkItem taskWorkItem = tasks.get(task.getId());

                        if (taskWorkItem == null) {
                          LOGGER.makeAlert("WTF?! TaskInfo disappeared!").addData("task", task.getId()).emit();
                          throw new ISE("TaskInfo disappeared for task[%s]!", task.getId());
                        }

                        if (taskWorkItem.shutdown) {
                          throw new IllegalStateException("Task has been shut down!");
                        }

                        if (taskWorkItem.processHolder != null) {
                          LOGGER.makeAlert("WTF?! TaskInfo already has a processHolder")
                                .addData("task", task.getId())
                                .emit();
                          throw new ISE("TaskInfo already has processHolder for task[%s]!", task.getId());
                        }

                        final List<String> command = new ArrayList<>();
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

                        Iterables.addAll(command, new QuotableWhiteSpaceSplitter(config.getJavaOpts()));
                        Iterables.addAll(command, config.getJavaOptsArray());

                        // Override task specific javaOpts
                        Object taskJavaOpts = task.getContextValue(
                            ForkingTaskRunnerConfig.JAVA_OPTS_PROPERTY
                        );
                        if (taskJavaOpts != null) {
                          Iterables.addAll(
                              command,
                              new QuotableWhiteSpaceSplitter((String) taskJavaOpts)
                          );
                        }

                        for (String propName : props.stringPropertyNames()) {
                          for (String allowedPrefix : config.getAllowedPrefixes()) {
                            // See https://github.com/apache/incubator-druid/issues/1841
                            if (propName.startsWith(allowedPrefix)
                                && !ForkingTaskRunnerConfig.JAVA_OPTS_PROPERTY.equals(propName)
                                && !ForkingTaskRunnerConfig.JAVA_OPTS_ARRAY_PROPERTY.equals(propName)
                            ) {
                              command.add(
                                  StringUtils.format(
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
                                StringUtils.format(
                                "-D%s=%s",
                                propName.substring(CHILD_PROPERTY_PREFIX.length()),
                                props.getProperty(propName)
                              )
                            );
                          }
                        }

                        // Override task specific properties
                        final Map<String, Object> context = task.getContext();
                        if (context != null) {
                          for (String propName : context.keySet()) {
                            if (propName.startsWith(CHILD_PROPERTY_PREFIX)) {
                              command.add(
                                  StringUtils.format(
                                  "-D%s=%s",
                                  propName.substring(CHILD_PROPERTY_PREFIX.length()),
                                  task.getContextValue(propName)
                                )
                              );
                            }
                          }
                        }

                        // Add dataSource, taskId and taskType for metrics or logging
                        command.add(
                            StringUtils.format(
                            "-D%s%s=%s",
                            MonitorsConfig.METRIC_DIMENSION_PREFIX,
                            DruidMetrics.DATASOURCE,
                            task.getDataSource()
                          )
                        );
                        command.add(
                            StringUtils.format(
                            "-D%s%s=%s",
                            MonitorsConfig.METRIC_DIMENSION_PREFIX,
                            DruidMetrics.TASK_ID,
                            task.getId()
                          )
                        );
                        command.add(
                            StringUtils.format(
                            "-D%s%s=%s",
                            MonitorsConfig.METRIC_DIMENSION_PREFIX,
                            DruidMetrics.TASK_TYPE,
                            task.getType()
                          )
                        );

                        command.add(StringUtils.format("-Ddruid.host=%s", childHost));
                        command.add(StringUtils.format("-Ddruid.plaintextPort=%d", childPort));
                        command.add(StringUtils.format("-Ddruid.tlsPort=%d", tlsChildPort));

                        // Let tasks know where they are running on.
                        // This information is used in native parallel indexing with shuffle.
                        command.add(StringUtils.format("-Ddruid.task.executor.service=%s", node.getServiceName()));
                        command.add(StringUtils.format("-Ddruid.task.executor.host=%s", node.getHost()));
                        command.add(
                            StringUtils.format("-Ddruid.task.executor.plaintextPort=%d", node.getPlaintextPort())
                        );
                        command.add(
                            StringUtils.format(
                                "-Ddruid.task.executor.enablePlaintextPort=%s",
                                node.isEnablePlaintextPort()
                            )
                        );
                        command.add(StringUtils.format("-Ddruid.task.executor.tlsPort=%d", node.getTlsPort()));
                        command.add(
                            StringUtils.format("-Ddruid.task.executor.enableTlsPort=%s", node.isEnableTlsPort())
                        );

                        // These are not enabled per default to allow the user to either set or not set them
                        // Users are highly suggested to be set in druid.indexer.runner.javaOpts
                        // See org.apache.druid.concurrent.TaskThreadPriority#getThreadPriorityFromTaskPriority(int)
                        // for more information
                        // command.add("-XX:+UseThreadPriorities");
                        // command.add("-XX:ThreadPriorityPolicy=42");

                        command.add("org.apache.druid.cli.Main");
                        command.add("internal");
                        command.add("peon");
                        command.add(taskFile.toString());
                        command.add(statusFile.toString());
                        command.add(reportsFile.toString());
                        String nodeType = task.getNodeType();
                        if (nodeType != null) {
                          command.add("--nodeType");
                          command.add(nodeType);
                        }

                        if (!taskFile.exists()) {
                          jsonMapper.writeValue(taskFile, task);
                        }

                        LOGGER.info("Running command: %s", Joiner.on(" ").join(command));
                        taskWorkItem.processHolder = new ProcessHolder(
                          new ProcessBuilder(ImmutableList.copyOf(command)).redirectErrorStream(true).start(),
                          logFile,
                          taskLocation.getHost(),
                          taskLocation.getPort(),
                          taskLocation.getTlsPort()
                        );

                        processHolder = taskWorkItem.processHolder;
                        processHolder.registerWithCloser(closer);
                      }

                      TaskRunnerUtils.notifyLocationChanged(listeners, task.getId(), taskLocation);
                      TaskRunnerUtils.notifyStatusChanged(
                          listeners,
                          task.getId(),
                          TaskStatus.running(task.getId())
                      );

                      LOGGER.info("Logging task %s output to: %s", task.getId(), logFile);
                      boolean runFailed = true;

                      final ByteSink logSink = Files.asByteSink(logFile, FileWriteMode.APPEND);

                      // This will block for a while. So we append the thread information with more details
                      final String priorThreadName = Thread.currentThread().getName();
                      Thread.currentThread().setName(StringUtils.format("%s-[%s]", priorThreadName, task.getId()));

                      try (final OutputStream toLogfile = logSink.openStream()) {
                        ByteStreams.copy(processHolder.process.getInputStream(), toLogfile);
                        final int statusCode = processHolder.process.waitFor();
                        LOGGER.info("Process exited with status[%d] for task: %s", statusCode, task.getId());
                        if (statusCode == 0) {
                          runFailed = false;
                        }
                      }
                      finally {
                        Thread.currentThread().setName(priorThreadName);
                        // Upload task logs
                        taskLogPusher.pushTaskLog(task.getId(), logFile);
                        if (reportsFile.exists()) {
                          taskLogPusher.pushTaskReports(task.getId(), reportsFile);
                        }
                      }

                      TaskStatus status;
                      if (!runFailed) {
                        // Process exited successfully
                        status = jsonMapper.readValue(statusFile, TaskStatus.class);
                      } else {
                        // Process exited unsuccessfully
                        status = TaskStatus.failure(task.getId());
                      }

                      TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), status);
                      return status;
                    }
                    catch (Throwable t) {
                      throw closer.rethrow(t);
                    }
                    finally {
                      closer.close();
                    }
                  }
                  catch (Throwable t) {
                    LOGGER.info(t, "Exception caught during execution");
                    throw new RuntimeException(t);
                  }
                  finally {
                    try {
                      synchronized (tasks) {
                        final ForkingTaskRunnerWorkItem taskWorkItem = tasks.remove(task.getId());
                        if (taskWorkItem != null && taskWorkItem.processHolder != null) {
                          taskWorkItem.processHolder.process.destroy();
                        }
                        if (!stopping) {
                          saveRunningTasks();
                        }
                      }

                      if (node.isEnablePlaintextPort()) {
                        portFinder.markPortUnused(childPort);
                      }
                      if (node.isEnableTlsPort()) {
                        portFinder.markPortUnused(tlsChildPort);
                      }

                      try {
                        if (!stopping && taskDir.exists()) {
                          LOGGER.info("Removing task directory: %s", taskDir);
                          FileUtils.deleteDirectory(taskDir);
                        }
                      }
                      catch (Exception e) {
                        LOGGER.makeAlert(e, "Failed to delete task directory")
                              .addData("taskDir", taskDir.toString())
                              .addData("task", task.getId())
                              .emit();
                      }
                    }
                    catch (Exception e) {
                      LOGGER.error(e, "Suppressing exception caught while cleaning up task");
                    }
                  }
                }
              }
            )
          )
      );
      saveRunningTasks();
      return tasks.get(task.getId()).getResult();
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    stopping = true;
    exec.shutdown();

    synchronized (tasks) {
      for (ForkingTaskRunnerWorkItem taskWorkItem : tasks.values()) {
        shutdownTaskProcess(taskWorkItem);
      }
    }

    final DateTime start = DateTimes.nowUtc();
    final long timeout = new Interval(start, taskConfig.getGracefulShutdownTimeout()).toDurationMillis();

    // Things should be terminating now. Wait for it to happen so logs can be uploaded and all that good stuff.
    LOGGER.info("Waiting up to %,dms for shutdown.", timeout);
    if (timeout > 0) {
      try {
        final boolean terminated = exec.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        final long elapsed = System.currentTimeMillis() - start.getMillis();
        if (terminated) {
          LOGGER.info("Finished stopping in %,dms.", elapsed);
        } else {
          final Set<String> stillRunning;
          synchronized (tasks) {
            stillRunning = ImmutableSet.copyOf(tasks.keySet());
          }

          LOGGER.makeAlert("Failed to stop forked tasks")
                .addData("stillRunning", stillRunning)
                .addData("elapsed", elapsed)
                .emit();

          LOGGER.warn(
              "Executor failed to stop after %,dms, not waiting for it! Tasks still running: [%s]",
              elapsed,
              Joiner.on("; ").join(stillRunning)
          );
        }
      }
      catch (InterruptedException e) {
        LOGGER.warn(e, "Interrupted while waiting for executor to finish.");
        Thread.currentThread().interrupt();
      }
    } else {
      LOGGER.warn("Ran out of time, not waiting for executor to finish!");
    }
  }

  @Override
  public void shutdown(final String taskid, String reason)
  {
    LOGGER.info("Shutdown [%s] because: [%s]", taskid, reason);
    final ForkingTaskRunnerWorkItem taskInfo;

    synchronized (tasks) {
      taskInfo = tasks.get(taskid);

      if (taskInfo == null) {
        LOGGER.info("Ignoring request to cancel unknown task: %s", taskid);
        return;
      }

      taskInfo.shutdown = true;

      shutdownTaskProcess(taskInfo);
    }
  }

  @Override
  public Collection<TaskRunnerWorkItem> getRunningTasks()
  {
    synchronized (tasks) {
      final List<TaskRunnerWorkItem> ret = new ArrayList<>();
      for (final ForkingTaskRunnerWorkItem taskWorkItem : tasks.values()) {
        if (taskWorkItem.processHolder != null) {
          ret.add(taskWorkItem);
        }
      }
      return ret;
    }
  }

  @Override
  public Collection<TaskRunnerWorkItem> getPendingTasks()
  {
    synchronized (tasks) {
      final List<TaskRunnerWorkItem> ret = new ArrayList<>();
      for (final ForkingTaskRunnerWorkItem taskWorkItem : tasks.values()) {
        if (taskWorkItem.processHolder == null) {
          ret.add(taskWorkItem);
        }
      }
      return ret;
    }
  }

  @Nullable
  @Override
  public RunnerTaskState getRunnerTaskState(String taskId)
  {
    final ForkingTaskRunnerWorkItem workItem = tasks.get(taskId);
    if (workItem == null) {
      return null;
    } else {
      if (workItem.processHolder == null) {
        return RunnerTaskState.PENDING;
      } else if (workItem.processHolder.process.isAlive()) {
        return RunnerTaskState.RUNNING;
      } else {
        return RunnerTaskState.NONE;
      }
    }
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.absent();
  }

  @Override
  public void start()
  {
    // No state setup required
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskid, final long offset)
  {
    final ProcessHolder processHolder;

    synchronized (tasks) {
      final ForkingTaskRunnerWorkItem taskWorkItem = tasks.get(taskid);
      if (taskWorkItem != null && taskWorkItem.processHolder != null) {
        processHolder = taskWorkItem.processHolder;
      } else {
        return Optional.absent();
      }
    }

    return Optional.of(
        new ByteSource()
        {
          @Override
          public InputStream openStream() throws IOException
          {
            return LogUtils.streamFile(processHolder.logFile, offset);
          }
        }
    );
  }

  /**
   * Close task output stream (input stream of process) sending EOF telling process to terminate, destroying the process
   * if an exception is encountered.
   */
  private void shutdownTaskProcess(ForkingTaskRunnerWorkItem taskInfo)
  {
    if (taskInfo.processHolder != null) {
      // Will trigger normal failure mechanisms due to process exit
      LOGGER.info("Closing output stream to task[%s].", taskInfo.getTask().getId());
      try {
        taskInfo.processHolder.process.getOutputStream().close();
      }
      catch (Exception e) {
        LOGGER.warn(e, "Failed to close stdout to task[%s]. Destroying task.", taskInfo.getTask().getId());
        taskInfo.processHolder.process.destroy();
      }
    }
  }

  protected static class ForkingTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    private final Task task;

    private volatile boolean shutdown = false;
    private volatile ProcessHolder processHolder = null;

    private ForkingTaskRunnerWorkItem(
        Task task,
        ListenableFuture<TaskStatus> statusFuture
    )
    {
      super(task.getId(), statusFuture);
      this.task = task;
    }

    public Task getTask()
    {
      return task;
    }

    @Override
    public TaskLocation getLocation()
    {
      if (processHolder == null) {
        return TaskLocation.unknown();
      } else {
        return TaskLocation.create(processHolder.host, processHolder.port, processHolder.tlsPort);
      }
    }

    @Override
    public String getTaskType()
    {
      return task.getType();
    }

    @Override
    public String getDataSource()
    {
      return task.getDataSource();
    }
  }

  private static class ProcessHolder
  {
    private final Process process;
    private final File logFile;
    private final String host;
    private final int port;
    private final int tlsPort;

    private ProcessHolder(Process process, File logFile, String host, int port, int tlsPort)
    {
      this.process = process;
      this.logFile = logFile;
      this.host = host;
      this.port = port;
      this.tlsPort = tlsPort;
    }

    private void registerWithCloser(Closer closer)
    {
      closer.register(process.getInputStream());
      closer.register(process.getOutputStream());
    }
  }
}

/**
 * Make an iterable of space delimited strings... unless there are quotes, which it preserves
 */
class QuotableWhiteSpaceSplitter implements Iterable<String>
{
  private final String string;

  public QuotableWhiteSpaceSplitter(String string)
  {
    this.string = Preconditions.checkNotNull(string);
  }

  @Override
  public Iterator<String> iterator()
  {
    return Splitter.on(
        new CharMatcher()
        {
          private boolean inQuotes = false;

          @Override
          public boolean matches(char c)
          {
            if ('"' == c) {
              inQuotes = !inQuotes;
            }
            if (inQuotes) {
              return false;
            }
            return CharMatcher.BREAKING_WHITESPACE.matches(c);
          }
        }
    ).omitEmptyStrings().split(string).iterator();
  }
}
