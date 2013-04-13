/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.merger.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.common.tasklogs.TaskLogProvider;
import com.metamx.druid.merger.common.tasklogs.TaskLogPusher;
import com.metamx.druid.merger.coordinator.config.ForkingTaskRunnerConfig;
import com.metamx.druid.merger.worker.executor.ExecutorMain;
import com.metamx.emitter.EmittingLogger;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs tasks in separate processes using {@link ExecutorMain}.
 */
public class ForkingTaskRunner implements TaskRunner, TaskLogProvider
{
  private static final EmittingLogger log = new EmittingLogger(ForkingTaskRunner.class);
  private static final String CHILD_PROPERTY_PREFIX = "druid.indexer.fork.property.";

  private final ForkingTaskRunnerConfig config;
  private final Properties props;
  private final TaskLogPusher taskLogPusher;
  private final ListeningExecutorService exec;
  private final ObjectMapper jsonMapper;

  private final Map<String, TaskInfo> tasks = Maps.newHashMap();

  public ForkingTaskRunner(
      ForkingTaskRunnerConfig config,
      Properties props,
      TaskLogPusher taskLogPusher,
      ExecutorService exec,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.props = props;
    this.taskLogPusher = taskLogPusher;
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.jsonMapper = jsonMapper;
  }

  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    synchronized (tasks) {
      if (!tasks.containsKey(task.getId())) {
        tasks.put(
            task.getId(),
            new TaskInfo(
                exec.submit(
                    new Callable<TaskStatus>()
                    {
                      @Override
                      public TaskStatus call()
                      {
                        final String attemptUUID = UUID.randomUUID().toString();
                        final File taskDir = new File(config.getBaseTaskDir(), task.getId());
                        final File attemptDir = new File(taskDir, attemptUUID);

                        final ProcessHolder processHolder;

                        try {
                          if (!attemptDir.mkdirs()) {
                            throw new IOException(String.format("Could not create directories: %s", attemptDir));
                          }

                          final File taskFile = new File(attemptDir, "task.json");
                          final File statusFile = new File(attemptDir, "status.json");
                          final File logFile = new File(attemptDir, "log");

                          // time to adjust process holders
                          synchronized (tasks) {
                            if (Thread.interrupted()) {
                              throw new InterruptedException();
                            }

                            final TaskInfo taskInfo = tasks.get(task.getId());
                            if (taskInfo == null) {
                              throw new ISE("WTF?! TaskInfo disappeared for task: %s", task.getId());
                            }

                            if (taskInfo.processHolder != null) {
                              throw new ISE("WTF?! TaskInfo already has a process holder for task: %s", task.getId());
                            }

                            final List<String> command = Lists.newArrayList();
                            final int childPort = findUnusedPort();
                            final String childHost = String.format(config.getHostPattern(), childPort);

                            command.add(config.getJavaCommand());
                            command.add("-cp");
                            command.add(config.getJavaClasspath());

                            Iterables.addAll(
                                command,
                                Splitter.on(CharMatcher.WHITESPACE)
                                        .omitEmptyStrings()
                                        .split(config.getJavaOptions())
                            );

                            for (String propName : props.stringPropertyNames()) {
                              if (propName.startsWith(CHILD_PROPERTY_PREFIX)) {
                                command.add(
                                    String.format(
                                        "-D%s=%s",
                                        propName.substring(CHILD_PROPERTY_PREFIX.length()),
                                        System.getProperty(propName)
                                    )
                                );
                              }
                            }

                            command.add(String.format("-Ddruid.host=%s", childHost));
                            command.add(String.format("-Ddruid.port=%d", childPort));

                            command.add(config.getMainClass());
                            command.add(taskFile.toString());
                            command.add(statusFile.toString());

                            jsonMapper.writeValue(taskFile, task);

                            log.info("Running command: %s", Joiner.on(" ").join(command));
                            taskInfo.processHolder = new ProcessHolder(
                                new ProcessBuilder(ImmutableList.copyOf(command)).redirectErrorStream(true).start(),
                                logFile,
                                childPort
                            );

                            processHolder = taskInfo.processHolder;
                          }

                          log.info("Logging task %s output to: %s", task.getId(), logFile);

                          final OutputStream toProc = processHolder.process.getOutputStream();
                          final InputStream fromProc = processHolder.process.getInputStream();
                          final OutputStream toLogfile = Files.newOutputStreamSupplier(logFile).getOutput();

                          boolean runFailed = false;

                          try {
                            ByteStreams.copy(fromProc, toLogfile);
                            final int statusCode = processHolder.process.waitFor();
                            log.info("Process exited with status[%d] for task: %s", statusCode, task.getId());

                            if (statusCode != 0) {
                              runFailed = true;
                            }
                          }
                          catch (Exception e) {
                            log.warn(e, "Failed to read from process for task: %s", task.getId());
                            runFailed = true;
                          }
                          finally {
                            Closeables.closeQuietly(fromProc);
                            Closeables.closeQuietly(toLogfile);
                            Closeables.closeQuietly(toProc);
                          }

                          // Upload task logs

                          // XXX: Consider uploading periodically for very long-lived tasks to prevent
                          // XXX: bottlenecks at the end or the possibility of losing a lot of logs all
                          // XXX: at once.

                          taskLogPusher.pushTaskLog(task.getId(), logFile);

                          if (!runFailed) {
                            // Process exited successfully
                            return jsonMapper.readValue(statusFile, TaskStatus.class);
                          } else {
                            // Process exited unsuccessfully
                            return TaskStatus.failure(task.getId());
                          }
                        }
                        catch (InterruptedException e) {
                          log.info(e, "Interrupted during execution");
                          return TaskStatus.failure(task.getId());
                        }
                        catch (IOException e) {
                          throw Throwables.propagate(e);
                        }
                        finally {
                          try {
                            synchronized (tasks) {
                              final TaskInfo taskInfo = tasks.remove(task.getId());
                              if (taskInfo != null && taskInfo.processHolder != null) {
                                taskInfo.processHolder.process.destroy();
                              }
                            }

                            log.info("Removing temporary directory: %s", attemptDir);
                            FileUtils.deleteDirectory(attemptDir);
                          }
                          catch (Exception e) {
                            log.error(e, "Suppressing exception caught while cleaning up task");
                          }
                        }
                      }
                    }
                )
            )
        );
      }

      return tasks.get(task.getId()).statusFuture;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (tasks) {
      exec.shutdown();

      for (TaskInfo taskInfo : tasks.values()) {
        if (taskInfo.processHolder != null) {
          log.info("Destroying process: %s", taskInfo.processHolder.process);
          taskInfo.processHolder.process.destroy();
        }
      }
    }
  }

  @Override
  public void shutdown(final String taskid)
  {
    final TaskInfo taskInfo;

    synchronized (tasks) {
      taskInfo = tasks.get(taskid);

      if (taskInfo == null) {
        log.info("Ignoring request to cancel unknown task: %s", taskid);
        return;
      }
    }

    taskInfo.statusFuture.cancel(true);

    if (taskInfo.processHolder != null) {
      final int shutdowns = taskInfo.processHolder.shutdowns.getAndIncrement();
      if (shutdowns == 0) {
        log.info("Attempting to gracefully shutdown task: %s", taskid);
        try {
          // This is gross, but it may still be nicer than talking to the forked JVM via HTTP.
          final OutputStream out = taskInfo.processHolder.process.getOutputStream();
          out.write(
              jsonMapper.writeValueAsBytes(
                  ImmutableMap.of(
                      "shutdown",
                      "now"
                  )
              )
          );
          out.write('\n');
          out.flush();
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      } else {
        // Will trigger normal failure mechanisms due to process exit
        log.info("Killing process for task: %s", taskid);
        taskInfo.processHolder.process.destroy();
      }
    }
  }

  @Override
  public Collection<TaskRunnerWorkItem> getRunningTasks()
  {
    return ImmutableList.of();
  }

  @Override
  public Collection<TaskRunnerWorkItem> getPendingTasks()
  {
    return ImmutableList.of();
  }

  @Override
  public Collection<ZkWorker> getWorkers()
  {
    return ImmutableList.of();
  }

  @Override
  public Optional<InputSupplier<InputStream>> streamTaskLog(final String taskid, final long offset)
  {
    final ProcessHolder processHolder;

    synchronized (tasks) {
      final TaskInfo taskInfo = tasks.get(taskid);
      if (taskInfo != null && taskInfo.processHolder != null) {
        processHolder = taskInfo.processHolder;
      } else {
        return Optional.absent();
      }
    }

    return Optional.<InputSupplier<InputStream>>of(
        new InputSupplier<InputStream>()
        {
          @Override
          public InputStream getInput() throws IOException
          {
            final RandomAccessFile raf = new RandomAccessFile(processHolder.logFile, "r");
            final long rafLength = raf.length();
            if (offset > 0) {
              raf.seek(offset);
            } else if (offset < 0 && offset < rafLength) {
              raf.seek(rafLength + offset);
            }
            return Channels.newInputStream(raf.getChannel());
          }
        }
    );
  }

  private int findUnusedPort()
  {
    synchronized (tasks) {
      int port = config.getStartPort();
      int maxPortSoFar = -1;

      for (TaskInfo taskInfo : tasks.values()) {
        if (taskInfo.processHolder != null) {
          if (taskInfo.processHolder.port > maxPortSoFar) {
            maxPortSoFar = taskInfo.processHolder.port;
          }

          if (taskInfo.processHolder.port == port) {
            port = maxPortSoFar + 1;
          }
        }
      }

      return port;
    }
  }

  private static class TaskInfo
  {
    private final ListenableFuture<TaskStatus> statusFuture;
    private volatile ProcessHolder processHolder = null;

    private TaskInfo(ListenableFuture<TaskStatus> statusFuture)
    {
      this.statusFuture = statusFuture;
    }
  }

  private static class ProcessHolder
  {
    private final Process process;
    private final File logFile;
    private final int port;
    private final AtomicInteger shutdowns = new AtomicInteger(0);

    private ProcessHolder(Process process, File logFile, int port)
    {
      this.process = process;
      this.logFile = logFile;
      this.port = port;
    }
  }
}
