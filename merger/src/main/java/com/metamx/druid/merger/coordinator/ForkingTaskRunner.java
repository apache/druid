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
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
import com.metamx.druid.merger.common.tasklogs.TaskLogProvider;
import com.metamx.druid.merger.common.task.Task;
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

  private final Object processLock = new Object();
  private final ForkingTaskRunnerConfig config;
  private final TaskLogPusher taskLogPusher;
  private final ListeningExecutorService exec;
  private final ObjectMapper jsonMapper;
  private final List<ProcessHolder> processes = Lists.newArrayList();

  public ForkingTaskRunner(
      ForkingTaskRunnerConfig config,
      TaskLogPusher taskLogPusher,
      ExecutorService exec,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.taskLogPusher = taskLogPusher;
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.jsonMapper = jsonMapper;
  }

  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    return exec.submit(
        new Callable<TaskStatus>()
        {
          @Override
          public TaskStatus call()
          {
            final String attemptUUID = UUID.randomUUID().toString();
            final File taskDir = new File(config.getBaseTaskDir(), task.getId());
            final File attemptDir = new File(taskDir, attemptUUID);

            ProcessHolder processHolder = null;

            try {
              if (!attemptDir.mkdirs()) {
                throw new IOException(String.format("Could not create directories: %s", attemptDir));
              }

              final File taskFile = new File(attemptDir, "task.json");
              final File statusFile = new File(attemptDir, "status.json");
              final File logFile = new File(attemptDir, "log");

              // locked so we can safely assign port = findUnusedPort
              synchronized (processLock) {
                if (getProcessHolder(task.getId()).isPresent()) {
                  throw new ISE("Task already running: %s", task.getId());
                }

                final List<String> command = Lists.newArrayList();
                final int childPort = findUnusedPort();
                final String childHost = String.format(config.getHostPattern(), childPort);

                Iterables.addAll(
                    command,
                    ImmutableList.of(
                        config.getJavaCommand(),
                        "-cp",
                        config.getJavaClasspath()
                    )
                );

                Iterables.addAll(
                    command,
                    Splitter.on(CharMatcher.WHITESPACE)
                            .omitEmptyStrings()
                            .split(config.getJavaOptions())
                );

                for (String propName : System.getProperties().stringPropertyNames()) {
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
                processHolder = new ProcessHolder(
                    task,
                    new ProcessBuilder(ImmutableList.copyOf(command)).redirectErrorStream(true).start(),
                    logFile,
                    childPort
                );

                processes.add(processHolder);
              }

              log.info("Logging task %s output to: %s", task.getId(), logFile);

              final OutputStream toLogfile = Files.newOutputStreamSupplier(logFile).getOutput();
              final InputStream fromProc = processHolder.process.getInputStream();

              boolean copyFailed = false;

              try {
                ByteStreams.copy(fromProc, toLogfile);
              } catch (Exception e) {
                log.warn(e, "Failed to read from process for task: %s", task.getId());
                copyFailed = true;
              } finally {
                Closeables.closeQuietly(fromProc);
                Closeables.closeQuietly(toLogfile);
              }

              final int statusCode = processHolder.process.waitFor();

              log.info("Process exited with status[%d] for task: %s", statusCode, task.getId());

              // Upload task logs
              // TODO: For very long-lived tasks, upload periodically? Truncated?
              // TODO: Store task logs for each attempt separately?
              taskLogPusher.pushTaskLog(task.getId(), logFile);

              if (!copyFailed && statusCode == 0) {
                // Process exited successfully
                return jsonMapper.readValue(statusFile, TaskStatus.class);
              } else {
                // Process exited unsuccessfully
                return TaskStatus.failure(task.getId());
              }
            }
            catch (InterruptedException e) {
              log.info(e, "Interrupted while waiting for process!");
              return TaskStatus.failure(task.getId());
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
            finally {
              try {
                if (processHolder != null) {
                  synchronized (processLock) {
                    processes.remove(processHolder);
                  }
                }

                log.info("Removing temporary directory: %s", attemptDir);
                FileUtils.deleteDirectory(attemptDir);
              }
              catch (Exception e) {
                log.error(e, "Failed to delete temporary directory");
              }
            }
          }
        }
    );
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (processLock) {
      exec.shutdown();

      for (ProcessHolder processHolder : processes) {
        log.info("Destroying process: %s", processHolder.process);
        processHolder.process.destroy();
      }
    }
  }

  @Override
  public void shutdown(final String taskid)
  {
    final Optional<ProcessHolder> processHolder = getProcessHolder(taskid);
    if (processHolder.isPresent()) {
      final int shutdowns = processHolder.get().shutdowns.getAndIncrement();
      if (shutdowns == 0) {
        log.info("Attempting to gracefully shutdown task: %s", taskid);
        try {
          // This is gross, but it may still be nicer than talking to the forked JVM via HTTP.
          final OutputStream out = processHolder.get().process.getOutputStream();
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
        processHolder.get().process.destroy();
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
    final Optional<ProcessHolder> processHolder = getProcessHolder(taskid);

    if (processHolder.isPresent()) {
      return Optional.<InputSupplier<InputStream>>of(
          new InputSupplier<InputStream>()
          {
            @Override
            public InputStream getInput() throws IOException
            {
              final RandomAccessFile raf = new RandomAccessFile(processHolder.get().logFile, "r");
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
    } else {
      return Optional.absent();
    }
  }

  private int findUnusedPort()
  {
    synchronized (processLock) {
      int port = config.getStartPort();
      int maxPortSoFar = -1;

      for (ProcessHolder processHolder : processes) {
        if (processHolder.port > maxPortSoFar) {
          maxPortSoFar = processHolder.port;
        }

        if (processHolder.port == port) {
          port = maxPortSoFar + 1;
        }
      }

      return port;
    }
  }

  private Optional<ProcessHolder> getProcessHolder(final String taskid)
  {
    synchronized (processLock) {
      return Iterables.tryFind(
          processes, new Predicate<ProcessHolder>()
      {
        @Override
        public boolean apply(ProcessHolder processHolder)
        {
          return processHolder.task.getId().equals(taskid);
        }
      }
      );
    }
  }

  private static class ProcessHolder
  {
    private final Task task;
    private final Process process;
    private final File logFile;
    private final int port;

    private AtomicInteger shutdowns = new AtomicInteger(0);

    private ProcessHolder(Task task, Process process, File logFile, int port)
    {
      this.task = task;
      this.process = process;
      this.logFile = logFile;
      this.port = port;
    }
  }
}
