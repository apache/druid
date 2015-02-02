/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 * Runs tasks in separate processes using the "internal peon" verb.
 */
public class ForkingTaskRunner implements TaskRunner, TaskLogStreamer
{
  private static final EmittingLogger log = new EmittingLogger(ForkingTaskRunner.class);
  private static final String CHILD_PROPERTY_PREFIX = "druid.indexer.fork.property.";
  private static final Splitter whiteSpaceSplitter = Splitter.on(CharMatcher.WHITESPACE).omitEmptyStrings();

  private final ForkingTaskRunnerConfig config;
  private final TaskConfig taskConfig;
  private final Properties props;
  private final TaskLogPusher taskLogPusher;
  private final DruidNode node;
  private final ListeningExecutorService exec;
  private final ObjectMapper jsonMapper;
  private final PortFinder portFinder;

  private final Map<String, ForkingTaskRunnerWorkItem> tasks = Maps.newHashMap();

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
    this.config = config;
    this.taskConfig = taskConfig;
    this.props = props;
    this.taskLogPusher = taskLogPusher;
    this.jsonMapper = jsonMapper;
    this.node = node;
    this.portFinder = new PortFinder(config.getStartPort());

    this.exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(workerConfig.getCapacity()));
  }

  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    synchronized (tasks) {
      if (!tasks.containsKey(task.getId())) {
        tasks.put(
            task.getId(),
            new ForkingTaskRunnerWorkItem(
                task.getId(),
                exec.submit(
                    new Callable<TaskStatus>()
                    {
                      @Override
                      public TaskStatus call()
                      {
                        final String attemptUUID = UUID.randomUUID().toString();
                        final File taskDir = new File(taskConfig.getBaseTaskDir(), task.getId());
                        final File attemptDir = new File(taskDir, attemptUUID);

                        final ProcessHolder processHolder;
                        final int childPort = portFinder.findUnusedPort();
                        try {
                          final Closer closer = Closer.create();
                          try {
                            if (!attemptDir.mkdirs()) {
                              throw new IOException(String.format("Could not create directories: %s", attemptDir));
                            }

                            final File taskFile = new File(attemptDir, "task.json");
                            final File statusFile = new File(attemptDir, "status.json");
                            final File logFile = new File(attemptDir, "log");

                            // time to adjust process holders
                            synchronized (tasks) {
                              final ForkingTaskRunnerWorkItem taskWorkItem = tasks.get(task.getId());

                              if (taskWorkItem.shutdown) {
                                throw new IllegalStateException("Task has been shut down!");
                              }

                              if (taskWorkItem == null) {
                                log.makeAlert("WTF?! TaskInfo disappeared!").addData("task", task.getId()).emit();
                                throw new ISE("TaskInfo disappeared for task[%s]!", task.getId());
                              }

                              if (taskWorkItem.processHolder != null) {
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

                              command.add("io.druid.cli.Main");
                              command.add("internal");
                              command.add("peon");
                              command.add(taskFile.toString());
                              command.add(statusFile.toString());
                              String nodeType = task.getNodeType();
                              if (nodeType != null) {
                                command.add("--nodeType");
                                command.add(nodeType);
                              }

                              jsonMapper.writeValue(taskFile, task);

                              log.info("Running command: %s", Joiner.on(" ").join(command));
                              taskWorkItem.processHolder = new ProcessHolder(
                                  new ProcessBuilder(ImmutableList.copyOf(command)).redirectErrorStream(true).start(),
                                  logFile,
                                  childPort
                              );

                              processHolder = taskWorkItem.processHolder;
                              processHolder.registerWithCloser(closer);
                            }

                            log.info("Logging task %s output to: %s", task.getId(), logFile);
                            boolean runFailed = true;

                            try (final OutputStream toLogfile = Files.asByteSink(logFile).openBufferedStream()) {
                              ByteStreams.copy(processHolder.process.getInputStream(), toLogfile);
                              final int statusCode = processHolder.process.waitFor();
                              log.info("Process exited with status[%d] for task: %s", statusCode, task.getId());
                              if (statusCode == 0) {
                                runFailed = false;
                              }
                            }
                            finally {
                              // Upload task logs
                              taskLogPusher.pushTaskLog(task.getId(), logFile);
                            }

                            if (!runFailed) {
                              // Process exited successfully
                              return jsonMapper.readValue(statusFile, TaskStatus.class);
                            } else {
                              // Process exited unsuccessfully
                              return TaskStatus.failure(task.getId());
                            }
                          } catch (Throwable t) {
                            throw closer.rethrow(t);
                          } finally {
                            closer.close();
                          }
                        }
                        catch (Throwable t) {
                          log.info(t, "Exception caught during execution");
                          throw Throwables.propagate(t);
                        }
                        finally {
                          try {
                            synchronized (tasks) {
                              final ForkingTaskRunnerWorkItem taskWorkItem = tasks.remove(task.getId());
                              if (taskWorkItem != null && taskWorkItem.processHolder != null) {
                                taskWorkItem.processHolder.process.destroy();
                              }
                            }
                            portFinder.markPortUnused(childPort);
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

      return tasks.get(task.getId()).getResult();
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (tasks) {
      exec.shutdown();

      for (ForkingTaskRunnerWorkItem taskWorkItem : tasks.values()) {
        if (taskWorkItem.processHolder != null) {
          log.info("Destroying process: %s", taskWorkItem.processHolder.process);
          taskWorkItem.processHolder.process.destroy();
        }
      }
    }
  }

  @Override
  public void shutdown(final String taskid)
  {
    final ForkingTaskRunnerWorkItem taskInfo;

    synchronized (tasks) {
      taskInfo = tasks.get(taskid);

      if (taskInfo == null) {
        log.info("Ignoring request to cancel unknown task: %s", taskid);
        return;
      }

      taskInfo.shutdown = true;
    }

    if (taskInfo.processHolder != null) {
      // Will trigger normal failure mechanisms due to process exit
      log.info("Killing process for task: %s", taskid);
      taskInfo.processHolder.process.destroy();
    }
  }

  @Override
  public Collection<TaskRunnerWorkItem> getRunningTasks()
  {
    synchronized (tasks) {
      final List<TaskRunnerWorkItem> ret = Lists.newArrayList();
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
      final List<TaskRunnerWorkItem> ret = Lists.newArrayList();
      for (final ForkingTaskRunnerWorkItem taskWorkItem : tasks.values()) {
        if (taskWorkItem.processHolder == null) {
          ret.add(taskWorkItem);
        }
      }
      return ret;
    }
  }

  @Override
  public Collection<TaskRunnerWorkItem> getKnownTasks()
  {
    synchronized (tasks) {
      return Lists.<TaskRunnerWorkItem>newArrayList(tasks.values());
    }
  }

  @Override
  public Collection<ZkWorker> getWorkers()
  {
    return ImmutableList.of();
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

    return Optional.<ByteSource>of(
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

  private static class ForkingTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    private volatile boolean shutdown = false;
    private volatile ProcessHolder processHolder = null;

    private ForkingTaskRunnerWorkItem(
        String taskId,
        ListenableFuture<TaskStatus> statusFuture
    )
    {
      super(taskId, statusFuture);
    }
  }

  private static class ProcessHolder
  {
    private final Process process;
    private final File logFile;
    private final int port;

    private ProcessHolder(Process process, File logFile, int port)
    {
      this.process = process;
      this.logFile = logFile;
      this.port = port;
    }

    private void registerWithCloser(Closer closer)
    {
      closer.register(process.getInputStream());
      closer.register(process.getOutputStream());
    }
  }
}
