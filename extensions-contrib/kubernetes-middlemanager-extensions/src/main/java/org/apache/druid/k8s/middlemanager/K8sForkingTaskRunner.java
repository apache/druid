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

package org.apache.druid.k8s.middlemanager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Pod;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.tasklogs.LogUtils;
import org.apache.druid.indexing.overlord.BaseRestorableTaskRunner;
import org.apache.druid.indexing.overlord.PortFinder;
import org.apache.druid.indexing.overlord.TaskRunnerUtils;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.k8s.middlemanager.common.K8sApiClient;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
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
public class K8sForkingTaskRunner
    extends BaseRestorableTaskRunner<K8sForkingTaskRunner.K8sForkingTaskRunnerWorkItem>
    implements TaskLogStreamer

{
  private static final EmittingLogger LOGGER = new EmittingLogger(K8sForkingTaskRunner.class);
  private static final String CHILD_PROPERTY_PREFIX = "druid.indexer.fork.property.";
  private static final String DRUID_INDEXER_NAMESPACE = "druid.indexer.namespace";
  private static final String DRUID_INDEXER_IMAGE = "druid.indexer.image";
  private static final String DRUID_INDEXER_DEFAULT_POD_CPU = "druid.indexer.default.pod.cpu";
  private static final String DRUID_INDEXER_DEFAULT_POD_MEMORY = "druid.indexer.default.pod.memory";
  private static final String DRUID_INDEXER_RUNNER_HOST_PATH = "druid.indexer.runner.hostPath";
  private static final String DRUID_INDEXER_RUNNER_MOUNT_PATH = "druid.indexer.runner.mountPath";
  private static final String DRUID_PEON_JAVA_OPTS = "druid.peon.javaOpts";
  private static final String DRUID_PEON_POD_MEMORY = "druid.peon.pod.memory";
  private static final String DRUID_PEON_POD_CPU = "druid.peon.pod.cpu";
  private static final String LABEL_KEY = "druid.ingest.task.id";
  private final ForkingTaskRunnerConfig config;
  private final Properties props;
  private final TaskLogPusher taskLogPusher;
  private final DruidNode node;
  private final ListeningExecutorService exec;
  private final PortFinder portFinder;
  private final StartupLoggingConfig startupLoggingConfig;
  private final K8sApiClient k8sApiClient;

  private volatile boolean stopping = false;
  private final String nameSpace;
  private final String image;
  private final String defaultPodCPU;
  private final String defaultPodMemory;

  @Inject
  public K8sForkingTaskRunner(
      ForkingTaskRunnerConfig config,
      TaskConfig taskConfig,
      WorkerConfig workerConfig,
      Properties props,
      TaskLogPusher taskLogPusher,
      ObjectMapper jsonMapper,
      @Self DruidNode node,
      StartupLoggingConfig startupLoggingConfig,
      K8sApiClient k8sApiClient
  )
  {
    super(jsonMapper, taskConfig);
    this.config = config;
    this.props = props;
    this.taskLogPusher = taskLogPusher;
    this.node = node;
    this.portFinder = new PortFinder(config.getStartPort(), config.getEndPort(), config.getPorts());
    this.startupLoggingConfig = startupLoggingConfig;
    this.exec = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(workerConfig.getCapacity(), "forking-task-runner-%d")
    );
    this.k8sApiClient = k8sApiClient;
    this.nameSpace = props.getProperty(DRUID_INDEXER_NAMESPACE, "default");
    this.image = props.getProperty(DRUID_INDEXER_IMAGE, "druid/cluster:v1");
    this.defaultPodCPU = props.getProperty(DRUID_INDEXER_DEFAULT_POD_CPU, "1");
    this.defaultPodMemory = props.getProperty(DRUID_INDEXER_DEFAULT_POD_MEMORY, "2G");

    assert image != null;
  }

  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    synchronized (tasks) {
      tasks.computeIfAbsent(
          task.getId(), k ->
          new K8sForkingTaskRunnerWorkItem(
            task,
            exec.submit(
              new Callable<TaskStatus>() {
                @Override
                public TaskStatus call()
                {
                  final String attemptUUID = UUID.randomUUID().toString();
                  final File taskDir = taskConfig.getTaskDir(task.getId());
                  final File attemptDir = new File(taskDir, attemptUUID);

                  final K8sProcessHolder processHolder;
                  // POD_IP is defined in env when create peon pod.
                  final String childHost = "$POD_IP";
                  final String tmpFileLoc = "/druidTmp";
                  int childPort = -1;
                  int tlsChildPort = -1;

                  if (node.isEnablePlaintextPort()) {
                    childPort = portFinder.findUnusedPort();
                  }

                  if (node.isEnableTlsPort()) {
                    tlsChildPort = portFinder.findUnusedPort();
                  }

                  TaskLocation taskLocation;

                  try {
                    final Closer closer = Closer.create();
                    try {

                      final File taskFile = new File(taskDir, "task.json");
                      final File statusFile = new File(attemptDir, "status.json");

                      final File logFile = new File(taskDir, "log");

                      if (!logFile.exists()) {
                        if (taskDir.exists()) {
                          logFile.createNewFile();
                        } else {
                          taskDir.mkdirs();
                          logFile.createNewFile();
                        }
                      }

                      final File reportsFile = new File(attemptDir, "report.json");
                      if (!reportsFile.exists()) {
                        if (attemptDir.exists()) {
                          reportsFile.createNewFile();
                        } else {
                          attemptDir.mkdirs();
                          reportsFile.createNewFile();
                        }
                      }
                      // time to adjust process holders
                      synchronized (tasks) {
                        // replace all the ": - . _" to "", try to reduce the length of pod name and meet pod naming specifications 63 charts.
                        final String label_value_ori = StringUtils.toLowerCase(
                                StringUtils.replace(
                                        StringUtils.replace(
                                                StringUtils.replace(
                                                        StringUtils.replace(
                                                                task.getId(),
                                                                "_", ""),
                                                        ":", ""),
                                                ".", ""),
                                        "-", ""));
                        String label_value;
                        if (label_value_ori.length() > 50) {
                          label_value = label_value_ori.substring(label_value_ori.length() - 50);
                        } else {
                          label_value = label_value_ori;
                        }

                        final K8sForkingTaskRunnerWorkItem taskWorkItem = tasks.get(task.getId());

                        if (taskWorkItem == null) {
                          LOGGER.makeAlert("TaskInfo disappeared!").addData("task", task.getId()).emit();
                          throw new ISE("TaskInfo disappeared for task[%s]!", task.getId());
                        }

                        if (taskWorkItem.shutdown) {
                          throw new IllegalStateException("Task has been shut down!");
                        }

                        if (taskWorkItem.processHolder != null) {
                          LOGGER.makeAlert("TaskInfo already has a processHolder")
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

                        String peonPodJavaOpts = task.getContextValue(DRUID_PEON_JAVA_OPTS, "");

                        // users can set a separate JVM config for each task through context
                        if (peonPodJavaOpts.isEmpty()) {
                          LOGGER.info("Get JavaOpts From ForkingTaskRunnerConfig [%s]", config.getJavaOpts());
                          Iterables.addAll(command, new K8sQuotableWhiteSpaceSplitter(config.getJavaOpts()));
                        } else {
                          LOGGER.info("Get JavaOpts From Task Context [%s]", peonPodJavaOpts);
                          Iterables.addAll(command, new K8sQuotableWhiteSpaceSplitter(peonPodJavaOpts));
                        }

                        Iterables.addAll(command, config.getJavaOptsArray());

                        // Override task specific javaOpts
                        Object taskJavaOpts = task.getContextValue(
                            ForkingTaskRunnerConfig.JAVA_OPTS_PROPERTY
                        );
                        if (taskJavaOpts != null) {
                          Iterables.addAll(
                              command,
                              new K8sQuotableWhiteSpaceSplitter((String) taskJavaOpts)
                          );
                        }

                        // useed for local deepStorage
                        String hostPath = props.getProperty(DRUID_INDEXER_RUNNER_HOST_PATH, "");
                        String mountPath = props.getProperty(DRUID_INDEXER_RUNNER_MOUNT_PATH, "");

                        for (String propName : props.stringPropertyNames()) {
                          for (String allowedPrefix : config.getAllowedPrefixes()) {
                            // See https://github.com/apache/druid/issues/1841
                            if (propName.startsWith(allowedPrefix)
                                && !ForkingTaskRunnerConfig.JAVA_OPTS_PROPERTY.equals(propName)
                                && !ForkingTaskRunnerConfig.JAVA_OPTS_ARRAY_PROPERTY.equals(propName)
                            ) {
                              // remove druid-kubernetes-middlemanager-extensions in druid.extensions.loadList for peon pod
                              if (propName.contains("druid.extensions.loadList")) {
                                String[] splits = StringUtils.replace(
                                        StringUtils.replace(
                                                props.getProperty(propName), "[", ""),
                                        "]", "")
                                        .split(",");
                                ArrayList<String> loadList = new ArrayList<>();
                                for (String extension : splits) {
                                  if (!extension.contains("druid-kubernetes-middlemanager-extensions")) {
                                    loadList.add(extension);
                                  }
                                }
                                command.add(
                                        StringUtils.format(
                                                "-D%s=%s",
                                                propName,
                                                loadList
                                        )
                                );
                              } else {
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
                        // don't support tlsPort for now
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

                        // If the task type is queryable, we need to load broadcast segments on the peon, used for
                        // join queries
                        if (task.supportsQueries()) {
                          command.add("--loadBroadcastSegments");
                          command.add("true");
                        }

                        String labels = LABEL_KEY + "=" + label_value;
                        if (!k8sApiClient.configMapIsExist(nameSpace, labels)) {
                          String taskString = jsonMapper.writeValueAsString(task);
                          k8sApiClient.createConfigMap(nameSpace, label_value, ImmutableMap.of(LABEL_KEY, label_value), ImmutableMap.of("task.json", taskString));
                        }

                        LOGGER.info("Running command: %s", getMaskedCommand(startupLoggingConfig.getMaskProperties(), command));

                        String cpu = defaultPodCPU;
                        String memory = defaultPodMemory;
                        String cpuFromContext = task.getContextValue(DRUID_PEON_POD_CPU, "");
                        String memoryFromContext = task.getContextValue(DRUID_PEON_POD_MEMORY, "");

                        if (!cpuFromContext.isEmpty()) {
                          cpu = cpuFromContext;
                        }
                        if (!memoryFromContext.isEmpty()) {
                          memory = memoryFromContext;
                        }

                        V1Pod peonPod = k8sApiClient.createPod(label_value,
                                image,
                                nameSpace,
                                ImmutableMap.of(LABEL_KEY, label_value),
                                ImmutableMap.of("cpu", Quantity.fromString(cpu), "memory", Quantity.fromString(memory)),
                                taskDir,
                                command,
                                childPort,
                                tlsChildPort,
                                tmpFileLoc,
                                "Never",
                                hostPath,
                                mountPath);
                        LOGGER.info("PeonPod created %s/%s", peonPod.getMetadata().getNamespace(), peonPod.getMetadata().getName());

                        k8sApiClient.waitForPodRunning(peonPod, labels);

                        taskLocation = TaskLocation.create(k8sApiClient.getPod(peonPod).getStatus().getPodIP(), childPort, tlsChildPort);

                        taskWorkItem.processHolder = new K8sProcessHolder(peonPod,
                          logFile,
                          taskLocation.getHost(),
                          taskLocation.getPort(),
                          taskLocation.getTlsPort(),
                          labels
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
                        ByteStreams.copy(processHolder.getInputStream(), toLogfile);

                        // wait for pod finished(Succeeded or Failed)
                        final String status = processHolder.waitForFinished();
                        LOGGER.info("Process exited with status[%s] for task: %s", status, task.getId());
                        if (status.equalsIgnoreCase("Succeeded")) {
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
                        status = TaskStatus.success(task.getId(), taskLocation);
                      } else {
                        // Process exited unsuccessfully
                        status = TaskStatus.failure(task.getId());
                      }
                      LOGGER.info("Current Task Status [%s]", status);

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
                    LOGGER.warn(t, "Exception caught during execution");
                    throw new RuntimeException(t);
                  }
                  finally {
                    try {
                      synchronized (tasks) {
                        final K8sForkingTaskRunnerWorkItem taskWorkItem = tasks.remove(task.getId());
                        if (taskWorkItem != null && taskWorkItem.processHolder != null) {
                          // delete finished pod
                          taskWorkItem.processHolder.deletePod();
                          taskWorkItem.processHolder.deleteConfigMap();

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
                          FileUtils.deleteDirectory(taskDir);
                          LOGGER.info("Removing task directory: %s", taskDir);
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
      for (K8sForkingTaskRunnerWorkItem taskWorkItem : tasks.values()) {
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
    final K8sForkingTaskRunnerWorkItem taskInfo;

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
      for (final K8sForkingTaskRunnerWorkItem taskWorkItem : tasks.values()) {
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
      for (final K8sForkingTaskRunnerWorkItem taskWorkItem : tasks.values()) {
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
    final K8sForkingTaskRunnerWorkItem workItem = tasks.get(taskId);
    if (workItem == null) {
      return null;
    } else {
      if (workItem.processHolder == null) {
        return RunnerTaskState.PENDING;
      } else if (workItem.processHolder.getPodStatus().equalsIgnoreCase("Running")) {
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
    final K8sProcessHolder processHolder;

    synchronized (tasks) {
      final K8sForkingTaskRunnerWorkItem taskWorkItem = tasks.get(taskid);
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
  private void shutdownTaskProcess(K8sForkingTaskRunnerWorkItem taskInfo)
  {
    if (taskInfo.processHolder != null) {
      // Will trigger normal failure mechanisms due to process exit
      LOGGER.info("Closing output stream to task[%s].", taskInfo.getTask().getId());
      try {
        taskInfo.processHolder.deletePod();
        taskInfo.processHolder.deleteConfigMap();
      }
      catch (Exception e) {
        LOGGER.warn(e, "Failed to close stdout to task[%s]. Destroying task.", taskInfo.getTask().getId());
        taskInfo.processHolder.deletePod();
        taskInfo.processHolder.deleteConfigMap();
      }
    }
  }

  String getMaskedCommand(List<String> maskedProperties, List<String> command)
  {
    final Set<String> maskedPropertiesSet = Sets.newHashSet(maskedProperties);
    final Iterator<String> maskedIterator = command.stream().map(element -> {
      String[] splits = element.split("=", 2);
      if (splits.length == 2) {
        for (String masked : maskedPropertiesSet) {
          if (splits[0].contains(masked)) {
            return StringUtils.format("%s=%s", splits[0], "<masked>");
          }
        }
      }
      return element;
    }).iterator();
    return Joiner.on(" ").join(maskedIterator);
  }

  @Override
  public long getTotalTaskSlotCount()
  {
    if (config.getPorts() != null && !config.getPorts().isEmpty()) {
      return config.getPorts().size();
    }
    return config.getEndPort() - config.getStartPort() + 1;
  }

  @Override
  public long getIdleTaskSlotCount()
  {
    return Math.max(getTotalTaskSlotCount() - getUsedTaskSlotCount(), 0);
  }

  @Override
  public long getUsedTaskSlotCount()
  {
    return portFinder.findUsedPortCount();
  }

  @Override
  public long getLazyTaskSlotCount()
  {
    return 0;
  }

  @Override
  public long getBlacklistedTaskSlotCount()
  {
    return 0;
  }

  protected static class K8sForkingTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    private final Task task;

    private volatile boolean shutdown = false;
    private volatile K8sProcessHolder processHolder = null;

    private K8sForkingTaskRunnerWorkItem(
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

  private class K8sProcessHolder
  {
    private final V1Pod peonPod;
    private final File logFile;
    private final String host;
    private final int port;
    private final int tlsPort;
    private final InputStream is;
    private final String labels;

    private K8sProcessHolder(V1Pod peonPod, File logFile, String host, int port, int tlsPort, String labels)
    {
      this.peonPod = peonPod;
      this.logFile = logFile;
      this.host = host;
      this.port = port;
      this.tlsPort = tlsPort;
      this.is = k8sApiClient.getPodLogs(peonPod);
      this.labels = labels;
    }

    private void registerWithCloser(Closer closer)
    {
      closer.register(is);
    }

    private InputStream getInputStream()
    {
      return is;
    }

    private String waitForFinished()
    {
      return k8sApiClient.waitForPodFinished(peonPod);
    }

    private void deletePod()
    {
      k8sApiClient.deletePod(peonPod);
    }

    private void deleteConfigMap()
    {
      k8sApiClient.deleteConfigMap(peonPod, labels);
    }


    private String getPodStatus()
    {
      return k8sApiClient.getPodStatus(peonPod);
    }
  }
}

/**
 * Make an iterable of space delimited strings... unless there are quotes, which it preserves
 */
class K8sQuotableWhiteSpaceSplitter implements Iterable<String>
{
  private final String string;

  public K8sQuotableWhiteSpaceSplitter(String string)
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
