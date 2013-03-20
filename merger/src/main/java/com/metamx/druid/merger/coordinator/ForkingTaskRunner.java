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
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.actions.TaskActionClient;
import com.metamx.druid.merger.common.actions.TaskActionClientFactory;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.config.ForkingTaskRunnerConfig;
import com.metamx.druid.merger.worker.executor.ExecutorMain;
import com.metamx.emitter.EmittingLogger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.Collection;
import java.util.List;
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

  private final Object lock = new Object();
  private final ForkingTaskRunnerConfig config;
  private final ListeningExecutorService exec;
  private final ObjectMapper jsonMapper;
  private final List<ProcessHolder> processes = Lists.newArrayList();

  public ForkingTaskRunner(
      ForkingTaskRunnerConfig config,
      ExecutorService exec,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
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
            // TODO Keep around for some amount of time?
            // TODO Directory per attempt? token? uuid?
            final File tempDir = Files.createTempDir();
            ProcessHolder processHolder = null;

            try {
              final File taskFile = new File(tempDir, "task.json");
              final File statusFile = new File(tempDir, "status.json");
              final File logFile = new File(tempDir, "log");

              // locked so we can choose childHost/childPort based on processes.size
              // and make sure we don't double up on ProcessHolders for a task
              synchronized (lock) {
                if (getProcessHolder(task.getId()).isPresent()) {
                  throw new ISE("Task already running: %s", task.getId());
                }

                final List<String> command = Lists.newArrayList();
                final int childPort = config.getStartPort() + processes.size();
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

                // TODO configurable
                command.add(ExecutorMain.class.getName());
                command.add(taskFile.toString());
                command.add(statusFile.toString());

                Files.write(jsonMapper.writeValueAsBytes(task), taskFile);

                log.info("Running command: %s", Joiner.on(" ").join(command));
                processHolder = new ProcessHolder(
                    task,
                    new ProcessBuilder(ImmutableList.copyOf(command)).redirectErrorStream(true).start(),
                    logFile
                );

                processes.add(processHolder);
              }

              log.info("Logging task %s output to: %s", task.getId(), logFile);
              final OutputStream toLogfile = Files.newOutputStreamSupplier(logFile).getOutput();

              final InputStream fromProc = processHolder.process.getInputStream();
              ByteStreams.copy(fromProc, toLogfile);
              fromProc.close();
              toLogfile.close();

              final int statusCode = processHolder.process.waitFor();

              if (statusCode == 0) {
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
              if (processHolder != null) {
                synchronized (lock) {
                  processes.remove(processHolder);
                }
              }

              if (tempDir.exists()) {
                log.info("Removing temporary directory: %s", tempDir);
                // TODO may want to keep this around a bit longer
//                try {
//                  FileUtils.deleteDirectory(tempDir);
//                }
//                catch (IOException e) {
//                  log.error(e, "Failed to delete temporary directory");
//                }
              }
            }
          }
        }
    );
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      exec.shutdownNow();

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
    if(processHolder.isPresent()) {
      final int shutdowns = processHolder.get().shutdowns.getAndIncrement();
      if (shutdowns == 0) {
        log.info("Attempting to gracefully shutdown task: %s", taskid);
        try {
          // TODO this is the WORST
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
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      } else {
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
  public Optional<InputSupplier<InputStream>> getLogs(final String taskid, final long offset)
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

  private Optional<ProcessHolder> getProcessHolder(final String taskid)
  {
    synchronized (lock) {
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

    private AtomicInteger shutdowns = new AtomicInteger(0);

    private ProcessHolder(Task task, Process process, File logFile)
    {
      this.task = task;
      this.process = process;
      this.logFile = logFile;
    }
  }
}
