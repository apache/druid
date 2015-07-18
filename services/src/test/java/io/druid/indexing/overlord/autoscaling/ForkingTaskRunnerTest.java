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

package io.druid.indexing.overlord.autoscaling;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.common.io.Closer;
import com.google.common.io.FileBackedOutputStream;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.overlord.ForkingTaskRunner;
import io.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.initialization.DruidModule;
import io.druid.initialization.Initialization;
import io.druid.server.DruidNode;
import io.druid.tasklogs.TaskLogPusher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Ignore // Takes too long to run on Travis
public class ForkingTaskRunnerTest
{
  private final Closer closer = Closer.create();
  @Rule
  final public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private String taskId;
  private File taskBaseDir;
  private File taskDir;
  private ForkingTaskRunner forkingTaskRunner;
  private static final Injector injector = Initialization.makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(), ImmutableList.<com.google.inject.Module>of(
          new com.google.inject.Module()
          {
            @Override
            public void configure(Binder binder)
            {
              JsonConfigProvider.bindInstance(
                  binder, Key.get(DruidNode.class, Self.class), new DruidNode("test", "localhost", null)
              );
            }
          }
      )
  );
  private HttpClient httpClient;
  private File watchFile;
  private ObjectMapper mapper;
  private final AtomicInteger pushTaskLogCalls = new AtomicInteger(0);


  private ForkingTaskRunner makeForkingTaskRunner(Integer timeout) throws IOException
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.processing.numThreads", "1");
    final ForkingTaskRunnerConfig config = new ForkingTaskRunnerConfig();
    if (timeout != null) {
      config.setSoftShutdownTimeLimit(timeout);
    }
    return new ForkingTaskRunner(
        config,
        new TaskConfig(
            temporaryFolder.newFolder().getAbsolutePath(),
            taskBaseDir.getAbsolutePath(),
            "/tmp",
            null,
            null
        ),
        new WorkerConfig(),
        properties,
        new TaskLogPusher()
        {
          @Override
          public void pushTaskLog(String taskid, File logFile) throws IOException
          {
            pushTaskLogCalls.incrementAndGet();
          }
        },
        mapper,
        new DruidNode("test/service", "localhost", -1),
        httpClient
    )
    {
      @Override
      public void stop()
      {
        super.stop();
        // Since we don't kill JVM between unit test instances, we want to make sure futures are trashed.
        exec.shutdownNow();
      }
    };
  }

  @Before
  public void setUp() throws IOException
  {
    mapper = injector.getBinding(Key.get(ObjectMapper.class, Json.class)).getProvider().get();
    watchFile = new File(temporaryFolder.newFolder(), "watchFile");
    taskId = "BusyTaskID-" + UUID.randomUUID().toString();
    taskBaseDir = temporaryFolder.newFolder();
    taskDir = new File(taskBaseDir, taskId);
    httpClient = injector.getInstance(Key.get(HttpClient.class, Global.class));
    forkingTaskRunner = makeForkingTaskRunner(30_000);
  }

  @After
  public void tearDown() throws IOException
  {
    watchFile.delete();
    forkingTaskRunner.stop();
    closer.close();
  }

  @Test(timeout = 600_000)
  public void testForkingCanKill() throws IOException, InterruptedException, ExecutionException
  {
    ListenableFuture<TaskStatus> future = waitForTaskStart(600_000);
    Assert.assertFalse(forkingTaskRunner.getRunningTasks().isEmpty());
    forkingTaskRunner.shutdown(taskId);
    waitForEmptyTaskList(1_000);
    Assert.assertTrue(future.get().isFailure());
    Assert.assertFalse(taskDir.exists());
  }

  @Test(timeout = 600_000)
  public void testForking() throws IOException, InterruptedException, ExecutionException
  {
    final ListenableFuture<TaskStatus> future = waitForTaskStart(60_000);
    Assert.assertTrue(watchFile.delete());
    Assert.assertTrue(future.get().isSuccess());
    Assert.assertFalse(taskDir.exists());
  }

  @Test(timeout = 600_000)
  public void testKillingForkedJobNewRunner() throws IOException, InterruptedException, ExecutionException
  {
    forkingTaskRunner = makeForkingTaskRunner(1_000);
    forkingTaskRunner.start();
    ListenableFuture<TaskStatus> future = waitForTaskStart(60_000);
    Assert.assertFalse(forkingTaskRunner.getRunningTasks().isEmpty());
    forkingTaskRunner.shutdown(taskId);
    waitForEmptyTaskList(1_000);
    Assert.assertTrue(future.get().isFailure());
    Assert.assertFalse(taskDir.exists());
  }

  @Test(timeout = 600_000)
  public void testStartingNewRunner() throws IOException, InterruptedException, ExecutionException
  {
    waitForTaskStart(600_000);
    Assert.assertTrue(taskDir.exists());
    Assert.assertFalse(forkingTaskRunner.getRunningTasks().isEmpty());
    forkingTaskRunner.stop();
    Assert.assertTrue(taskDir.exists());
    forkingTaskRunner = makeForkingTaskRunner(600_000);
    forkingTaskRunner.start();
    // Should pick up prior task
    Assert.assertFalse(forkingTaskRunner.getRunningTasks().isEmpty());
    ListenableFuture<TaskStatus> future = forkingTaskRunner.run(new BusyTask(taskId, null, 60_000L));
    // Signal task to exit
    Assert.assertTrue(watchFile.delete());
    Assert.assertTrue(future.get().isSuccess());
    // Wait for task to clean up itself
    if (taskDir.exists()) {
      try (WatchService watchService = taskDir.toPath().getFileSystem().newWatchService()) {
        taskDir.toPath().getParent().register(watchService, StandardWatchEventKinds.ENTRY_DELETE);
        while (taskDir.exists()) {
          Assert.assertNotNull(watchService.poll(1, TimeUnit.MINUTES));
        }
      }
    }
    Assert.assertFalse(taskDir.exists());
  }

  @Test
  public void testBadPort() throws IOException
  {
    final File attemptDir = new File(taskDir, "attempt_dir");
    Assert.assertTrue(attemptDir.mkdirs());

    final File portFile = new File(attemptDir, "task.port");
    writeStringToFile("bad string", portFile);

    final File taskFile = new File(attemptDir, "task.json");
    writeStringToFile(
        mapper.writeValueAsString(
            new BusyTask(
                taskId,
                watchFile.getAbsolutePath(),
                100
            )
        ), taskFile
    );

    final File logFile = new File(attemptDir, "task.log");
    Assert.assertTrue(logFile.createNewFile());

    final File statusFile = new File(attemptDir, "status.json");
    Assert.assertTrue(statusFile.createNewFile());

    forkingTaskRunner.start();
    waitForEmptyTaskList(1_000);
  }

  @Test
  public void testBadTask() throws IOException
  {
    final File attemptDir = new File(taskDir, "attempt_dir");
    Assert.assertTrue(attemptDir.mkdirs());

    final File portFile = new File(attemptDir, "task.port");
    writeStringToFile("12345", portFile);

    final File taskFile = new File(attemptDir, "task.json");
    writeStringToFile(
        mapper.writeValueAsString(
            new BusyTask(
                taskId,
                watchFile.getAbsolutePath(),
                100
            )
        ), taskFile
    );

    try (FileOutputStream fos = new FileOutputStream(taskFile)) {
      fos.write(new byte[]{1, 2, 3, 4, 5, 6});
    }

    final File logFile = new File(attemptDir, "task.log");
    Assert.assertTrue(logFile.createNewFile());

    final File statusFile = new File(attemptDir, "status.json");
    Assert.assertTrue(statusFile.createNewFile());

    forkingTaskRunner.start();
    waitForEmptyTaskList(1_000);
  }


  @Test
  public void testTaskHadFinished() throws IOException
  {
    final File attemptDir = new File(taskDir, "attempt_dir");
    Assert.assertTrue(attemptDir.mkdirs());

    final File taskFile = new File(attemptDir, "task.json");
    writeStringToFile(
        mapper.writeValueAsString(
            new BusyTask(
                taskId,
                watchFile.getAbsolutePath(),
                100
            )
        ), taskFile
    );

    final File logFile = new File(attemptDir, "task.log");
    Assert.assertTrue(logFile.createNewFile());

    final File statusFile = new File(attemptDir, "status.json");
    writeStringToFile(mapper.writeValueAsString(TaskStatus.success(taskId)), statusFile);

    Assert.assertEquals(0, pushTaskLogCalls.get());
    forkingTaskRunner.start();
    waitForEmptyTaskList(1_000);
    Assert.assertEquals(1, pushTaskLogCalls.get());
  }

  private void waitForEmptyTaskList(long timeout)
  {
    long start = System.currentTimeMillis();
    while(!forkingTaskRunner.getRunningTasks().isEmpty())
    {
      Assert.assertTrue(System.currentTimeMillis() - start < timeout);
    }
  }

  private void writeStringToFile(String string, final File file) throws IOException
  {
    new ByteSink()
    {
      @Override
      public OutputStream openStream() throws IOException
      {
        return new FileOutputStream(file);
      }
    }.write(
        StringUtils.toUtf8(string)
    );
    Assert.assertTrue(file.exists());
    Assert.assertTrue(file.length() > 0);
  }

  private ListenableFuture<TaskStatus> waitForTaskStart(long sleep) throws InterruptedException, IOException
  {
    if (!forkingTaskRunner.isStarted()) {
      forkingTaskRunner.start();
    }
    final Path watchPath = watchFile.toPath().getParent();
    Assert.assertFalse(watchFile.exists());
    try (final WatchService watchService = watchPath.getFileSystem().newWatchService()) {
      closer.register(watchService);
      watchPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
      final ListenableFuture<TaskStatus> future = forkingTaskRunner.run(
          new BusyTask(
              taskId,
              watchFile.getAbsolutePath(),
              sleep
          )
      );
      while (!watchFile.exists()) {
        watchService.take();
      }
      Assert.assertTrue(watchFile.exists());
      return future;
    }
  }

  @JsonTypeName("busyTask")
  public static class BusyTask extends AbstractTask
  {
    private static final Logger log = new Logger(BusyTask.class);
    private final String lockFile;
    private final long sleep;

    public BusyTask(
        @JsonProperty("id") String id,
        @JsonProperty("lockFile") String lockFile,
        @JsonProperty("sleep") long sleep
    )
    {
      super(id == null ? "testTask-" + UUID.randomUUID().toString() : id, "noDataSource");
      this.lockFile = lockFile;
      this.sleep = sleep;
    }

    @JsonProperty("lockFile")
    public String getLockFile()
    {
      return lockFile;
    }

    @JsonProperty("sleep")
    public long getSleep()
    {
      return sleep;
    }

    @Override
    public String getType()
    {
      return "busyTask";
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient) throws Exception
    {
      return true;
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      log.info("Deleting file at [%s]", getLockFile());
      File file = new File(getLockFile());
      if (!file.createNewFile()) {
        log.error("Error deleting file at [%s]", file);
      }
      final Path path = file.toPath();
      while (file.exists()) {
        try (WatchService service = path.getFileSystem().newWatchService()) {
          path.getParent().register(service, StandardWatchEventKinds.ENTRY_DELETE);
          if (file.exists()) {
            WatchKey key = service.poll(sleep, TimeUnit.MILLISECONDS);
            if (key == null) {
              log.error("Ran out of time waiting for [%s]", path);
              return TaskStatus.failure(getId());
            }
            log.info("Delete event found for [%s]", path);
          }
        }
      }
      return TaskStatus.success(getId());
    }
  }


  public static class ForkingTaskRunnerTestModule implements DruidModule
  {
    @Override
    public List<? extends Module> getJacksonModules()
    {
      final SimpleModule module = new SimpleModule("ForkingTaskRunnerTestModule");
      module.registerSubtypes(BusyTask.class);
      return ImmutableList.of(module);
    }

    @Override
    public void configure(Binder binder)
    {
      // NOOP
    }
  }
}
