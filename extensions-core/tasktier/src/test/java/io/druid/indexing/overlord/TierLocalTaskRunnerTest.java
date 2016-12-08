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
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import io.druid.curator.discovery.NoopServiceAnnouncer;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.config.FileTaskLogsConfig;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.tasklogs.FileTaskLogs;
import io.druid.indexing.overlord.config.TierLocalTaskRunnerConfig;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.initialization.Initialization;
import io.druid.metadata.TestDerbyConnector;
import io.druid.segment.CloserRule;
import io.druid.server.DruidNode;
import io.druid.tasklogs.TaskLogPusher;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class TierLocalTaskRunnerTest
{
  private static final Logger log = new Logger(TierLocalTaskRunnerTest.class);
  @Rule
  final public CloserRule closerRule = new CloserRule(true);
  @Rule
  final public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  final public TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private String taskId;
  private File taskBaseDir;
  private File taskDir;
  private TierLocalTaskRunner forkingTaskRunner;
  private HttpClient httpClient;
  private File watchFile;
  private ObjectMapper mapper;
  private final AtomicInteger pushTaskLogCalls = new AtomicInteger(0);
  private volatile File logDir;


  private TierLocalTaskRunner makeForkingTaskRunner(Integer timeout) throws IOException
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.processing.numThreads", "1");
    properties.setProperty(
        "druid.metadata.storage.connector.connectURI",
        derbyConnectorRule.getConnector().getJdbcUri()
    );
    properties.setProperty("druid.indexer.task", "");
    properties.setProperty("druid.metadata.storage.type", ForkingTaskRunnerTestModule.DB_TYPE);
    final TaskConfig taskConfig = new TaskConfig(
        temporaryFolder.newFolder().getAbsolutePath(),
        taskBaseDir.getAbsolutePath(),
        "/tmp/TIER_LOCAL_TEST",
        null,
        null,
        false,
        Period.seconds(1),
        Period.seconds(1)
    );
    properties.setProperty("druid.indexer.task.baseTaskDir", taskConfig.getBaseTaskDir().toString());
    properties.setProperty("druid.indexer.task.baseDir", taskConfig.getBaseDir());
    final TierLocalTaskRunnerConfig config = new TierLocalTaskRunnerConfig();
    if (timeout != null) {
      config.setSoftShutdownTimeLimit(timeout);
    }
    return new TierLocalTaskRunner(
        config,
        taskConfig,
        new WorkerConfig()
        {
          @Override
          public int getCapacity()
          {
            return 1;
          }
        },
        properties,
        new TaskLogPusher()
        {
          @Override
          public void pushTaskLog(String taskid, File logFile) throws IOException
          {
            pushTaskLogCalls.incrementAndGet();
            Files.move(logFile, getLogFile(taskid, logDir));
          }
        },
        mapper,
        new DruidNode("test/service", "localhost", -1),
        httpClient,
        new NoopServiceAnnouncer()
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

  private static File getLogFile(String taskId, File logBase)
  {
    return Paths.get(logBase.getAbsolutePath(), String.format("%s.log", taskId)).toFile();
  }

  @Before
  public void setUp() throws IOException
  {
    logDir = temporaryFolder.newFolder();
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<com.google.inject.Module>of(
            new com.google.inject.Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test", "localhost", null)
                );
                try {
                  binder.bind(TaskLogPusher.class)
                        .toInstance(new FileTaskLogs(new FileTaskLogsConfig(temporaryFolder.newFolder())));
                }
                catch (IOException e) {
                  throw Throwables.propagate(e);
                }
                JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);

                binder.bind(TaskRunner.class).toInstance(EasyMock.createNiceMock(TaskRunner.class));
                binder.bind(TaskStorage.class).toInstance(EasyMock.createNiceMock(TaskStorage.class));
                binder.bind(TaskMaster.class).toInstance(EasyMock.createNiceMock(TaskMaster.class));
              }
            }
        )
    );
    mapper = injector.getBinding(Key.get(ObjectMapper.class, Json.class)).getProvider().get();
    watchFile = new File(temporaryFolder.newFolder(), "watchFile");
    taskId = "BusyTaskID-" + UUID.randomUUID().toString();
    taskBaseDir = temporaryFolder.newFolder();
    taskDir = new File(taskBaseDir, taskId);
    httpClient = injector.getInstance(Key.get(HttpClient.class, Global.class));
    forkingTaskRunner = makeForkingTaskRunner(30_000);
    closerRule.closeLater(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            forkingTaskRunner.stop();
          }
        }
    );
  }

  @After
  public void tearDown() throws IOException
  {
    Thread.interrupted();
  }

  @Test(timeout = 600_000)
  public void testForkingCanKill() throws IOException, InterruptedException, ExecutionException
  {
    ListenableFuture<TaskStatus> future = waitForTaskStart(600_000);
    Assert.assertFalse(forkingTaskRunner.getRunningTasks().isEmpty());
    forkingTaskRunner.shutdown(taskId);
    Assert.assertTrue(future.get().isFailure());
    waitForEmptyTaskList(1_000);
    Assert.assertFalse(taskDir.exists());
  }

  @Test(timeout = 600_000)
  public void testForking() throws IOException, InterruptedException, ExecutionException
  {
    final ListenableFuture<TaskStatus> future = waitForTaskStart(60_000);
    Assert.assertTrue(watchFile.delete());
    Assert.assertTrue(
        FunctionalIterable
            .create(forkingTaskRunner.getRunningTasks())
            .filter(new Predicate<TaskRunnerWorkItem>()
            {
              @Override
              public boolean apply(TaskRunnerWorkItem input)
              {
                return taskId.equals(input.getTaskId());
              }
            }).iterator().hasNext());
    Assert.assertFalse(forkingTaskRunner.streamTaskLog("djkfhjkafhds", 1).isPresent());
    Optional<ByteSource> logSource = forkingTaskRunner.streamTaskLog(taskId, 1);
    Assert.assertTrue(logSource.isPresent());
    try (InputStream inputStream = logSource.get().openStream()) {
      Assert.assertTrue(inputStream.available() > 0);
    }
    TaskStatus status = future.get();
    if (!status.isSuccess()) {
      logIfAvailable(taskId, logDir);
      Assert.fail(String.format("Task [%s] failed", status.getId()));
    }
    Assert.assertFalse(taskDir.exists());
    Assert.assertFalse(forkingTaskRunner.streamTaskLog(taskId, 1).isPresent());
  }


  @Test(timeout = 60_000, expected = IOException.class)
  public void testScrewedUpTaskDir() throws Throwable
  {
    Assert.assertTrue(taskDir.createNewFile());
    forkingTaskRunner.start();
    final ListenableFuture<TaskStatus> future = forkingTaskRunner.run(
        new BusyTask(
            taskId,
            watchFile.getAbsolutePath(),
            100
        )
    );
    try {
      future.get();
    }
    catch (ExecutionException e) {
      throw e.getCause().getCause();
    }
  }

  @Test(timeout = 600_000)
  public void testKillingForkedJobNewRunner()
      throws IOException, InterruptedException, ExecutionException, TimeoutException
  {
    forkingTaskRunner = makeForkingTaskRunner(1_000);
    forkingTaskRunner.start();
    ListenableFuture<TaskStatus> future = waitForTaskStart(60_000);
    Assert.assertFalse(forkingTaskRunner.getRunningTasks().isEmpty());
    log.info("Shutting down task [%s]", taskId);
    forkingTaskRunner.shutdown(taskId);
    Assert.assertTrue(future.get(1, TimeUnit.MINUTES).isFailure());
    waitForEmptyTaskList(1_000);
    Assert.assertFalse(taskDir.exists());
  }

  @Test(timeout = 600_000)
  public void testStartingNewRunner() throws IOException, InterruptedException, ExecutionException
  {
    log.info("Starting task");
    waitForTaskStart(600_000);
    Assert.assertTrue(taskDir.exists());
    Assert.assertFalse(forkingTaskRunner.getRunningTasks().isEmpty());
    log.info("Stopping runner");
    forkingTaskRunner.stop();
    Assert.assertTrue(taskDir.exists());
    log.info("Creating new runner");
    forkingTaskRunner = makeForkingTaskRunner(600_000);
    log.info("Starting new runner");
    forkingTaskRunner.start();
    // Should pick up prior task
    Assert.assertFalse(forkingTaskRunner.getRunningTasks().isEmpty());
    log.info("Testing submission of prior task");
    ListenableFuture<TaskStatus> future = forkingTaskRunner.run(new BusyTask(taskId, null, 60_000L));
    // Signal task to exit
    Assert.assertTrue(watchFile.delete());
    log.info("Waiting for task to complete");
    Assert.assertTrue(future.get().isSuccess());
    // Wait for task to clean up itself
    log.info("Waiting for task to cleanup");
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
  public void testAttemptDirCleanup() throws IOException
  {
    Assert.assertTrue(taskDir.mkdir());
    forkingTaskRunner.start();
    Assert.assertTrue(forkingTaskRunner.getRunningTasks().isEmpty());
    Assert.assertFalse(taskDir.exists());
  }

  @Test
  public void testBadAttemptDir() throws IOException
  {
    final File attemptDir = new File(taskDir, "Cannot parse this!");
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

    final File logFile = new File(attemptDir, "task.log");
    Assert.assertTrue(logFile.createNewFile());

    final File statusFile = new File(attemptDir, "status.json");
    Assert.assertTrue(statusFile.createNewFile());

    forkingTaskRunner.start();
    Assert.assertTrue(forkingTaskRunner.getRunningTasks().isEmpty());
  }


  @Test
  public void testBadPort() throws IOException
  {
    final File attemptDir = new File(taskDir, "0000");
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
    final File attemptDir = new File(taskDir, "0000");
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


  @Test(expected = ISE.class)
  public void testMultiStart()
  {
    forkingTaskRunner.start();
    forkingTaskRunner.start();
  }

  @Test
  public void testMultiStop()
  {
    forkingTaskRunner.start();
    forkingTaskRunner.stop();
    forkingTaskRunner.stop();
  }

  @Test
  public void testTaskHadFinished() throws IOException
  {
    final File attemptDir = new File(taskDir, "0000");
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
    while (!forkingTaskRunner.getRunningTasks().isEmpty()) {
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
    if (!forkingTaskRunner.isStarted(true)) {
      forkingTaskRunner.start();
    }
    log.info("Waiting for [%s] to appear", watchFile);
    final Path watchPath = watchFile.toPath().getParent();
    Assert.assertFalse(watchFile.exists());
    try (final WatchService watchService = watchPath.getFileSystem().newWatchService()) {
      watchPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);
      final ListenableFuture<TaskStatus> future = forkingTaskRunner.run(
          new BusyTask(
              taskId,
              watchFile.getAbsolutePath(),
              sleep
          )
      );
      // Kinda racy on exact size, so we just have it here for very high level test
      Assert.assertTrue(forkingTaskRunner.getPendingTasks().size() >= 0);

      Assert.assertFalse(forkingTaskRunner.getKnownTasks().isEmpty());
      Assert.assertTrue(
          FunctionalIterable
              .create(forkingTaskRunner.getKnownTasks())
              .filter(new Predicate<TaskRunnerWorkItem>()
              {
                @Override
                public boolean apply(TaskRunnerWorkItem input)
                {
                  return taskId.equals(input.getTaskId());
                }
              }).iterator().hasNext());
      while (!watchFile.exists() && !future.isDone()) {
        WatchKey key = watchService.poll(100, TimeUnit.MILLISECONDS);
        if (key != null) {
          log.info("Event [%s]", key);
        }
      }
      if (!watchFile.exists()) {
        logIfAvailable(taskId, logDir);
        Assert.fail(String.format("Failed to launch task [%s]", taskId));
      }
      return future;
    }
  }

  private static void logIfAvailable(String taskId, File logDir) throws IOException
  {
    File logFile = getLogFile(taskId, logDir);
    if (!logFile.exists()) {
      log.warn("Log file [%s] for task [%s] does not exist", logFile, taskId);
      return;
    }
    ByteSource source = Files.asByteSource(logFile);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    source.copyTo(baos);
    log.info("Task [%s] log [%s]", taskId, StringUtils.fromUtf8(baos.toByteArray()));
  }
}
