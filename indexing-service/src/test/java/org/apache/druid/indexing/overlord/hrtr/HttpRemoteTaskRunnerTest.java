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

package org.apache.druid.indexing.overlord.hrtr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.common.guava.DSuppliers;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.discovery.WorkerNodeService;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.autoscaling.NoopProvisioningStrategy;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.easymock.EasyMock;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class HttpRemoteTaskRunnerTest
{
  /*
  Simulates startup of Overlord and Workers being discovered with no previously known tasks. Fresh tasks are given
  and expected to be completed.
   */
  @Test(timeout = 60_000L)
  public void testFreshStart() throws Exception
  {
    TestDruidNodeDiscovery druidNodeDiscovery = new TestDruidNodeDiscovery();
    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);

    HttpRemoteTaskRunner taskRunner = new HttpRemoteTaskRunner(
        TestHelper.makeJsonMapper(),
        new HttpRemoteTaskRunnerConfig()
        {
          @Override
          public int getPendingTasksRunnerNumThreads()
          {
            return 3;
          }
        },
        EasyMock.createNiceMock(HttpClient.class),
        DSuppliers.of(new AtomicReference<>(DefaultWorkerBehaviorConfig.defaultConfig())),
        new NoopProvisioningStrategy<>(),
        druidNodeDiscoveryProvider,
        EasyMock.createNiceMock(TaskStorage.class),
        EasyMock.createNiceMock(CuratorFramework.class),
        new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null)
    )
    {
      @Override
      protected WorkerHolder createWorkerHolder(
          ObjectMapper smileMapper,
          HttpClient httpClient,
          HttpRemoteTaskRunnerConfig config,
          ScheduledExecutorService workersSyncExec,
          WorkerHolder.Listener listener,
          Worker worker,
          List<TaskAnnouncement> knownAnnouncements
      )
      {
        return HttpRemoteTaskRunnerTest.createWorkerHolder(
            smileMapper,
            httpClient,
            config,
            workersSyncExec,
            listener,
            worker,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            new AtomicInteger(),
            ImmutableSet.of()
        );
      }
    };

    taskRunner.start();

    DiscoveryDruidNode druidNode1 = new DiscoveryDruidNode(
        new DruidNode("service", "host1", false, 8080, null, true, false),
        NodeType.MIDDLE_MANAGER,
        ImmutableMap.of(
            WorkerNodeService.DISCOVERY_SERVICE_KEY, new WorkerNodeService("ip1", 2, "0", WorkerConfig.DEFAULT_CATEGORY)
        )
    );

    DiscoveryDruidNode druidNode2 = new DiscoveryDruidNode(
        new DruidNode("service", "host2", false, 8080, null, true, false),
        NodeType.MIDDLE_MANAGER,
        ImmutableMap.of(
            WorkerNodeService.DISCOVERY_SERVICE_KEY, new WorkerNodeService("ip2", 2, "0", WorkerConfig.DEFAULT_CATEGORY)
        )
    );

    druidNodeDiscovery.listener.nodesAdded(ImmutableList.of(druidNode1, druidNode2));

    int numTasks = 8;
    List<Future<TaskStatus>> futures = new ArrayList<>();
    for (int i = 0; i < numTasks; i++) {
      futures.add(taskRunner.run(NoopTask.create("task-id-" + i, 0)));
    }

    for (Future<TaskStatus> future : futures) {
      Assert.assertTrue(future.get().isSuccess());
    }

    Assert.assertEquals(numTasks, taskRunner.getKnownTasks().size());
    Assert.assertEquals(numTasks, taskRunner.getCompletedTasks().size());
  }

  /*
  Simulates one task not getting acknowledged to be running after assigning it to a worker. But, other tasks are
  successfully assigned to other worker and get completed.
   */
  @Test(timeout = 60_000L)
  public void testOneStuckTaskAssignmentDoesntBlockOthers() throws Exception
  {
    TestDruidNodeDiscovery druidNodeDiscovery = new TestDruidNodeDiscovery();
    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);

    Task task1 = NoopTask.create("task-id-1", 0);
    Task task2 = NoopTask.create("task-id-2", 0);
    Task task3 = NoopTask.create("task-id-3", 0);

    HttpRemoteTaskRunner taskRunner = new HttpRemoteTaskRunner(
        TestHelper.makeJsonMapper(),
        new HttpRemoteTaskRunnerConfig()
        {
          @Override
          public int getPendingTasksRunnerNumThreads()
          {
            return 3;
          }
        },
        EasyMock.createNiceMock(HttpClient.class),
        DSuppliers.of(new AtomicReference<>(DefaultWorkerBehaviorConfig.defaultConfig())),
        new NoopProvisioningStrategy<>(),
        druidNodeDiscoveryProvider,
        EasyMock.createNiceMock(TaskStorage.class),
        EasyMock.createNiceMock(CuratorFramework.class),
        new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null)
    )
    {
      @Override
      protected WorkerHolder createWorkerHolder(
          ObjectMapper smileMapper,
          HttpClient httpClient,
          HttpRemoteTaskRunnerConfig config,
          ScheduledExecutorService workersSyncExec,
          WorkerHolder.Listener listener,
          Worker worker,
          List<TaskAnnouncement> knownAnnouncements
      )
      {
        return HttpRemoteTaskRunnerTest.createWorkerHolder(
            smileMapper,
            httpClient,
            config,
            workersSyncExec,
            listener,
            worker,
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(task1, ImmutableList.of()), //no announcements would be received for task1
            new AtomicInteger(),
            ImmutableSet.of()
        );
      }
    };

    taskRunner.start();

    DiscoveryDruidNode druidNode1 = new DiscoveryDruidNode(
        new DruidNode("service", "host1", false, 8080, null, true, false),
        NodeType.MIDDLE_MANAGER,
        ImmutableMap.of(
            WorkerNodeService.DISCOVERY_SERVICE_KEY, new WorkerNodeService("ip1", 2, "0", WorkerConfig.DEFAULT_CATEGORY)
        )
    );

    DiscoveryDruidNode druidNode2 = new DiscoveryDruidNode(
        new DruidNode("service", "host2", false, 8080, null, true, false),
        NodeType.MIDDLE_MANAGER,
        ImmutableMap.of(
            WorkerNodeService.DISCOVERY_SERVICE_KEY, new WorkerNodeService("ip2", 2, "0", WorkerConfig.DEFAULT_CATEGORY)
        )
    );

    druidNodeDiscovery.listener.nodesAdded(ImmutableList.of(druidNode1, druidNode2));

    taskRunner.run(task1);
    Future<TaskStatus> future2 = taskRunner.run(task2);
    Future<TaskStatus> future3 = taskRunner.run(task3);

    Assert.assertTrue(future2.get().isSuccess());
    Assert.assertTrue(future3.get().isSuccess());

    Assert.assertEquals(task1.getId(), Iterables.getOnlyElement(taskRunner.getPendingTasks()).getTaskId());
  }

  /*
  Simulates restart of the Overlord where taskRunner, on start, discovers workers with prexisting tasks.
   */
  @Test(timeout = 60_000L)
  public void testTaskRunnerRestart() throws Exception
  {
    TestDruidNodeDiscovery druidNodeDiscovery = new TestDruidNodeDiscovery();
    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);

    ConcurrentMap<String, CustomFunction> workerHolders = new ConcurrentHashMap<>();

    Task task1 = NoopTask.create("task-id-1", 0);
    Task task2 = NoopTask.create("task-id-2", 0);
    Task task3 = NoopTask.create("task-id-3", 0);
    Task task4 = NoopTask.create("task-id-4", 0);
    Task task5 = NoopTask.create("task-id-5", 0);

    TaskStorage taskStorageMock = EasyMock.createStrictMock(TaskStorage.class);
    EasyMock.expect(taskStorageMock.getStatus(task1.getId())).andReturn(Optional.absent());
    EasyMock.expect(taskStorageMock.getStatus(task2.getId())).andReturn(Optional.absent()).times(2);
    EasyMock.expect(taskStorageMock.getStatus(task3.getId())).andReturn(Optional.of(TaskStatus.running(task3.getId())));
    EasyMock.expect(taskStorageMock.getStatus(task4.getId())).andReturn(Optional.of(TaskStatus.running(task4.getId())));
    EasyMock.expect(taskStorageMock.getStatus(task5.getId())).andReturn(Optional.of(TaskStatus.success(task5.getId())));
    EasyMock.replay(taskStorageMock);

    HttpRemoteTaskRunner taskRunner = new HttpRemoteTaskRunner(
        TestHelper.makeJsonMapper(),
        new HttpRemoteTaskRunnerConfig()
        {
          @Override
          public int getPendingTasksRunnerNumThreads()
          {
            return 3;
          }
        },
        EasyMock.createNiceMock(HttpClient.class),
        DSuppliers.of(new AtomicReference<>(DefaultWorkerBehaviorConfig.defaultConfig())),
        new NoopProvisioningStrategy<>(),
        druidNodeDiscoveryProvider,
        taskStorageMock,
        EasyMock.createNiceMock(CuratorFramework.class),
        new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null)
    )
    {
      @Override
      protected WorkerHolder createWorkerHolder(
          ObjectMapper smileMapper,
          HttpClient httpClient,
          HttpRemoteTaskRunnerConfig config,
          ScheduledExecutorService workersSyncExec,
          WorkerHolder.Listener listener,
          Worker worker,
          List<TaskAnnouncement> knownAnnouncements
      )
      {
        if (workerHolders.containsKey(worker.getHost())) {
          return workerHolders.get(worker.getHost()).apply(
              smileMapper,
              httpClient,
              config,
              workersSyncExec,
              listener,
              worker,
              knownAnnouncements
          );
        } else {
          throw new ISE("No WorkerHolder for [%s].", worker.getHost());
        }
      }
    };

    taskRunner.start();

    DiscoveryDruidNode druidNode = new DiscoveryDruidNode(
        new DruidNode("service", "host", false, 1234, null, true, false),
        NodeType.MIDDLE_MANAGER,
        ImmutableMap.of(
            WorkerNodeService.DISCOVERY_SERVICE_KEY, new WorkerNodeService("ip1", 2, "0", WorkerConfig.DEFAULT_CATEGORY)
        )
    );

    AtomicInteger ticks = new AtomicInteger();
    Set<String> taskShutdowns = new HashSet<>();

    workerHolders.put(
        "host:1234",
        (mapper, httpClient, config, exec, listener, worker, knownAnnouncements) -> createWorkerHolder(
            mapper,
            httpClient,
            config,
            exec,
            listener,
            worker,
            knownAnnouncements,
            ImmutableList.of(
                TaskAnnouncement.create(
                    task1,
                    TaskStatus.success(task1.getId()),
                    TaskLocation.create("host", 1234, 1235)
                ),
                TaskAnnouncement.create(
                    task2,
                    TaskStatus.running(task2.getId()),
                    TaskLocation.create("host", 1234, 1235)
                ),
                TaskAnnouncement.create(
                    task2,
                    TaskStatus.success(task2.getId()),
                    TaskLocation.create("host", 1234, 1235)
                ),
                TaskAnnouncement.create(
                    task3,
                    TaskStatus.success(task3.getId()),
                    TaskLocation.create("host", 1234, 1235)
                ),
                TaskAnnouncement.create(
                    task4,
                    TaskStatus.running(task4.getId()),
                    TaskLocation.create("host", 1234, 1235)
                ),
                TaskAnnouncement.create(
                    task5,
                    TaskStatus.running(task5.getId()),
                    TaskLocation.create("host", 1234, 1235)
                )
            ),
            ImmutableMap.of(),
            ticks,
            taskShutdowns
        )
    );

    druidNodeDiscovery.listener.nodesAdded(
        ImmutableList.of(
            druidNode
        )
    );

    while (ticks.get() < 1) {
      Thread.sleep(100);
    }

    EasyMock.verify(taskStorageMock);

    Assert.assertEquals(ImmutableSet.of(task2.getId(), task5.getId()), taskShutdowns);
    Assert.assertTrue(taskRunner.getPendingTasks().isEmpty());

    TaskRunnerWorkItem item = Iterables.getOnlyElement(taskRunner.getRunningTasks());
    Assert.assertEquals(task4.getId(), item.getTaskId());

    Assert.assertTrue(taskRunner.run(task3).get().isSuccess());

    Assert.assertEquals(2, taskRunner.getKnownTasks().size());

  }

  @Test(timeout = 60_000L)
  public void testWorkerDisapperAndReappearBeforeItsCleanup() throws Exception
  {
    TestDruidNodeDiscovery druidNodeDiscovery = new TestDruidNodeDiscovery();
    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);

    ConcurrentMap<String, CustomFunction> workerHolders = new ConcurrentHashMap<>();

    HttpRemoteTaskRunner taskRunner = new HttpRemoteTaskRunner(
        TestHelper.makeJsonMapper(),
        new HttpRemoteTaskRunnerConfig()
        {
          @Override
          public int getPendingTasksRunnerNumThreads()
          {
            return 3;
          }
        },
        EasyMock.createNiceMock(HttpClient.class),
        DSuppliers.of(new AtomicReference<>(DefaultWorkerBehaviorConfig.defaultConfig())),
        new NoopProvisioningStrategy<>(),
        druidNodeDiscoveryProvider,
        EasyMock.createNiceMock(TaskStorage.class),
        EasyMock.createNiceMock(CuratorFramework.class),
        new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null)
    )
    {
      @Override
      protected WorkerHolder createWorkerHolder(
          ObjectMapper smileMapper,
          HttpClient httpClient,
          HttpRemoteTaskRunnerConfig config,
          ScheduledExecutorService workersSyncExec,
          WorkerHolder.Listener listener,
          Worker worker,
          List<TaskAnnouncement> knownAnnouncements
      )
      {
        if (workerHolders.containsKey(worker.getHost())) {
          return workerHolders.get(worker.getHost()).apply(
              smileMapper,
              httpClient,
              config,
              workersSyncExec,
              listener,
              worker,
              knownAnnouncements
          );
        } else {
          throw new ISE("No WorkerHolder for [%s].", worker.getHost());
        }
      }
    };

    taskRunner.start();

    Task task1 = NoopTask.create("task-id-1", 0);
    Task task2 = NoopTask.create("task-id-2", 0);

    DiscoveryDruidNode druidNode = new DiscoveryDruidNode(
        new DruidNode("service", "host", false, 1234, null, true, false),
        NodeType.MIDDLE_MANAGER,
        ImmutableMap.of(
            WorkerNodeService.DISCOVERY_SERVICE_KEY, new WorkerNodeService("ip1", 2, "0", WorkerConfig.DEFAULT_CATEGORY)
        )
    );

    workerHolders.put(
        "host:1234",
        (mapper, httpClient, config, exec, listener, worker, knownAnnouncements) -> createWorkerHolder(
            mapper,
            httpClient,
            config,
            exec,
            listener,
            worker,
            knownAnnouncements,
            ImmutableList.of(),
            ImmutableMap.of(
                task1, ImmutableList.of(
                    TaskAnnouncement.create(
                        task1,
                        TaskStatus.running(task1.getId()),
                        TaskLocation.unknown()
                    ),
                    TaskAnnouncement.create(
                        task1,
                        TaskStatus.running(task1.getId()),
                        TaskLocation.create("host", 1234, 1235)
                    ),
                    TaskAnnouncement.create(
                        task1,
                        TaskStatus.success(task1.getId()),
                        TaskLocation.create("host", 1234, 1235)
                    )
                ),
                task2, ImmutableList.of(
                    TaskAnnouncement.create(
                        task2,
                        TaskStatus.running(task2.getId()),
                        TaskLocation.unknown()
                    ),
                    TaskAnnouncement.create(
                        task2,
                        TaskStatus.running(task2.getId()),
                        TaskLocation.create("host", 1234, 1235)
                    )
                )
            ),
            new AtomicInteger(),
            ImmutableSet.of()
        )
    );

    druidNodeDiscovery.listener.nodesAdded(
        ImmutableList.of(
            druidNode
        )
    );

    Future<TaskStatus> future1 = taskRunner.run(task1);
    Future<TaskStatus> future2 = taskRunner.run(task2);

    while (taskRunner.getPendingTasks().size() > 0) {
      Thread.sleep(100);
    }

    druidNodeDiscovery.listener.nodesRemoved(
        ImmutableList.of(
            druidNode
        )
    );

    workerHolders.put(
        "host:1234",
        (mapper, httpClient, config, exec, listener, worker, knownAnnouncements) -> createWorkerHolder(
            mapper,
            httpClient,
            config,
            exec,
            listener,
            worker,
            knownAnnouncements,
            ImmutableList.of(
                TaskAnnouncement.create(
                    task2,
                    TaskStatus.running(task2.getId()),
                    TaskLocation.create("host", 1234, 1235)
                ),
                TaskAnnouncement.create(
                    task2,
                    TaskStatus.success(task2.getId()),
                    TaskLocation.create("host", 1234, 1235)
                )

            ),
            ImmutableMap.of(),
            new AtomicInteger(),
            ImmutableSet.of()
        )
    );

    druidNodeDiscovery.listener.nodesAdded(
        ImmutableList.of(
            druidNode
        )
    );

    Assert.assertTrue(future1.get().isSuccess());
    Assert.assertTrue(future2.get().isSuccess());
  }

  @Test(timeout = 60_000L)
  public void testWorkerDisapperAndReappearAfterItsCleanup() throws Exception
  {
    TestDruidNodeDiscovery druidNodeDiscovery = new TestDruidNodeDiscovery();
    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);

    ConcurrentMap<String, CustomFunction> workerHolders = new ConcurrentHashMap<>();

    HttpRemoteTaskRunner taskRunner = new HttpRemoteTaskRunner(
        TestHelper.makeJsonMapper(),
        new HttpRemoteTaskRunnerConfig()
        {
          @Override
          public Period getTaskCleanupTimeout()
          {
            return Period.millis(1);
          }
        },
        EasyMock.createNiceMock(HttpClient.class),
        DSuppliers.of(new AtomicReference<>(DefaultWorkerBehaviorConfig.defaultConfig())),
        new NoopProvisioningStrategy<>(),
        druidNodeDiscoveryProvider,
        EasyMock.createNiceMock(TaskStorage.class),
        EasyMock.createNiceMock(CuratorFramework.class),
        new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null)
    )
    {
      @Override
      protected WorkerHolder createWorkerHolder(
          ObjectMapper smileMapper,
          HttpClient httpClient,
          HttpRemoteTaskRunnerConfig config,
          ScheduledExecutorService workersSyncExec,
          WorkerHolder.Listener listener,
          Worker worker,
          List<TaskAnnouncement> knownAnnouncements
      )
      {
        if (workerHolders.containsKey(worker.getHost())) {
          return workerHolders.get(worker.getHost()).apply(
              smileMapper,
              httpClient,
              config,
              workersSyncExec,
              listener,
              worker,
              knownAnnouncements
          );
        } else {
          throw new ISE("No WorkerHolder for [%s].", worker.getHost());
        }
      }
    };

    taskRunner.start();

    Task task1 = NoopTask.create("task-id-1", 0);
    Task task2 = NoopTask.create("task-id-2", 0);

    DiscoveryDruidNode druidNode = new DiscoveryDruidNode(
        new DruidNode("service", "host", false, 1234, null, true, false),
        NodeType.MIDDLE_MANAGER,
        ImmutableMap.of(WorkerNodeService.DISCOVERY_SERVICE_KEY, new WorkerNodeService("ip1", 2, "0", WorkerConfig.DEFAULT_CATEGORY))
    );

    workerHolders.put(
        "host:1234",
        (mapper, httpClient, config, exec, listener, worker, knownAnnouncements) -> createWorkerHolder(
            mapper,
            httpClient,
            config,
            exec,
            listener,
            worker,
            knownAnnouncements,
            ImmutableList.of(),
            ImmutableMap.of(
                task1, ImmutableList.of(
                    TaskAnnouncement.create(
                        task1,
                        TaskStatus.running(task1.getId()),
                        TaskLocation.unknown()
                    ),
                    TaskAnnouncement.create(
                        task1,
                        TaskStatus.running(task1.getId()),
                        TaskLocation.create("host", 1234, 1235)
                    )
                ),
                task2, ImmutableList.of(
                    TaskAnnouncement.create(
                        task2,
                        TaskStatus.running(task2.getId()),
                        TaskLocation.unknown()
                    ),
                    TaskAnnouncement.create(
                        task2,
                        TaskStatus.running(task2.getId()),
                        TaskLocation.create("host", 1234, 1235)
                    )
                )
            ),
            new AtomicInteger(),
            ImmutableSet.of()
        )
    );

    druidNodeDiscovery.listener.nodesAdded(
        ImmutableList.of(
            druidNode
        )
    );

    Future<TaskStatus> future1 = taskRunner.run(task1);
    Future<TaskStatus> future2 = taskRunner.run(task2);

    while (taskRunner.getPendingTasks().size() > 0) {
      Thread.sleep(100);
    }

    druidNodeDiscovery.listener.nodesRemoved(
        ImmutableList.of(
            druidNode
        )
    );

    Assert.assertTrue(future1.get().isFailure());
    Assert.assertTrue(future2.get().isFailure());

    AtomicInteger ticks = new AtomicInteger();
    Set<String> actualShutdowns = new ConcurrentHashSet<>();

    workerHolders.put(
        "host:1234",
        (mapper, httpClient, config, exec, listener, worker, knownAnnouncements) -> createWorkerHolder(
            mapper,
            httpClient,
            config,
            exec,
            listener,
            worker,
            knownAnnouncements,
            ImmutableList.of(
                TaskAnnouncement.create(
                    task1,
                    TaskStatus.success(task1.getId()),
                    TaskLocation.create("host", 1234, 1235)
                ),
                TaskAnnouncement.create(
                    task2,
                    TaskStatus.running(task2.getId()),
                    TaskLocation.create("host", 1234, 1235)
                )
            ),
            ImmutableMap.of(),
            ticks,
            actualShutdowns
        )
    );

    druidNodeDiscovery.listener.nodesAdded(
        ImmutableList.of(
            druidNode
        )
    );

    while (ticks.get() < 1) {
      Thread.sleep(100);
    }

    Assert.assertEquals(ImmutableSet.of(task2.getId()), actualShutdowns);
    Assert.assertTrue(taskRunner.run(task1).get().isFailure());
    Assert.assertTrue(taskRunner.run(task2).get().isFailure());
  }

  @Test(timeout = 60_000L)
  public void testMarkWorkersLazy() throws Exception
  {
    TestDruidNodeDiscovery druidNodeDiscovery = new TestDruidNodeDiscovery();
    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);

    Task task1 = NoopTask.create("task-id-1", 0);
    Task task2 = NoopTask.create("task-id-2", 0);

    ConcurrentMap<String, CustomFunction> workerHolders = new ConcurrentHashMap<>();

    HttpRemoteTaskRunner taskRunner = new HttpRemoteTaskRunner(
        TestHelper.makeJsonMapper(),
        new HttpRemoteTaskRunnerConfig()
        {
          @Override
          public int getPendingTasksRunnerNumThreads()
          {
            return 3;
          }
        },
        EasyMock.createNiceMock(HttpClient.class),
        DSuppliers.of(new AtomicReference<>(DefaultWorkerBehaviorConfig.defaultConfig())),
        new NoopProvisioningStrategy<>(),
        druidNodeDiscoveryProvider,
        EasyMock.createNiceMock(TaskStorage.class),
        EasyMock.createNiceMock(CuratorFramework.class),
        new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null)
    )
    {
      @Override
      protected WorkerHolder createWorkerHolder(
          ObjectMapper smileMapper,
          HttpClient httpClient,
          HttpRemoteTaskRunnerConfig config,
          ScheduledExecutorService workersSyncExec,
          WorkerHolder.Listener listener,
          Worker worker,
          List<TaskAnnouncement> knownAnnouncements
      )
      {
        if (workerHolders.containsKey(worker.getHost())) {
          return workerHolders.get(worker.getHost()).apply(
              smileMapper,
              httpClient,
              config,
              workersSyncExec,
              listener,
              worker,
              knownAnnouncements
          );
        } else {
          throw new ISE("No WorkerHolder for [%s].", worker.getHost());
        }
      }
    };

    taskRunner.start();

    AtomicInteger ticks = new AtomicInteger();

    DiscoveryDruidNode druidNode1 = new DiscoveryDruidNode(
        new DruidNode("service", "host1", false, 8080, null, true, false),
        NodeType.MIDDLE_MANAGER,
        ImmutableMap.of(
            WorkerNodeService.DISCOVERY_SERVICE_KEY, new WorkerNodeService("ip1", 1, "0", WorkerConfig.DEFAULT_CATEGORY)
        )
    );

    workerHolders.put(
        "host1:8080",
        (mapper, httpClient, config, exec, listener, worker, knownAnnouncements) -> createWorkerHolder(
            mapper,
            httpClient,
            config,
            exec,
            listener,
            worker,
            knownAnnouncements,
            ImmutableList.of(),
            ImmutableMap.of(
                task1, ImmutableList.of(
                    TaskAnnouncement.create(
                        task1,
                        TaskStatus.running(task1.getId()),
                        TaskLocation.unknown()
                    ),
                    TaskAnnouncement.create(
                        task1,
                        TaskStatus.running(task1.getId()),
                        TaskLocation.create("host1", 8080, -1)
                    )
                )
            ),
            ticks,
            ImmutableSet.of()
        )
    );

    druidNodeDiscovery.listener.nodesAdded(ImmutableList.of(druidNode1));

    taskRunner.run(task1);

    while (ticks.get() < 1) {
      Thread.sleep(100);
    }

    DiscoveryDruidNode druidNode2 = new DiscoveryDruidNode(
        new DruidNode("service", "host2", false, 8080, null, true, false),
        NodeType.MIDDLE_MANAGER,
        ImmutableMap.of(WorkerNodeService.DISCOVERY_SERVICE_KEY, new WorkerNodeService("ip2", 1, "0", WorkerConfig.DEFAULT_CATEGORY))
    );

    workerHolders.put(
        "host2:8080",
        (mapper, httpClient, config, exec, listener, worker, knownAnnouncements) -> createWorkerHolder(
            mapper,
            httpClient,
            config,
            exec,
            listener,
            worker,
            knownAnnouncements,
            ImmutableList.of(),
            ImmutableMap.of(task2, ImmutableList.of()),
            ticks,
            ImmutableSet.of()
        )
    );

    druidNodeDiscovery.listener.nodesAdded(ImmutableList.of(druidNode2));

    taskRunner.run(task2);

    while (ticks.get() < 2) {
      Thread.sleep(100);
    }

    DiscoveryDruidNode druidNode3 = new DiscoveryDruidNode(
        new DruidNode("service", "host3", false, 8080, null, true, false),
        NodeType.MIDDLE_MANAGER,
        ImmutableMap.of(WorkerNodeService.DISCOVERY_SERVICE_KEY, new WorkerNodeService("ip2", 1, "0", WorkerConfig.DEFAULT_CATEGORY))
    );

    workerHolders.put(
        "host3:8080",
        (mapper, httpClient, config, exec, listener, worker, knownAnnouncements) -> createWorkerHolder(
            mapper,
            httpClient,
            config,
            exec,
            listener,
            worker,
            knownAnnouncements,
            ImmutableList.of(),
            ImmutableMap.of(),
            new AtomicInteger(),
            ImmutableSet.of()
        )
    );

    druidNodeDiscovery.listener.nodesAdded(ImmutableList.of(druidNode3));

    Assert.assertEquals(task1.getId(), Iterables.getOnlyElement(taskRunner.getRunningTasks()).getTaskId());
    Assert.assertEquals(task2.getId(), Iterables.getOnlyElement(taskRunner.getPendingTasks()).getTaskId());

    Assert.assertEquals(
        "host3:8080",
        Iterables.getOnlyElement(taskRunner.markWorkersLazy(Predicates.alwaysTrue(), Integer.MAX_VALUE))
                 .getHost()
    );
  }

  /*
   * Task goes PENDING -> RUNNING -> SUCCESS and few more useless notifications in between.
   */
  @Test
  public void testTaskAddedOrUpdated1() throws Exception
  {
    Task task = NoopTask.create("task");
    List<Object> listenerNotificationsAccumulator = new ArrayList<>();
    HttpRemoteTaskRunner taskRunner = createTaskRunnerForTestTaskAddedOrUpdated(
        EasyMock.createStrictMock(TaskStorage.class),
        listenerNotificationsAccumulator
    );

    WorkerHolder workerHolder = EasyMock.createMock(WorkerHolder.class);
    EasyMock.expect(workerHolder.getWorker()).andReturn(new Worker("http", "worker", "127.0.0.1", 1, "v1", WorkerConfig.DEFAULT_CATEGORY)).anyTimes();
    workerHolder.setLastCompletedTaskTime(EasyMock.anyObject());
    workerHolder.resetContinuouslyFailedTasksCount();
    EasyMock.expect(workerHolder.getContinuouslyFailedTasksCount()).andReturn(0);
    EasyMock.replay(workerHolder);

    Future<TaskStatus> future = taskRunner.run(task);
    Assert.assertEquals(task.getId(), Iterables.getOnlyElement(taskRunner.getPendingTasks()).getTaskId());

    // RUNNING notification from worker
    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task,
        TaskStatus.running(task.getId()),
        TaskLocation.create("worker", 1000, 1001)
    ), workerHolder);
    Assert.assertEquals(task.getId(), Iterables.getOnlyElement(taskRunner.getRunningTasks()).getTaskId());

    // Another RUNNING notification from worker, notifying change in location
    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task,
        TaskStatus.running(task.getId()),
        TaskLocation.create("worker", 1, 2)
    ), workerHolder);
    Assert.assertEquals(task.getId(), Iterables.getOnlyElement(taskRunner.getRunningTasks()).getTaskId());

    // Redundant RUNNING notification from worker, ignored
    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task,
        TaskStatus.running(task.getId()),
        TaskLocation.create("worker", 1, 2)
    ), workerHolder);
    Assert.assertEquals(task.getId(), Iterables.getOnlyElement(taskRunner.getRunningTasks()).getTaskId());

    // Another "rogue-worker" reports running it, and gets asked to shutdown the task
    WorkerHolder rogueWorkerHolder = EasyMock.createMock(WorkerHolder.class);
    EasyMock.expect(rogueWorkerHolder.getWorker())
            .andReturn(new Worker("http", "rogue-worker", "127.0.0.1", 5, "v1", WorkerConfig.DEFAULT_CATEGORY))
            .anyTimes();
    rogueWorkerHolder.shutdownTask(task.getId());
    EasyMock.replay(rogueWorkerHolder);
    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task,
        TaskStatus.running(task.getId()),
        TaskLocation.create("rogue-worker", 1, 2)
    ), rogueWorkerHolder);
    Assert.assertEquals(task.getId(), Iterables.getOnlyElement(taskRunner.getRunningTasks()).getTaskId());
    EasyMock.verify(rogueWorkerHolder);

    // "rogue-worker" reports FAILURE for the task, ignored
    rogueWorkerHolder = EasyMock.createMock(WorkerHolder.class);
    EasyMock.expect(rogueWorkerHolder.getWorker())
            .andReturn(new Worker("http", "rogue-worker", "127.0.0.1", 5, "v1", WorkerConfig.DEFAULT_CATEGORY))
            .anyTimes();
    EasyMock.replay(rogueWorkerHolder);
    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task,
        TaskStatus.failure(task.getId()),
        TaskLocation.create("rogue-worker", 1, 2)
    ), rogueWorkerHolder);
    Assert.assertEquals(task.getId(), Iterables.getOnlyElement(taskRunner.getRunningTasks()).getTaskId());
    EasyMock.verify(rogueWorkerHolder);

    // workers sends SUCCESS notification, task is marked SUCCESS now.
    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task,
        TaskStatus.success(task.getId()),
        TaskLocation.create("worker", 1, 2)
    ), workerHolder);
    Assert.assertEquals(task.getId(), Iterables.getOnlyElement(taskRunner.getCompletedTasks()).getTaskId());
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // "rogue-worker" reports running it, and gets asked to shutdown the task
    rogueWorkerHolder = EasyMock.createMock(WorkerHolder.class);
    EasyMock.expect(rogueWorkerHolder.getWorker())
            .andReturn(new Worker("http", "rogue-worker", "127.0.0.1", 5, "v1", WorkerConfig.DEFAULT_CATEGORY))
            .anyTimes();
    rogueWorkerHolder.shutdownTask(task.getId());
    EasyMock.replay(rogueWorkerHolder);
    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task,
        TaskStatus.running(task.getId()),
        TaskLocation.create("rogue-worker", 1, 2)
    ), rogueWorkerHolder);
    Assert.assertEquals(task.getId(), Iterables.getOnlyElement(taskRunner.getCompletedTasks()).getTaskId());
    EasyMock.verify(rogueWorkerHolder);

    // "rogue-worker" reports FAILURE for the tasks, ignored
    rogueWorkerHolder = EasyMock.createMock(WorkerHolder.class);
    EasyMock.expect(rogueWorkerHolder.getWorker())
            .andReturn(new Worker("http", "rogue-worker", "127.0.0.1", 5, "v1", WorkerConfig.DEFAULT_CATEGORY))
            .anyTimes();
    EasyMock.replay(rogueWorkerHolder);
    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task,
        TaskStatus.failure(task.getId()),
        TaskLocation.create("rogue-worker", 1, 2)
    ), rogueWorkerHolder);
    Assert.assertEquals(task.getId(), Iterables.getOnlyElement(taskRunner.getCompletedTasks()).getTaskId());
    EasyMock.verify(rogueWorkerHolder);

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    EasyMock.verify(workerHolder);

    Assert.assertEquals(
        listenerNotificationsAccumulator,
        ImmutableList.of(
            ImmutableList.of(task.getId(), TaskLocation.create("worker", 1000, 1001)),
            ImmutableList.of(task.getId(), TaskLocation.create("worker", 1, 2)),
            ImmutableList.of(task.getId(), TaskStatus.success(task.getId()))
        )
    );
  }

  /*
   * Task goes from PENDING -> SUCCESS . Happens when TaskRunner is given task but a worker reported it being already
   * completed with SUCCESS.
   */
  @Test
  public void testTaskAddedOrUpdated2() throws Exception
  {
    Task task = NoopTask.create("task");
    List<Object> listenerNotificationsAccumulator = new ArrayList<>();
    HttpRemoteTaskRunner taskRunner = createTaskRunnerForTestTaskAddedOrUpdated(
        EasyMock.createStrictMock(TaskStorage.class),
        listenerNotificationsAccumulator
    );

    Worker worker = new Worker("http", "localhost", "127.0.0.1", 1, "v1", WorkerConfig.DEFAULT_CATEGORY);

    WorkerHolder workerHolder = EasyMock.createMock(WorkerHolder.class);
    EasyMock.expect(workerHolder.getWorker()).andReturn(worker).anyTimes();
    workerHolder.setLastCompletedTaskTime(EasyMock.anyObject());
    workerHolder.resetContinuouslyFailedTasksCount();
    EasyMock.expect(workerHolder.getContinuouslyFailedTasksCount()).andReturn(0);
    EasyMock.replay(workerHolder);

    Future<TaskStatus> future = taskRunner.run(task);
    Assert.assertEquals(task.getId(), Iterables.getOnlyElement(taskRunner.getPendingTasks()).getTaskId());

    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task,
        TaskStatus.success(task.getId()),
        TaskLocation.create("worker", 1, 2)
    ), workerHolder);
    Assert.assertEquals(task.getId(), Iterables.getOnlyElement(taskRunner.getCompletedTasks()).getTaskId());

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    EasyMock.verify(workerHolder);

    Assert.assertEquals(
        listenerNotificationsAccumulator,
        ImmutableList.of(
            ImmutableList.of(task.getId(), TaskLocation.create("worker", 1, 2)),
            ImmutableList.of(task.getId(), TaskStatus.success(task.getId()))
        )
    );
  }

  /*
   * Notifications received for tasks not known to TaskRunner maybe known to TaskStorage.
   * This could happen when TaskRunner starts and workers reports running/completed tasks on them.
   */
  @Test
  public void testTaskAddedOrUpdated3()
  {
    Task task1 = NoopTask.create("task1");
    Task task2 = NoopTask.create("task2");
    Task task3 = NoopTask.create("task3");
    Task task4 = NoopTask.create("task4");
    Task task5 = NoopTask.create("task5");
    Task task6 = NoopTask.create("task6");

    TaskStorage taskStorage = EasyMock.createMock(TaskStorage.class);
    EasyMock.expect(taskStorage.getStatus(task1.getId())).andReturn(Optional.of(TaskStatus.running(task1.getId())));
    EasyMock.expect(taskStorage.getStatus(task2.getId())).andReturn(Optional.of(TaskStatus.running(task2.getId())));
    EasyMock.expect(taskStorage.getStatus(task3.getId())).andReturn(Optional.of(TaskStatus.success(task3.getId())));
    EasyMock.expect(taskStorage.getStatus(task4.getId())).andReturn(Optional.of(TaskStatus.success(task4.getId())));
    EasyMock.expect(taskStorage.getStatus(task5.getId())).andReturn(Optional.absent());
    EasyMock.expect(taskStorage.getStatus(task6.getId())).andReturn(Optional.absent());
    EasyMock.replay(taskStorage);

    List<Object> listenerNotificationsAccumulator = new ArrayList<>();
    HttpRemoteTaskRunner taskRunner =
        createTaskRunnerForTestTaskAddedOrUpdated(taskStorage, listenerNotificationsAccumulator);

    Worker worker = new Worker("http", "localhost", "127.0.0.1", 1, "v1", WorkerConfig.DEFAULT_CATEGORY);

    WorkerHolder workerHolder = EasyMock.createMock(WorkerHolder.class);
    EasyMock.expect(workerHolder.getWorker()).andReturn(worker).anyTimes();
    workerHolder.setLastCompletedTaskTime(EasyMock.anyObject());
    workerHolder.resetContinuouslyFailedTasksCount();
    EasyMock.expect(workerHolder.getContinuouslyFailedTasksCount()).andReturn(0);
    workerHolder.shutdownTask(task3.getId());
    workerHolder.shutdownTask(task5.getId());
    EasyMock.replay(workerHolder);

    Assert.assertEquals(0, taskRunner.getKnownTasks().size());

    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task1,
        TaskStatus.running(task1.getId()),
        TaskLocation.create("worker", 1, 2)
    ), workerHolder);

    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task2,
        TaskStatus.success(task2.getId()),
        TaskLocation.create("worker", 3, 4)
    ), workerHolder);

    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task3,
        TaskStatus.running(task3.getId()),
        TaskLocation.create("worker", 5, 6)
    ), workerHolder);

    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task4,
        TaskStatus.success(task4.getId()),
        TaskLocation.create("worker", 7, 8)
    ), workerHolder);

    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task5,
        TaskStatus.running(task5.getId()),
        TaskLocation.create("worker", 9, 10)
    ), workerHolder);

    taskRunner.taskAddedOrUpdated(TaskAnnouncement.create(
        task6,
        TaskStatus.success(task6.getId()),
        TaskLocation.create("worker", 11, 12)
    ), workerHolder);

    EasyMock.verify(workerHolder, taskStorage);

    Assert.assertEquals(
        listenerNotificationsAccumulator,
        ImmutableList.of(
            ImmutableList.of(task1.getId(), TaskLocation.create("worker", 1, 2)),
            ImmutableList.of(task2.getId(), TaskLocation.create("worker", 3, 4)),
            ImmutableList.of(task2.getId(), TaskStatus.success(task2.getId()))
        )
    );
  }

  private HttpRemoteTaskRunner createTaskRunnerForTestTaskAddedOrUpdated(
      TaskStorage taskStorage,
      List<Object> listenerNotificationsAccumulator
  )
  {
    TestDruidNodeDiscovery druidNodeDiscovery = new TestDruidNodeDiscovery();
    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);

    HttpRemoteTaskRunner taskRunner = new HttpRemoteTaskRunner(
        TestHelper.makeJsonMapper(),
        new HttpRemoteTaskRunnerConfig()
        {
          @Override
          public int getPendingTasksRunnerNumThreads()
          {
            return 3;
          }
        },
        EasyMock.createNiceMock(HttpClient.class),
        DSuppliers.of(new AtomicReference<>(DefaultWorkerBehaviorConfig.defaultConfig())),
        new NoopProvisioningStrategy<>(),
        druidNodeDiscoveryProvider,
        taskStorage,
        EasyMock.createNiceMock(CuratorFramework.class),
        new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null)
    );

    taskRunner.start();

    if (listenerNotificationsAccumulator != null) {
      taskRunner.registerListener(
          new TaskRunnerListener()
          {
            @Override
            public String getListenerId()
            {
              return "test-listener";
            }

            @Override
            public void locationChanged(String taskId, TaskLocation newLocation)
            {
              listenerNotificationsAccumulator.add(ImmutableList.of(taskId, newLocation));
            }

            @Override
            public void statusChanged(String taskId, TaskStatus status)
            {
              listenerNotificationsAccumulator.add(ImmutableList.of(taskId, status));
            }
          },
          Execs.directExecutor()
      );
    }

    return taskRunner;
  }

  private static WorkerHolder createWorkerHolder(
      ObjectMapper smileMapper,
      HttpClient httpClient,
      HttpRemoteTaskRunnerConfig config,
      ScheduledExecutorService workersSyncExec,
      WorkerHolder.Listener listener,
      Worker worker,
      List<TaskAnnouncement> knownAnnouncements,

      // simulates task announcements received from worker on first sync call for the tasks that are already
      // running/completed on the worker.
      List<TaskAnnouncement> preExistingTaskAnnouncements,

      // defines behavior for what to do when a particular task is assigned
      Map<Task, List<TaskAnnouncement>> toBeAssignedTasks,

      // incremented on each runnable completion in workersSyncExec, useful for deterministically watching that some
      // work completed
      AtomicInteger ticks,

      // Updated each time a shutdown(taskId) call is received, useful for asserting that expected shutdowns indeed
      // happened.
      Set<String> actualShutdowns
  )
  {
    return new WorkerHolder(smileMapper, httpClient, config, workersSyncExec, listener, worker, knownAnnouncements)
    {
      private final String workerHost;
      private final int workerPort;

      {
        String hostAndPort = worker.getHost();
        int colonIndex = hostAndPort.indexOf(':');
        if (colonIndex == -1) {
          throw new IAE("Invalid host and port: [%s]", colonIndex);
        }
        workerHost = hostAndPort.substring(0, colonIndex);
        workerPort = Integer.parseInt(hostAndPort.substring(colonIndex + 1));
      }

      @Override
      public void start()
      {
        disabled.set(false);

        if (!preExistingTaskAnnouncements.isEmpty()) {
          workersSyncExec.execute(
              () -> {
                for (TaskAnnouncement announcement : preExistingTaskAnnouncements) {
                  tasksSnapshotRef.get().put(announcement.getTaskId(), announcement);
                  listener.taskAddedOrUpdated(announcement, this);
                }
                ticks.incrementAndGet();
              }
          );
        }
      }

      @Override
      public void stop()
      {

      }

      @Override
      public void waitForInitialization()
      {
      }

      @Override
      public boolean assignTask(Task task)
      {
        // artificial sleeps are introduced to simulate some latency.

        try {
          Thread.sleep(500);
        }
        catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }

        if (toImmutable().getCurrCapacityUsed() > worker.getCapacity()) {
          throw new ISE("Got assigned tasks more than capacity.");
        }

        final List<TaskAnnouncement> announcements;
        if (toBeAssignedTasks.containsKey(task)) {
          announcements = toBeAssignedTasks.get(task);
        } else {
          // no behavior specified for the task, so do default behavior of completing the task
          announcements = new ArrayList<>();
          announcements.add(
              TaskAnnouncement.create(
                  task,
                  TaskStatus.running(task.getId()),
                  TaskLocation.unknown()
              )
          );
          announcements.add(
              TaskAnnouncement.create(
                  task,
                  TaskStatus.running(task.getId()),
                  TaskLocation.create(workerHost, workerPort, -1)
              )
          );
          announcements.add(
              TaskAnnouncement.create(
                  task,
                  TaskStatus.success(task.getId()),
                  TaskLocation.create(workerHost, workerPort, -1)
              )
          );
        }

        workersSyncExec.execute(
            () -> {
              for (TaskAnnouncement announcement : announcements) {
                try {
                  Thread.sleep(100);
                }
                catch (InterruptedException ex) {
                  throw new RuntimeException(ex);
                }

                tasksSnapshotRef.get().put(announcement.getTaskId(), announcement);
                listener.taskAddedOrUpdated(announcement, this);
              }

              ticks.incrementAndGet();
            }
        );
        return true;
      }

      @Override
      public void shutdownTask(String taskId)
      {
        actualShutdowns.add(taskId);
      }
    };
  }

  private static class TestDruidNodeDiscovery implements DruidNodeDiscovery
  {
    Listener listener;

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      throw new UnsupportedOperationException("Not Implemented.");
    }

    @Override
    public void registerListener(Listener listener)
    {
      listener.nodesAdded(ImmutableList.of());
      listener.nodeViewInitialized();
      this.listener = listener;
    }
  }

  private interface CustomFunction
  {
    WorkerHolder apply(
        ObjectMapper smileMapper,
        HttpClient httpClient,
        HttpRemoteTaskRunnerConfig config,
        ScheduledExecutorService workersSyncExec,
        WorkerHolder.Listener listener,
        Worker worker,
        List<TaskAnnouncement> knownAnnouncements
    );
  }
}
