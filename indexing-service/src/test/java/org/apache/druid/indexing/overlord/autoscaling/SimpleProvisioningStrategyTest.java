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

package org.apache.druid.indexing.overlord.autoscaling;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.guava.DSuppliers;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TestTasks;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunner;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class SimpleProvisioningStrategyTest
{
  private AutoScaler autoScaler;
  private Task testTask;
  private SimpleWorkerProvisioningStrategy strategy;
  private AtomicReference<WorkerBehaviorConfig> workerConfig;
  private ScheduledExecutorService executorService = Execs.scheduledSingleThreaded("test service");

  @Before
  public void setUp()
  {
    autoScaler = EasyMock.createMock(AutoScaler.class);
    testTask = TestTasks.immediateSuccess("task1");

    final SimpleWorkerProvisioningConfig simpleWorkerProvisioningConfig = new SimpleWorkerProvisioningConfig()
        .setWorkerIdleTimeout(new Period(0))
        .setMaxScalingDuration(new Period(1000))
        .setNumEventsToTrack(1)
        .setPendingTaskTimeout(new Period(0))
        .setWorkerVersion("");

    final ProvisioningSchedulerConfig schedulerConfig = new ProvisioningSchedulerConfig();

    workerConfig = new AtomicReference<>(
        new DefaultWorkerBehaviorConfig(
            null,
            autoScaler
        )
    );

    strategy = new SimpleWorkerProvisioningStrategy(
        simpleWorkerProvisioningConfig,
        DSuppliers.of(workerConfig),
        schedulerConfig,
        new Supplier<>()
        {
          @Override
          public ScheduledExecutorService get()
          {
            return executorService;
          }
        }
    );
  }

  @After
  public void tearDown()
  {
    executorService.shutdownNow();
  }

  @Test
  public void testSuccessfulProvision()
  {
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(new ArrayList<String>());
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("aNode"))
    );
    HttpRemoteTaskRunner runner = EasyMock.createMock(HttpRemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        (Collection) Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    );
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            workerWithTask(testTask)
        )
    );
    EasyMock.replay(runner);
    EasyMock.replay(autoScaler);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();

    Assert.assertTrue(provisionedSomething);
    Assert.assertTrue(provisioner.getStats().toList().size() == 1);
    Assert.assertTrue(
        provisioner.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.PROVISION
    );

    EasyMock.verify(autoScaler);
    EasyMock.verify(runner);
  }

  @Test
  public void testSomethingProvisioning()
  {
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0).times(2);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2).times(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(new ArrayList<String>()).times(2);
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("fake"))
    );
    HttpRemoteTaskRunner runner = EasyMock.createMock(HttpRemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        (Collection) Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            workerWithTask(testTask)
        )
    ).times(2);
    EasyMock.replay(runner);
    EasyMock.replay(autoScaler);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();

    Assert.assertTrue(provisionedSomething);
    Assert.assertTrue(provisioner.getStats().toList().size() == 1);
    DateTime createdTime = provisioner.getStats().toList().get(0).getTimestamp();
    Assert.assertTrue(
        provisioner.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.PROVISION
    );

    provisionedSomething = provisioner.doProvision();

    Assert.assertFalse(provisionedSomething);
    Assert.assertTrue(
        provisioner.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.PROVISION
    );
    DateTime anotherCreatedTime = provisioner.getStats().toList().get(0).getTimestamp();
    Assert.assertTrue(
        createdTime.equals(anotherCreatedTime)
    );

    EasyMock.verify(autoScaler);
    EasyMock.verify(runner);
  }

  @Test
  public void testProvisionAlert() throws Exception
  {
    ServiceEmitter emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(emitter);

    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0).times(2);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2).times(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(new ArrayList<String>()).times(2);
    EasyMock.expect(autoScaler.terminateWithIds(EasyMock.anyObject()))
            .andReturn(null);
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("fake"))
    );
    EasyMock.replay(autoScaler);
    HttpRemoteTaskRunner runner = EasyMock.createMock(HttpRemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        (Collection) Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            workerWithTask(testTask)
        )
    ).times(2);
    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();

    Assert.assertTrue(provisionedSomething);
    Assert.assertTrue(provisioner.getStats().toList().size() == 1);
    DateTime createdTime = provisioner.getStats().toList().get(0).getTimestamp();
    Assert.assertTrue(
        provisioner.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.PROVISION
    );

    Thread.sleep(2000);

    provisionedSomething = provisioner.doProvision();

    Assert.assertFalse(provisionedSomething);
    Assert.assertTrue(
        provisioner.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.PROVISION
    );
    DateTime anotherCreatedTime = provisioner.getStats().toList().get(0).getTimestamp();
    Assert.assertTrue(
        createdTime.equals(anotherCreatedTime)
    );

    EasyMock.verify(autoScaler);
    EasyMock.verify(emitter);
    EasyMock.verify(runner);
  }

  @Test
  public void testDoSuccessfulTerminate()
  {
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(1);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(new ArrayList<String>());
    EasyMock.expect(autoScaler.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(new ArrayList<>())
    );
    EasyMock.replay(autoScaler);
    HttpRemoteTaskRunner runner = EasyMock.createMock(HttpRemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        (Collection) Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            workerWithTask(testTask)
        )
    ).times(2);
    EasyMock.expect(runner.markWorkersLazy(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(Collections.singletonList(workerWithTask(testTask).getWorker()));
    EasyMock.expect(runner.getLazyWorkers()).andReturn(new ArrayList<>());
    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean terminatedSomething = provisioner.doTerminate();

    Assert.assertTrue(terminatedSomething);
    Assert.assertTrue(provisioner.getStats().toList().size() == 1);
    Assert.assertTrue(
        provisioner.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.TERMINATE
    );

    EasyMock.verify(autoScaler);
  }

  @Test
  public void testSomethingTerminating()
  {
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0).times(2);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(1).times(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip")).times(2);
    EasyMock.expect(autoScaler.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(Collections.singletonList("ip"))
    );
    EasyMock.replay(autoScaler);

    HttpRemoteTaskRunner runner = EasyMock.createMock(HttpRemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        (Collection) Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            workerWithTask(testTask)
        )
    ).times(2);
    EasyMock.expect(runner.getLazyWorkers()).andReturn(new ArrayList<>()).times(2);
    EasyMock.expect(runner.markWorkersLazy(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(Collections.singletonList(workerWithTask(testTask).getWorker()));
    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean terminatedSomething = provisioner.doTerminate();

    Assert.assertTrue(terminatedSomething);
    Assert.assertTrue(provisioner.getStats().toList().size() == 1);
    Assert.assertTrue(
        provisioner.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.TERMINATE
    );

    terminatedSomething = provisioner.doTerminate();

    Assert.assertFalse(terminatedSomething);
    Assert.assertTrue(provisioner.getStats().toList().size() == 1);
    Assert.assertTrue(
        provisioner.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.TERMINATE
    );

    EasyMock.verify(autoScaler);
    EasyMock.verify(runner);
  }

  @Test
  public void testNoActionNeeded()
  {
    EasyMock.reset(autoScaler);
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip"));
    EasyMock.replay(autoScaler);

    HttpRemoteTaskRunner runner = EasyMock.createMock(HttpRemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        (Collection) Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            workerWithTask(NoopTask.create()),
            workerWithTask(NoopTask.create())
        )
    ).times(2);
    EasyMock.expect(runner.getLazyWorkers()).andReturn(new ArrayList<>());
    EasyMock.expect(runner.markWorkersLazy(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(Collections.emptyList());
    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean terminatedSomething = provisioner.doTerminate();

    Assert.assertFalse(terminatedSomething);
    EasyMock.verify(autoScaler);

    EasyMock.reset(autoScaler);
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip"));
    EasyMock.replay(autoScaler);

    boolean provisionedSomething = provisioner.doProvision();

    Assert.assertFalse(provisionedSomething);
    EasyMock.verify(autoScaler);
    EasyMock.verify(runner);
  }

  @Test
  public void testMinCountIncrease()
  {
    // Don't terminate anything
    EasyMock.reset(autoScaler);
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip"));
    EasyMock.replay(autoScaler);
    HttpRemoteTaskRunner runner = EasyMock.createMock(HttpRemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        Collections.emptyList()
    ).times(3);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            workerWithTask(NoopTask.create(), "http", "h1", "i1", "0")
        )
    ).times(3);
    EasyMock.expect(runner.getLazyWorkers()).andReturn(new ArrayList<>());
    EasyMock.expect(runner.markWorkersLazy(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(Collections.emptyList());
    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean terminatedSomething = provisioner.doTerminate();
    Assert.assertFalse(terminatedSomething);
    EasyMock.verify(autoScaler);

    // Don't provision anything
    EasyMock.reset(autoScaler);
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip"));
    EasyMock.replay(autoScaler);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertFalse(provisionedSomething);
    EasyMock.verify(autoScaler);

    EasyMock.reset(autoScaler);
    // Increase minNumWorkers
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(3);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(5);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip"));
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("h3"))
    );
    // Should provision two new workers
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("h4"))
    );
    EasyMock.replay(autoScaler);
    provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    EasyMock.verify(autoScaler);
    EasyMock.verify(runner);
  }

  @Test
  public void testNullWorkerConfig()
  {
    workerConfig.set(null);
    EasyMock.replay(autoScaler);

    HttpRemoteTaskRunner runner = EasyMock.createMock(HttpRemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        (Collection) Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            workerWithTask(null)
        )
    ).times(1);
    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean terminatedSomething = provisioner.doTerminate();

    boolean provisionedSomething = provisioner.doProvision();

    Assert.assertFalse(terminatedSomething);
    Assert.assertFalse(provisionedSomething);

    EasyMock.verify(autoScaler);
    EasyMock.verify(runner);
  }

  private static ImmutableWorkerInfo workerWithTask(Task task)
  {
    return workerWithTask(task, "http", "host", "ip", "0");
  }

  private static ImmutableWorkerInfo workerWithTask(
      Task task,
      String scheme,
      String host,
      String ip,
      String version
  )
  {
    Worker worker = new Worker(scheme, host, ip, 3, version, WorkerConfig.DEFAULT_CATEGORY);
    Map<String, TaskAnnouncement> running = task == null
        ? new HashMap<>()
        : ImmutableMap.of(
            task.getId(),
            TaskAnnouncement.create(task, TaskStatus.running(task.getId()), TaskLocation.unknown())
        );
    return ImmutableWorkerInfo.fromWorkerAnnouncements(worker, running, DateTimes.EPOCH, null);
  }
}
