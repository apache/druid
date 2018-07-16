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

package io.druid.indexing.overlord.autoscaling;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.DSuppliers;
import io.druid.indexer.TaskLocation;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.common.TestTasks;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.RemoteTaskRunner;
import io.druid.indexing.overlord.RemoteTaskRunnerWorkItem;
import io.druid.indexing.overlord.ZkWorker;
import io.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.worker.TaskAnnouncement;
import io.druid.indexing.worker.Worker;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceEventBuilder;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
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
        new Supplier<ScheduledExecutorService>()
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
            .andReturn(Lists.<String>newArrayList());
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Lists.newArrayList("aNode"))
    );
    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    );
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new TestZkWorker(testTask).toImmutable()
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
            .andReturn(Lists.<String>newArrayList()).times(2);
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Lists.newArrayList("fake"))
    );
    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new TestZkWorker(testTask).toImmutable()
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
            .andReturn(Lists.<String>newArrayList()).times(2);
    EasyMock.expect(autoScaler.terminateWithIds(EasyMock.anyObject()))
            .andReturn(null);
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Lists.newArrayList("fake"))
    );
    EasyMock.replay(autoScaler);
    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new TestZkWorker(testTask).toImmutable()
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
            .andReturn(Lists.<String>newArrayList());
    EasyMock.expect(autoScaler.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(Lists.newArrayList())
    );
    EasyMock.replay(autoScaler);
    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new TestZkWorker(testTask).toImmutable()
        )
    ).times(2);
    EasyMock.expect(runner.markWorkersLazy(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(Collections.singletonList(new TestZkWorker(testTask).getWorker()));
    EasyMock.expect(runner.getLazyWorkers()).andReturn(Lists.newArrayList());
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
            .andReturn(Lists.newArrayList("ip")).times(2);
    EasyMock.expect(autoScaler.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(Lists.newArrayList("ip"))
    );
    EasyMock.replay(autoScaler);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new TestZkWorker(testTask).toImmutable()
        )
    ).times(2);
    EasyMock.expect(runner.getLazyWorkers()).andReturn(Lists.newArrayList()).times(2);
    EasyMock.expect(runner.markWorkersLazy(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(Collections.singletonList(new TestZkWorker(testTask).getWorker()));
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
            .andReturn(Lists.newArrayList("ip"));
    EasyMock.replay(autoScaler);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new TestZkWorker(NoopTask.create()).toImmutable(),
            new TestZkWorker(NoopTask.create()).toImmutable()
        )
    ).times(2);
    EasyMock.expect(runner.getLazyWorkers()).andReturn(Lists.newArrayList());
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
            .andReturn(Lists.newArrayList("ip"));
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
            .andReturn(Lists.newArrayList("ip"));
    EasyMock.replay(autoScaler);
    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        Collections.emptyList()
    ).times(3);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new TestZkWorker(NoopTask.create(), "http", "h1", "i1", "0").toImmutable()
        )
    ).times(3);
    EasyMock.expect(runner.getLazyWorkers()).andReturn(Lists.newArrayList());
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
            .andReturn(Lists.newArrayList("ip"));
    EasyMock.replay(autoScaler);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertFalse(provisionedSomething);
    EasyMock.verify(autoScaler);

    EasyMock.reset(autoScaler);
    // Increase minNumWorkers
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(3);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(5);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Lists.newArrayList("ip"));
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Lists.newArrayList("h3"))
    );
    // Should provision two new workers
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Lists.newArrayList("h4"))
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

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTasks()).andReturn(
        Collections.singletonList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), testTask.getType(), null, null, testTask.getDataSource())
                .withQueueInsertionTime(DateTimes.nowUtc())
        )
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new TestZkWorker(null).toImmutable()
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

  private static class TestZkWorker extends ZkWorker
  {
    private final Task testTask;

    public TestZkWorker(
        Task testTask
    )
    {
      this(testTask, "http", "host", "ip", "0");
    }

    public TestZkWorker(
        Task testTask,
        String scheme,
        String host,
        String ip,
        String version
    )
    {
      super(new Worker(scheme, host, ip, 3, version), null, new DefaultObjectMapper());

      this.testTask = testTask;
    }

    @Override
    public Map<String, TaskAnnouncement> getRunningTasks()
    {
      if (testTask == null) {
        return Maps.newHashMap();
      }
      return ImmutableMap.of(
          testTask.getId(),
          TaskAnnouncement.create(
              testTask,
              TaskStatus.running(testTask.getId()),
              TaskLocation.unknown()
          )
      );
    }
  }
}
