/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord.autoscaling;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import io.druid.common.guava.DSuppliers;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TestMergeTask;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.RemoteTaskRunnerWorkItem;
import io.druid.indexing.overlord.ZkWorker;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.worker.TaskAnnouncement;
import io.druid.indexing.worker.Worker;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexSpec;
import io.druid.timeline.DataSegment;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class SimpleResourceManagementStrategyTest
{
  private AutoScaler autoScaler;
  private Task testTask;
  private SimpleResourceManagementConfig simpleResourceManagementConfig;
  private SimpleResourceManagementStrategy simpleResourceManagementStrategy;
  private AtomicReference<WorkerBehaviorConfig> workerConfig;

  @Before
  public void setUp() throws Exception
  {
    autoScaler = EasyMock.createMock(AutoScaler.class);

    final IndexSpec indexSpec = new IndexSpec();

    testTask = new TestMergeTask(
        "task1",
        "dummyDs",
        Lists.<DataSegment>newArrayList(
            new DataSegment(
                "dummyDs",
                new Interval("2012-01-01/2012-01-02"),
                new DateTime().toString(),
                null,
                null,
                null,
                null,
                0,
                0
            )
        ),
        Lists.<AggregatorFactory>newArrayList(),
        indexSpec
    );

    simpleResourceManagementConfig = new SimpleResourceManagementConfig()
        .setWorkerIdleTimeout(new Period(0))
        .setMaxScalingDuration(new Period(1000))
        .setNumEventsToTrack(1)
        .setPendingTaskTimeout(new Period(0))
        .setWorkerVersion("");

    workerConfig = new AtomicReference<>(
        new WorkerBehaviorConfig(
            null,
            autoScaler
        )
    );

    simpleResourceManagementStrategy = new SimpleResourceManagementStrategy(
        simpleResourceManagementConfig,
        DSuppliers.of(workerConfig)
    );
  }

  @Test
  public void testSuccessfulProvision() throws Exception
  {
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList());
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("aNode"))
    );
    EasyMock.replay(autoScaler);

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(testTask)
        )
    );

    Assert.assertTrue(provisionedSomething);
    Assert.assertTrue(simpleResourceManagementStrategy.getStats().toList().size() == 1);
    Assert.assertTrue(
        simpleResourceManagementStrategy.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.PROVISION
    );

    EasyMock.verify(autoScaler);
  }

  @Test
  public void testSomethingProvisioning() throws Exception
  {
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0).times(2);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2).times(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList()).times(2);
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("fake"))
    );
    EasyMock.replay(autoScaler);

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(testTask)
        )
    );

    Assert.assertTrue(provisionedSomething);
    Assert.assertTrue(simpleResourceManagementStrategy.getStats().toList().size() == 1);
    DateTime createdTime = simpleResourceManagementStrategy.getStats().toList().get(0).getTimestamp();
    Assert.assertTrue(
        simpleResourceManagementStrategy.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.PROVISION
    );

    provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(testTask)
        )
    );

    Assert.assertFalse(provisionedSomething);
    Assert.assertTrue(
        simpleResourceManagementStrategy.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.PROVISION
    );
    DateTime anotherCreatedTime = simpleResourceManagementStrategy.getStats().toList().get(0).getTimestamp();
    Assert.assertTrue(
        createdTime.equals(anotherCreatedTime)
    );

    EasyMock.verify(autoScaler);
  }

  @Test
  public void testProvisionAlert() throws Exception
  {
    ServiceEmitter emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall();
    EasyMock.replay(emitter);

    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0).times(2);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2).times(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList()).times(2);
    EasyMock.expect(autoScaler.terminateWithIds(EasyMock.<List<String>>anyObject()))
            .andReturn(null);
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("fake"))
    );
    EasyMock.replay(autoScaler);

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(testTask)
        )
    );

    Assert.assertTrue(provisionedSomething);
    Assert.assertTrue(simpleResourceManagementStrategy.getStats().toList().size() == 1);
    DateTime createdTime = simpleResourceManagementStrategy.getStats().toList().get(0).getTimestamp();
    Assert.assertTrue(
        simpleResourceManagementStrategy.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.PROVISION
    );

    Thread.sleep(2000);

    provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(testTask)
        )
    );

    Assert.assertFalse(provisionedSomething);
    Assert.assertTrue(
        simpleResourceManagementStrategy.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.PROVISION
    );
    DateTime anotherCreatedTime = simpleResourceManagementStrategy.getStats().toList().get(0).getTimestamp();
    Assert.assertTrue(
        createdTime.equals(anotherCreatedTime)
    );

    EasyMock.verify(autoScaler);
    EasyMock.verify(emitter);
  }

  @Test
  public void testDoSuccessfulTerminate() throws Exception
  {
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(1);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList());
    EasyMock.expect(autoScaler.terminate(EasyMock.<List<String>>anyObject())).andReturn(
        new AutoScalingData(Lists.<String>newArrayList())
    );
    EasyMock.replay(autoScaler);

    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(null)
        )
    );

    Assert.assertTrue(terminatedSomething);
    Assert.assertTrue(simpleResourceManagementStrategy.getStats().toList().size() == 1);
    Assert.assertTrue(
        simpleResourceManagementStrategy.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.TERMINATE
    );

    EasyMock.verify(autoScaler);
  }

  @Test
  public void testSomethingTerminating() throws Exception
  {
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0).times(2);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(1).times(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip")).times(2);
    EasyMock.expect(autoScaler.terminate(EasyMock.<List<String>>anyObject())).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("ip"))
    );
    EasyMock.replay(autoScaler);

    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(null)
        )
    );

    Assert.assertTrue(terminatedSomething);
    Assert.assertTrue(simpleResourceManagementStrategy.getStats().toList().size() == 1);
    Assert.assertTrue(
        simpleResourceManagementStrategy.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.TERMINATE
    );

    terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(null)
        )
    );

    Assert.assertFalse(terminatedSomething);
    Assert.assertTrue(simpleResourceManagementStrategy.getStats().toList().size() == 1);
    Assert.assertTrue(
        simpleResourceManagementStrategy.getStats().toList().get(0).getEvent() == ScalingStats.EVENT.TERMINATE
    );

    EasyMock.verify(autoScaler);
  }

  @Test
  public void testNoActionNeeded() throws Exception
  {
    EasyMock.reset(autoScaler);
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.replay(autoScaler);

    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create()),
            new TestZkWorker(NoopTask.create())
        )
    );

    Assert.assertFalse(terminatedSomething);
    EasyMock.verify(autoScaler);

    EasyMock.reset(autoScaler);
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.replay(autoScaler);

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create()),
            new TestZkWorker(NoopTask.create())
        )
    );

    Assert.assertFalse(provisionedSomething);
    EasyMock.verify(autoScaler);
  }

  @Test
  public void testMinCountIncrease() throws Exception
  {
    // Don't terminate anything
    EasyMock.reset(autoScaler);
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.replay(autoScaler);
    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create(), "h1", "i1", "0")
        )
    );
    Assert.assertFalse(terminatedSomething);
    EasyMock.verify(autoScaler);

    // Don't provision anything
    EasyMock.reset(autoScaler);
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(2);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.replay(autoScaler);
    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create())
        )
    );
    Assert.assertFalse(provisionedSomething);
    EasyMock.verify(autoScaler);

    EasyMock.reset(autoScaler);
    // Increase minNumWorkers
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(3);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(5);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("h3"))
    );
    // Should provision two new workers
    EasyMock.expect(autoScaler.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("h4"))
    );
    EasyMock.replay(autoScaler);
    provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create(), "h1", "i1", "0")
        )
    );
    Assert.assertTrue(provisionedSomething);
    EasyMock.verify(autoScaler);
  }

  @Test
  public void testNullWorkerConfig() throws Exception
  {
    workerConfig.set(null);
    EasyMock.replay(autoScaler);

    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(null)
        )
    );

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(null)
        )
    );

    Assert.assertFalse(terminatedSomething);
    Assert.assertFalse(provisionedSomething);

    EasyMock.verify(autoScaler);
  }

  private static class TestZkWorker extends ZkWorker
  {
    private final Task testTask;

    public TestZkWorker(
        Task testTask
    )
    {
      this(testTask, "host", "ip", "0");
    }

    public TestZkWorker(
        Task testTask,
        String host,
        String ip,
        String version
    )
    {
      super(new Worker(host, ip, 3, version), null, new DefaultObjectMapper());

      this.testTask = testTask;
    }

    @Override
    public Map<String, TaskAnnouncement> getRunningTasks()
    {
      if (testTask == null) {
        return Maps.newHashMap();
      }
      return ImmutableMap.of(testTask.getId(), TaskAnnouncement.create(testTask, TaskStatus.running(testTask.getId())));
    }
  }
}
