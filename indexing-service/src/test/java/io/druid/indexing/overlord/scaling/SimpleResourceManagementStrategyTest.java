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

package io.druid.indexing.overlord.scaling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import io.druid.common.guava.DSuppliers;
import io.druid.indexing.common.TestMergeTask;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.RemoteTaskRunnerWorkItem;
import io.druid.indexing.overlord.ZkWorker;
import io.druid.indexing.overlord.setup.WorkerSetupData;
import io.druid.indexing.worker.TaskAnnouncement;
import io.druid.indexing.worker.Worker;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
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
  private AutoScalingStrategy autoScalingStrategy;
  private Task testTask;
  private SimpleResourceManagementStrategy simpleResourceManagementStrategy;
  private AtomicReference<WorkerSetupData> workerSetupData;

  @Before
  public void setUp() throws Exception
  {
    autoScalingStrategy = EasyMock.createMock(AutoScalingStrategy.class);
    workerSetupData = new AtomicReference<>(
        new WorkerSetupData(
            "0", 0, 2, null, null, null
        )
    );

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
        Lists.<AggregatorFactory>newArrayList()
    );
    simpleResourceManagementStrategy = new SimpleResourceManagementStrategy(
        autoScalingStrategy,
        new SimpleResourceManagementConfig()
            .setWorkerIdleTimeout(new Period(0))
            .setMaxScalingDuration(new Period(1000))
            .setNumEventsToTrack(1)
            .setPendingTaskTimeout(new Period(0))
            .setWorkerVersion(""),
        DSuppliers.of(workerSetupData)
    );
  }

  @Test
  public void testSuccessfulProvision() throws Exception
  {
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList());
    EasyMock.expect(autoScalingStrategy.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("aNode"))
    );
    EasyMock.replay(autoScalingStrategy);

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
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

    EasyMock.verify(autoScalingStrategy);
  }

  @Test
  public void testSomethingProvisioning() throws Exception
  {
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList()).times(2);
    EasyMock.expect(autoScalingStrategy.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("fake"))
    );
    EasyMock.replay(autoScalingStrategy);

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
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
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
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

    EasyMock.verify(autoScalingStrategy);
  }

  @Test
  public void testProvisionAlert() throws Exception
  {
    ServiceEmitter emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall();
    EasyMock.replay(emitter);

    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList()).times(2);
    EasyMock.expect(autoScalingStrategy.terminateWithIds(EasyMock.<List<String>>anyObject()))
                    .andReturn(null);
    EasyMock.expect(autoScalingStrategy.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("fake"))
    );
    EasyMock.replay(autoScalingStrategy);

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
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
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
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

    EasyMock.verify(autoScalingStrategy);
    EasyMock.verify(emitter);
  }

  @Test
  public void testDoSuccessfulTerminate() throws Exception
  {
    workerSetupData.set(new WorkerSetupData("0", 0, 1, null, null, null));

    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList());
    EasyMock.expect(autoScalingStrategy.terminate(EasyMock.<List<String>>anyObject())).andReturn(
        new AutoScalingData(Lists.<String>newArrayList())
    );
    EasyMock.replay(autoScalingStrategy);

    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
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

    EasyMock.verify(autoScalingStrategy);
  }

  @Test
  public void testSomethingTerminating() throws Exception
  {
    workerSetupData.set(new WorkerSetupData("0", 0, 1, null, null, null));

    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip")).times(2);
    EasyMock.expect(autoScalingStrategy.terminate(EasyMock.<List<String>>anyObject())).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("ip"))
    );
    EasyMock.replay(autoScalingStrategy);

    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
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
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
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

    EasyMock.verify(autoScalingStrategy);
  }

  @Test
  public void testNoActionNeeded() throws Exception
  {
    EasyMock.reset(autoScalingStrategy);
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.replay(autoScalingStrategy);

    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create()),
            new TestZkWorker(NoopTask.create())
        )
    );

    Assert.assertFalse(terminatedSomething);
    EasyMock.verify(autoScalingStrategy);

    EasyMock.reset(autoScalingStrategy);
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.replay(autoScalingStrategy);

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create()),
            new TestZkWorker(NoopTask.create())
        )
    );

    Assert.assertFalse(provisionedSomething);
    EasyMock.verify(autoScalingStrategy);
  }

  @Test
  public void testMinCountIncrease() throws Exception
  {
    // Don't terminate anything
    EasyMock.reset(autoScalingStrategy);
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.replay(autoScalingStrategy);
    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create(), "h1", "i1", "0")
        )
    );
    Assert.assertFalse(terminatedSomething);
    EasyMock.verify(autoScalingStrategy);

    // Don't provision anything
    EasyMock.reset(autoScalingStrategy);
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.replay(autoScalingStrategy);
    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create())
        )
    );
    Assert.assertFalse(provisionedSomething);
    EasyMock.verify(autoScalingStrategy);

    // Increase minNumWorkers
    workerSetupData.set(new WorkerSetupData("0", 3, 5, null, null, null));

    // Should provision two new workers
    EasyMock.reset(autoScalingStrategy);
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.expect(autoScalingStrategy.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("h3"))
    );
    EasyMock.expect(autoScalingStrategy.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("h4"))
    );
    EasyMock.replay(autoScalingStrategy);
    provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create(), "h1", "i1", "0")
        )
    );
    Assert.assertTrue(provisionedSomething);
    EasyMock.verify(autoScalingStrategy);
  }

  @Test
  public void testMinVersionIncrease() throws Exception
  {
    // Don't terminate anything
    EasyMock.reset(autoScalingStrategy);
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.replay(autoScalingStrategy);
    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create(), "h1", "i1", "0"),
            new TestZkWorker(NoopTask.create(), "h1", "i2", "0")
        )
    );
    Assert.assertFalse(terminatedSomething);
    EasyMock.verify(autoScalingStrategy);

    // Don't provision anything
    EasyMock.reset(autoScalingStrategy);
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.replay(autoScalingStrategy);
    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create()),
            new TestZkWorker(NoopTask.create())
        )
    );
    Assert.assertFalse(provisionedSomething);
    EasyMock.verify(autoScalingStrategy);

    // Increase minVersion
    workerSetupData.set(new WorkerSetupData("1", 0, 2, null, null, null));

    // Provision two new workers
    EasyMock.reset(autoScalingStrategy);
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip"));
    EasyMock.expect(autoScalingStrategy.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("h3"))
    );
    EasyMock.expect(autoScalingStrategy.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("h4"))
    );
    EasyMock.replay(autoScalingStrategy);
    provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(NoopTask.create(), "h1", "i1", "0"),
            new TestZkWorker(NoopTask.create(), "h2", "i2", "0")
        )
    );
    Assert.assertTrue(provisionedSomething);
    EasyMock.verify(autoScalingStrategy);

    // Terminate old workers
    EasyMock.reset(autoScalingStrategy);
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(ImmutableList.of("i1", "i2", "i3", "i4"))).andReturn(
        ImmutableList.of("h1", "h2", "h3", "h4")
    );
    EasyMock.expect(autoScalingStrategy.terminate(ImmutableList.of("i1", "i2"))).andReturn(
        new AutoScalingData(ImmutableList.of("h1", "h2"))
    );
    EasyMock.replay(autoScalingStrategy);
    terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(null, "h1", "i1", "0"),
            new TestZkWorker(null, "h2", "i2", "0"),
            new TestZkWorker(NoopTask.create(), "h3", "i3", "1"),
            new TestZkWorker(NoopTask.create(), "h4", "i4", "1")
        )
    );
    Assert.assertTrue(terminatedSomething);
    EasyMock.verify(autoScalingStrategy);
  }

  @Test
  public void testNullWorkerSetupData() throws Exception
  {
    workerSetupData.set(null);
    EasyMock.replay(autoScalingStrategy);

    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(null)
        )
    );

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<RemoteTaskRunnerWorkItem>asList(
            new RemoteTaskRunnerWorkItem(testTask.getId(), null, null).withQueueInsertionTime(new DateTime())
        ),
        Arrays.<ZkWorker>asList(
            new TestZkWorker(null)
        )
    );

    Assert.assertFalse(terminatedSomething);
    Assert.assertFalse(provisionedSomething);

    EasyMock.verify(autoScalingStrategy);
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
