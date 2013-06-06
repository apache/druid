/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexing.coordinator.scaling;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.indexing.TestTask;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.coordinator.TaskRunnerWorkItem;
import com.metamx.druid.indexing.coordinator.ZkWorker;
import com.metamx.druid.indexing.coordinator.setup.WorkerSetupData;
import com.metamx.druid.indexing.worker.Worker;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
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
    workerSetupData = new AtomicReference<WorkerSetupData>(
        new WorkerSetupData(
            "0", 0, 2, null, null
        )
    );

    testTask = new TestTask(
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
        TaskStatus.success("task1")
    );
    simpleResourceManagementStrategy = new SimpleResourceManagementStrategy(
        autoScalingStrategy,
        new SimpleResourceManagmentConfig()
        {
          @Override
          public int getMaxWorkerIdleTimeMillisBeforeDeletion()
          {
            return 0;
          }

          @Override
          public Duration getMaxScalingDuration()
          {
            return new Duration(1000);
          }

          @Override
          public int getNumEventsToTrack()
          {
            return 1;
          }

          @Override
          public Duration getMaxPendingTaskDuration()
          {
            return new Duration(0);
          }
        },
        workerSetupData
    );
  }

  @Test
  public void testSuccessfulProvision() throws Exception
  {
    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList());
    EasyMock.expect(autoScalingStrategy.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList(), Lists.newArrayList())
    );
    EasyMock.replay(autoScalingStrategy);

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<TaskRunnerWorkItem>asList(
            new TaskRunnerWorkItem(testTask, null, null, null).withQueueInsertionTime(new DateTime())
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
        new AutoScalingData(Lists.<String>newArrayList("fake"), Lists.newArrayList("faker"))
    );
    EasyMock.replay(autoScalingStrategy);

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<TaskRunnerWorkItem>asList(
            new TaskRunnerWorkItem(testTask, null, null, null).withQueueInsertionTime(new DateTime())
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
        Arrays.<TaskRunnerWorkItem>asList(
            new TaskRunnerWorkItem(testTask, null, null, null).withQueueInsertionTime(new DateTime())
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
    EasyMock.expect(autoScalingStrategy.idToIpLookup(EasyMock.<List<String>>anyObject()))
                .andReturn(Lists.<String>newArrayList());
    EasyMock.expect(autoScalingStrategy.terminate(EasyMock.<List<String>>anyObject()))
                    .andReturn(null);
    EasyMock.expect(autoScalingStrategy.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("fake"), Lists.newArrayList("faker"))
    );
    EasyMock.replay(autoScalingStrategy);

    boolean provisionedSomething = simpleResourceManagementStrategy.doProvision(
        Arrays.<TaskRunnerWorkItem>asList(
            new TaskRunnerWorkItem(testTask, null, null, null).withQueueInsertionTime(new DateTime())
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
        Arrays.<TaskRunnerWorkItem>asList(
            new TaskRunnerWorkItem(testTask, null, null, null).withQueueInsertionTime(new DateTime())
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
    workerSetupData.set(new WorkerSetupData("0", 0, 1, null, null));

    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList());
    EasyMock.expect(autoScalingStrategy.terminate(EasyMock.<List<String>>anyObject())).andReturn(
        new AutoScalingData(Lists.<String>newArrayList(), Lists.newArrayList())
    );
    EasyMock.replay(autoScalingStrategy);

    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<TaskRunnerWorkItem>asList(
            new TaskRunnerWorkItem(testTask, null, null, null).withQueueInsertionTime(new DateTime())
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
    workerSetupData.set(new WorkerSetupData("0", 0, 1, null, null));

    EasyMock.expect(autoScalingStrategy.ipToIdLookup(EasyMock.<List<String>>anyObject()))
            .andReturn(Lists.<String>newArrayList("ip")).times(2);
    EasyMock.expect(autoScalingStrategy.terminate(EasyMock.<List<String>>anyObject())).andReturn(
        new AutoScalingData(Lists.<String>newArrayList("ip"), Lists.newArrayList("ip"))
    );
    EasyMock.replay(autoScalingStrategy);

    boolean terminatedSomething = simpleResourceManagementStrategy.doTerminate(
        Arrays.<TaskRunnerWorkItem>asList(
            new TaskRunnerWorkItem(testTask, null, null, null).withQueueInsertionTime(new DateTime())
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
        Arrays.<TaskRunnerWorkItem>asList(
            new TaskRunnerWorkItem(testTask, null, null, null).withQueueInsertionTime(new DateTime())
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

  private static class TestZkWorker extends ZkWorker
  {
    private final Task testTask;

    private TestZkWorker(
        Task testTask
    )
    {
      super(new Worker("host", "ip", 3, "version"), null, null);

      this.testTask = testTask;
    }

    @Override
    public Set<String> getRunningTasks()
    {
      if (testTask == null) {
        return Sets.newHashSet();
      }
      return Sets.newHashSet(testTask.getId());
    }
  }
}
