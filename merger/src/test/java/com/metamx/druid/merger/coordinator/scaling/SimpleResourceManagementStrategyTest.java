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

package com.metamx.druid.merger.coordinator.scaling;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.TestTask;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.WorkerWrapper;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupManager;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

/**
 */
public class SimpleResourceManagementStrategyTest
{
  private AutoScalingStrategy autoScalingStrategy;
  private WorkerSetupManager workerSetupManager;
  private Task testTask;
  private SimpleResourceManagementStrategy simpleResourceManagementStrategy;

  @Before
  public void setUp() throws Exception
  {
    workerSetupManager = EasyMock.createMock(WorkerSetupManager.class);
    autoScalingStrategy = EasyMock.createMock(AutoScalingStrategy.class);

    testTask = new TestTask(
        "task1",
        "dummyDs",
        Lists.<DataSegment>newArrayList(
            new DataSegment(
                "dummyDs",
                new Interval(new DateTime(), new DateTime()),
                new DateTime().toString(),
                null,
                null,
                null,
                null,
                0
            )
        ), Lists.<AggregatorFactory>newArrayList()
    );
    simpleResourceManagementStrategy = new SimpleResourceManagementStrategy(
        new TestAutoScalingStrategy(),
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
            return null;
          }

          @Override
          public int getNumEventsToTrack()
          {
            return 1;
          }
        },
        workerSetupManager
    );
  }

  @Test
  public void testSuccessfulProvision() throws Exception
  {
    EasyMock.expect(autoScalingStrategy.provision()).andReturn(
        new AutoScalingData(Lists.<String>newArrayList(), Lists.newArrayList())
    );
    EasyMock.replay(autoScalingStrategy);

    simpleResourceManagementStrategy.doProvision(
        Arrays.<Task>asList(
            testTask
        ),
        Arrays.<WorkerWrapper>asList(
            new TestWorkerWrapper(testTask)
        )
    );

    EasyMock.verify(autoScalingStrategy);
  }

  @Test
  public void testDoTerminate() throws Exception
  {

  }

  private static class TestWorkerWrapper extends WorkerWrapper
  {
    private final Task testTask;

    private TestWorkerWrapper(
        Task testTask
    )
    {
      super(null, null, null);

      this.testTask = testTask;
    }

    @Override
    public Set<String> getRunningTasks()
    {
      return Sets.newHashSet(testTask.getId());
    }
  }
}
