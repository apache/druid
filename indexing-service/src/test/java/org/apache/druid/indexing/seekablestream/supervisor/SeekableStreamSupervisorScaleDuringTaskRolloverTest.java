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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.google.common.base.Optional;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SeekableStreamSupervisorScaleDuringTaskRolloverTest extends SeekableStreamSupervisorTestBase
{
  private static final int DEFAULT_TASK_COUNT = 10;

  private SupervisorStateManagerConfig supervisorConfig;

  @Before
  public void setup()
  {
    supervisorConfig = new SupervisorStateManagerConfig();

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);
  }

  @Test
  public void test_maybeScaleDuringTaskRollover_noAutoScaler_doesNotScale()
  {
    // Given
    setupSpecExpectations(createIOConfig(5, null));
    EasyMock.expect(spec.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(spec);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(10);
    supervisor.start();

    int beforeTaskCount = supervisor.getIoConfig().getTaskCount();

    // When
    supervisor.maybeScaleDuringTaskRollover();

    // Then
    Assert.assertNull(supervisor.getIoConfig().getAutoScalerConfig());
  }

  @Test
  public void test_maybeScaleDuringTaskRollover_rolloverCountNonPositive_doesNotScale()
  {
    // Given
    setupSpecExpectations(getIOConfigWithCostBasedAutoScaler());
    EasyMock.expect(spec.createAutoscaler(EasyMock.anyObject()))
            .andReturn(createMockAutoScaler(-1))
            .anyTimes();
    EasyMock.replay(spec);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(100);
    supervisor.start();
    supervisor.createAutoscaler(spec);

    int beforeTaskCount = supervisor.getIoConfig().getTaskCount();

    // When
    supervisor.maybeScaleDuringTaskRollover();

    // Then
    Assert.assertNotNull(supervisor.getIoConfig().getAutoScalerConfig());
    Assert.assertEquals(
        "Task count should not change when rolloverTaskCount <= 0",
        beforeTaskCount,
        (int) supervisor.getIoConfig().getAutoScalerConfig().getTaskCountStart()
    );
  }

  @Test
  public void test_maybeScaleDuringTaskRollover_rolloverCountPositive_performsScaling()
  {
    // Given
    final int targetTaskCount = 5;

    setupSpecExpectations(getIOConfigWithCostBasedAutoScaler());
    EasyMock.expect(spec.createAutoscaler(EasyMock.anyObject()))
            .andReturn(createMockAutoScaler(targetTaskCount))
            .anyTimes();
    EasyMock.replay(spec);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(100);
    supervisor.start();
    supervisor.createAutoscaler(spec);

    // When
    supervisor.maybeScaleDuringTaskRollover();

    // Then
    Assert.assertNotNull(supervisor.getIoConfig().getAutoScalerConfig());
    Assert.assertEquals(
        "Task count should be updated to " + targetTaskCount + " when rolloverTaskCount > 0",
        targetTaskCount,
        (int) supervisor.getIoConfig().getAutoScalerConfig().getTaskCountStart()
    );
  }

  @Test
  public void test_maybeScaleDuringTaskRollover_rolloverCountZero_doesNotScale()
  {
    // Given
    setupSpecExpectations(getIOConfigWithCostBasedAutoScaler());
    EasyMock.expect(spec.createAutoscaler(EasyMock.anyObject()))
            .andReturn(createMockAutoScaler(0))
            .anyTimes();
    EasyMock.replay(spec);

    TestSeekableStreamSupervisor supervisor = new TestSeekableStreamSupervisor(100);
    supervisor.start();
    supervisor.createAutoscaler(spec);

    int beforeTaskCount = supervisor.getIoConfig().getTaskCount();

    // When
    supervisor.maybeScaleDuringTaskRollover();

    // Then
    Assert.assertNotNull(supervisor.getIoConfig().getAutoScalerConfig());
    Assert.assertEquals(
        "Task count should not change when rolloverTaskCount is 0",
        beforeTaskCount,
        (int) supervisor.getIoConfig().getAutoScalerConfig().getTaskCountStart()
    );
  }

  // Helper methods for test setup

  /**
   * Sets up common spec expectations. Call EasyMock.replay(spec) after this and any additional expectations.
   */
  private void setupSpecExpectations(SeekableStreamSupervisorIOConfig ioConfig)
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema(DATASOURCE)).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
  }

  /**
   * Creates a mock autoscaler that returns the specified rollover count.
   */
  private static SupervisorTaskAutoScaler createMockAutoScaler(int rolloverCount)
  {
    return new SupervisorTaskAutoScaler()
    {
      @Override
      public void start()
      {
      }

      @Override
      public void stop()
      {
      }

      @Override
      public void reset()
      {
      }

      @Override
      public int computeTaskCountForRollover()
      {
        return rolloverCount;
      }
    };
  }

  // Helper methods for config creation
  private static CostBasedAutoScalerConfig getCostBasedAutoScalerConfig()
  {
    return CostBasedAutoScalerConfig.builder()
                                    .enableTaskAutoScaler(true)
                                    .taskCountMax(100)
                                    .taskCountMin(1)
                                    .taskCountStart(1)
                                    .scaleActionPeriodMillis(60000)
                                    .build();
  }

  private SeekableStreamSupervisorIOConfig getIOConfigWithCostBasedAutoScaler()
  {
    return createIOConfig(DEFAULT_TASK_COUNT, getCostBasedAutoScalerConfig());
  }
}
