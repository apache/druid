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

package org.apache.druid.indexing.seekablestream.supervisor.autoscaler;

import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;


public class LagBasedAutoScalerTest
{
  private static final int PARTITION_COUNT = 100;

  private SupervisorSpec mockSpec;
  private SeekableStreamSupervisor mockSupervisor;
  private SeekableStreamSupervisorIOConfig mockIoConfig;
  private ServiceEmitter mockEmitter;
  private LagBasedAutoScalerConfig config;

  @Before
  public void setUp()
  {
    mockSpec = Mockito.mock(SupervisorSpec.class);
    mockSupervisor = Mockito.mock(SeekableStreamSupervisor.class);
    mockEmitter = Mockito.mock(ServiceEmitter.class);
    mockIoConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(mockSpec.getId()).thenReturn("test-supervisor");
    when(mockSpec.getDataSources()).thenReturn(List.of("test-datasource"));
    when(mockSupervisor.getIoConfig()).thenReturn(mockIoConfig);
    when(mockIoConfig.getStream()).thenReturn("test-stream");

    config = new LagBasedAutoScalerConfig(
        30_000L,    // lagCollectionIntervalMillis
        300_000L,   // lagCollectionRangeMillis
        300_000L,   // scaleActionStartDelayMillis
        60_000L,    // scaleActionPeriodMillis
        2_000_000L,
        300_000L,
        0.7,
        0.9,
        100,
        null,       // taskCountStart
        50,
        1,          // scaleInStep
        4,
        true,       // enableTaskAutoScaler
        6_000_000L, // minTriggerScaleActionFrequencyMillis
        null,       // lagAggregate
        null        // stopTaskCountRatio
    );
  }

  /**
   * Verifies that scale-out uses the configured task count as the baseline.
   */
  @Test
  public void testScaleOutDoesNotReturnCountBelowTaskCountMin()
  {
    when(mockIoConfig.getTaskCount()).thenReturn(50);
    when(mockSupervisor.getPartitionCount()).thenReturn(PARTITION_COUNT);

    Assert.assertEquals(54, createAutoScaler().computeDesiredTaskCount(createLagSamples(2_000_001L)));
  }

  @Test
  public void testReturnsTaskCountMinWhenConfiguredTaskCountIsBelowMin()
  {
    when(mockIoConfig.getTaskCount()).thenReturn(1);
    when(mockSupervisor.getPartitionCount()).thenReturn(PARTITION_COUNT);

    Assert.assertEquals(50, createAutoScaler().computeDesiredTaskCount(createLagSamples(2_000_001L)));
  }

  @Test
  public void testReturnsTaskCountMaxWhenConfiguredTaskCountIsAboveMax()
  {
    when(mockIoConfig.getTaskCount()).thenReturn(101);
    when(mockSupervisor.getPartitionCount()).thenReturn(PARTITION_COUNT);

    Assert.assertEquals(100, createAutoScaler().computeDesiredTaskCount(createLagSamples(299_999L)));
  }

  private LagBasedAutoScaler createAutoScaler()
  {
    return new LagBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter);
  }

  private List<Long> createLagSamples(long lag)
  {
    return new ArrayList<>(Collections.nCopies(11, lag));
  }
}
