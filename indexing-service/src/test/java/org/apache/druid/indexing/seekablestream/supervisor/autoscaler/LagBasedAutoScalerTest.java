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
  private static final int ACTIVE_TASK_GROUP_AMOUNT = 25;
  private static final int PARTITION_COUNT = 100;
  private static final int TASK_COUNT_MIN = 50;
  private static final int TASK_COUNT_MAX = 100;
  private static final int SCALE_OUT_STEP = 4;
  private static final long SCALE_OUT_THRESHOLD = 2_000_000L;
  private static final long SCALE_IN_THRESHOLD = 300_000L;
  private static final double TRIGGER_SCALE_OUT_FRACTION = 0.7;
  private static final double TRIGGER_SCALE_IN_FRACTION = 0.9;

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
        30_000L,   // lagCollectionIntervalMillis
        300_000L,  // lagCollectionRangeMillis
        300_000L,  // scaleActionStartDelayMillis
        60_000L,   // scaleActionPeriodMillis
        SCALE_OUT_THRESHOLD,
        SCALE_IN_THRESHOLD,
        TRIGGER_SCALE_OUT_FRACTION,
        TRIGGER_SCALE_IN_FRACTION,
        TASK_COUNT_MAX,
        null,      // taskCountStart
        TASK_COUNT_MIN,
        1,         // scaleInStep
        SCALE_OUT_STEP,
        true,      // enableTaskAutoScaler
        6_000_000L, // minTriggerScaleActionFrequencyMillis
        null,      // lagAggregate
        null       // stopTaskCountRatio
    );
  }

  /**
   * Reproduces the bug where scale-out from a low activelyReadingTaskGroups count
   * yields a desiredTaskCount below taskCountMin.
   */
  @Test
  public void testScaleOutDoesNotReturnCountBelowTaskCountMin()
  {
    when(mockIoConfig.getTaskCount()).thenReturn(TASK_COUNT_MIN);
    when(mockSupervisor.getActiveTaskGroupsCount()).thenReturn(ACTIVE_TASK_GROUP_AMOUNT);
    when(mockSupervisor.getPartitionCount()).thenReturn(PARTITION_COUNT);

    LagBasedAutoScaler autoScaler = new LagBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter);
    List<Long> highLagSamples = Collections.nCopies(11, SCALE_OUT_THRESHOLD + 1);

    int result = autoScaler.computeDesiredTaskCount(new ArrayList<>(highLagSamples));

    // Bug: old code used getActiveTaskGroupsCount()=25 as baseline → 25+4=29 < taskCountMin(50)
    // Fix: uses ioConfig.getTaskCount()=50 as baseline → 50+4=54 >= taskCountMin(50)
    Assert.assertEquals(TASK_COUNT_MIN + SCALE_OUT_STEP, result);
  }
}
