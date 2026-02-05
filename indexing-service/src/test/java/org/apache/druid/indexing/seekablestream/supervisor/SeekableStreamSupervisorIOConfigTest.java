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

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SeekableStreamSupervisorIOConfigTest
{
  @Test
  public void testAllDefaults()
  {
    LagAggregator lagAggregator = mock(LagAggregator.class);
    InputFormat inputFormat = mock(InputFormat.class);

    SeekableStreamSupervisorIOConfig config = new SeekableStreamSupervisorIOConfig(
        "stream",
        inputFormat,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        lagAggregator,
        null,
        null,
        null
    )
    {
    };

    Assert.assertEquals("stream", config.getStream());
    Assert.assertEquals(inputFormat, config.getInputFormat());
    Assert.assertEquals(Integer.valueOf(1), config.getReplicas());
    Assert.assertEquals(Integer.valueOf(1), config.getTaskCount());
    Assert.assertEquals(Duration.standardHours(1), config.getTaskDuration());
    Assert.assertEquals(Duration.standardSeconds(5), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(30), config.getPeriod());
    Assert.assertFalse(config.isUseEarliestSequenceNumber());
    Assert.assertEquals(Duration.standardMinutes(30), config.getCompletionTimeout());
    Assert.assertFalse(config.getEarlyMessageRejectionPeriod().isPresent());
    Assert.assertFalse(config.getLateMessageRejectionPeriod().isPresent());
    Assert.assertFalse(config.getLateMessageRejectionStartDateTime().isPresent());
    Assert.assertNull(config.getIdleConfig());
    Assert.assertNull(config.getStopTaskCount());
    Assert.assertEquals(lagAggregator, config.getLagAggregator());
    Assert.assertEquals(1, config.getMaxAllowedStops());
  }

  @Test
  public void testAutoScalerEnabledPreservesTaskCountWhenNonNull()
  {
    LagAggregator lagAggregator = mock(LagAggregator.class);

    // autoScalerEnabled = true
    AutoScalerConfig autoScalerConfig = mock(AutoScalerConfig.class);
    when(autoScalerConfig.getEnableTaskAutoScaler()).thenReturn(true);
    when(autoScalerConfig.getTaskCountStart()).thenReturn(5);
    when(autoScalerConfig.getTaskCountMin()).thenReturn(3);

    SeekableStreamSupervisorIOConfig configAuto = new SeekableStreamSupervisorIOConfig(
        "stream",
        null,
        2,
        10, // (taskCount should be ignored)
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        autoScalerConfig,
        lagAggregator,
        null,
        null,
        null
    )
    {
    };

    Assert.assertEquals(Integer.valueOf(5), configAuto.getTaskCount()); // taskCountStart

    // autoScalerEnabled = false
    SeekableStreamSupervisorIOConfig configNoAuto = new SeekableStreamSupervisorIOConfig(
        "stream",
        null,
        2,
        10,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        lagAggregator,
        null,
        null,
        null
    )
    {
    };

    Assert.assertEquals(Integer.valueOf(10), configNoAuto.getTaskCount());
  }

  @Test
  public void testBothLateMessageRejectionPeriodAndStartDateTime()
  {
    LagAggregator lagAggregator = mock(LagAggregator.class);

    IAE ex = Assert.assertThrows(
        IAE.class,
        () -> new SeekableStreamSupervisorIOConfig(
            "stream",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Period.seconds(10),
            null,
            null,
            lagAggregator,
            DateTimes.nowUtc(),
            null,
            null
        )
        {
        }
    );
    Assert.assertTrue(
        ex.getMessage()
          .contains(
              "SeekableStreamSupervisorIOConfig does not support both properties lateMessageRejectionStartDateTime and lateMessageRejectionPeriod"
          )
    );
  }

  @Test
  public void testNullAggregatorThrows()
  {
    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new SeekableStreamSupervisorIOConfig(
            "stream",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )
        {
        }
    );
    Assert.assertTrue(
        ex.getMessage().contains("'lagAggregator' must be specified in supervisor 'spec.ioConfig'")
    );
  }

  @Test
  public void testGetMaxAllowedStopsScalingDisabled()
  {
    LagAggregator lagAggregator = mock(LagAggregator.class);

    // Autoscaler disabled, stopTaskCount unset
    SeekableStreamSupervisorIOConfig config1 = new SeekableStreamSupervisorIOConfig(
        "stream",
        null,
        null,
        7,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        lagAggregator,
        null,
        null,
        null
    )
    {
    };
    Assert.assertEquals(7, config1.getMaxAllowedStops());

    // Autoscaler disabled, stopTaskCount set
    SeekableStreamSupervisorIOConfig config2 = new SeekableStreamSupervisorIOConfig(
        "stream",
        null,
        null,
        7,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        lagAggregator,
        null,
        null,
        3
    )
    {
    };
    Assert.assertEquals(3, config2.getMaxAllowedStops());
  }

  @Test
  public void testGetMaxAllowedStopsScalingEnabled()
  {
    LagAggregator lagAggregator = mock(LagAggregator.class);

    AutoScalerConfig autoScalerConfig = mock(AutoScalerConfig.class);

    // Autoscaler enabled, stopTaskCountRatio set
    when(autoScalerConfig.getEnableTaskAutoScaler()).thenReturn(true);
    when(autoScalerConfig.getTaskCountStart()).thenReturn(10);
    when(autoScalerConfig.getStopTaskCountRatio()).thenReturn(0.5);

    SeekableStreamSupervisorIOConfig config = new SeekableStreamSupervisorIOConfig(
        "stream",
        null,
        null,
        10,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        autoScalerConfig,
        lagAggregator,
        null,
        null,
        1
    )
    {
    };

    Assert.assertEquals(5, config.getMaxAllowedStops());

    // Ensure never goes below 1
    when(autoScalerConfig.getStopTaskCountRatio()).thenReturn(0.05);
    Assert.assertEquals(1, config.getMaxAllowedStops());

    // Autoscaler enabled, stopTaskCountRatio unset, stopTaskCount set
    when(autoScalerConfig.getEnableTaskAutoScaler()).thenReturn(true);
    when(autoScalerConfig.getTaskCountStart()).thenReturn(10);
    when(autoScalerConfig.getStopTaskCountRatio()).thenReturn(null);

    SeekableStreamSupervisorIOConfig config2 = new SeekableStreamSupervisorIOConfig(
        "stream",
        null,
        null,
        10,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        autoScalerConfig,
        lagAggregator,
        null,
        null,
        1
    )
    {
    };

    Assert.assertEquals(1, config2.getMaxAllowedStops());


    // Autoscaler enabled, stopTaskCountRatio unset, stopTaskCount unset
    when(autoScalerConfig.getEnableTaskAutoScaler()).thenReturn(true);
    when(autoScalerConfig.getTaskCountStart()).thenReturn(10);
    when(autoScalerConfig.getStopTaskCountRatio()).thenReturn(null);

    SeekableStreamSupervisorIOConfig config3 = new SeekableStreamSupervisorIOConfig(
        "stream",
        null,
        null,
        10,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        autoScalerConfig,
        lagAggregator,
        null,
        null,
        null
    )
    {
    };

    Assert.assertEquals(10, config3.getMaxAllowedStops());
  }
}
