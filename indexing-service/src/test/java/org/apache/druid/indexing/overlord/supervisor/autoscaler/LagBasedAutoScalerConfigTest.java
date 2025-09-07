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

package org.apache.druid.indexing.overlord.supervisor.autoscaler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.LagBasedAutoScalerConfig;
import org.junit.Assert;
import org.junit.Test;

public class LagBasedAutoScalerConfigTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testDefaults()
  {
    LagBasedAutoScalerConfig config = new LagBasedAutoScalerConfig(
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
        null,
        null,
        null
    );

    Assert.assertFalse(config.getEnableTaskAutoScaler());
    Assert.assertEquals(30000, config.getLagCollectionIntervalMillis());
    Assert.assertEquals(600000, config.getLagCollectionRangeMillis());
    Assert.assertEquals(300000, config.getScaleActionStartDelayMillis());
    Assert.assertEquals(60000, config.getScaleActionPeriodMillis());
    Assert.assertEquals(6000000, config.getScaleOutThreshold());
    Assert.assertEquals(1000000, config.getScaleInThreshold());
    Assert.assertEquals(0.3, config.getTriggerScaleOutFractionThreshold(), 0.00001);
    Assert.assertEquals(0.9, config.getTriggerScaleInFractionThreshold(), 0.00001);
    Assert.assertEquals(1, config.getScaleInStep());
    Assert.assertEquals(2, config.getScaleOutStep());
    Assert.assertEquals(600000, config.getMinTriggerScaleActionFrequencyMillis());
    Assert.assertNull(config.getLagAggregate());
    Assert.assertNull(config.getStopTaskCountRatio());
    Assert.assertEquals(0, config.getTaskCountMax());
    Assert.assertEquals(0, config.getTaskCountMin());
    Assert.assertNull(config.getTaskCountStart());
  }

  @Test
  public void testSerde() throws Exception
  {
    LagBasedAutoScalerConfig config1 = new LagBasedAutoScalerConfig(
        100000L,
        100000L,
        100000L,
        100000L,
        1000000L,
        100000L,
        0.1,
        0.9,
        10,
        5,
        1,
        1,
        5,
        true,
        5000L,
        AggregateFunction.SUM,
        0.1
    );
    LagBasedAutoScalerConfig config2 = OBJECT_MAPPER.readValue(
        OBJECT_MAPPER.writeValueAsString(
            config1
        ), LagBasedAutoScalerConfig.class
    );

    Assert.assertEquals(config1, config2);
  }

  @Test
  public void testEnabledTaskCountChecks()
  {
    // Should throw if taskCountMax or taskCountMin is missing
    RuntimeException ex1 = Assert.assertThrows(
        RuntimeException.class,
        () ->
            new LagBasedAutoScalerConfig(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                1,
                null,
                null,
                null,
                true,
                null,
                null,
                null
            )
    );
    Assert.assertTrue(ex1.getMessage().contains("taskCountMax or taskCountMin can't be null"));

    // Should throw if taskCountMax < taskCountMin
    RuntimeException ex2 = Assert.assertThrows(
        RuntimeException.class,
        () ->
            new LagBasedAutoScalerConfig(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                2,
                1,
                3,
                null,
                null,
                true,
                null,
                null,
                null
            )
    );
    Assert.assertTrue(ex2.getMessage().contains("taskCountMax can't lower than taskCountMin"));

    // Should throw if taskCountStart out of range
    RuntimeException ex3 = Assert.assertThrows(
        RuntimeException.class,
        () ->
            new LagBasedAutoScalerConfig(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                5,
                10,
                3,
                null,
                null,
                true,
                null,
                null,
                null
            )
    );
    Assert.assertTrue(ex3.getMessage().contains("taskCountMin <= taskCountStart <= taskCountMax"));
  }

  @Test
  public void testStopTaskCountRatioBounds()
  {
    // Fail if stopTaskCountRatio = 0
    IllegalArgumentException ex1 = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new LagBasedAutoScalerConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            10,
            null,
            1,
            1,
            2,
            true,
            null,
            null,
            0.0
        )
    );
    Assert.assertTrue(ex1.getMessage().contains("0.0 < stopTaskCountRatio <= 1.0"));

    // Fail if stopTaskCountRatio < 0
    IllegalArgumentException ex2 = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new LagBasedAutoScalerConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            10,
            null,
            1,
            1, 2,
            true,
            null,
            null,
            -0.1
        )
    );
    Assert.assertTrue(ex2.getMessage().contains("0.0 < stopTaskCountRatio <= 1.0"));

    // Fail if stopTaskCountRatio > 1.0
    IllegalArgumentException ex3 = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new LagBasedAutoScalerConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            10,
            null,
            1,
            1,
            2,
            true,
            null,
            null,
            1.1
        )
    );
    Assert.assertTrue(ex3.getMessage().contains("0.0 < stopTaskCountRatio <= 1.0"));

    // Should succeed for a valid value
    LagBasedAutoScalerConfig config = new LagBasedAutoScalerConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        10,
        null,
        1,
        1,
        2,
        true,
        null,
        null,
        0.5
    );
    Assert.assertEquals(Double.valueOf(0.5), config.getStopTaskCountRatio());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    LagBasedAutoScalerConfig config1 = new LagBasedAutoScalerConfig(
        1000L,
        2000L,
        3000L,
        4000L,
        5000L,
        6000L,
        0.1,
        0.2,
        10,
        5,
        1,
        2,
        3,
        true,
        7000L,
        AggregateFunction.SUM,
        0.5
    );
    LagBasedAutoScalerConfig config2 = new LagBasedAutoScalerConfig(
        2000L,
        2000L,
        5000L,
        4500L,
        5000L,
        6000L,
        0.2,
        0.4,
        40,
        20,
        1,
        4,
        1,
        true,
        7000L,
        AggregateFunction.AVERAGE,
        0.5
    );

    Assert.assertNotEquals(config1, config2);
    Assert.assertNotEquals(config1.hashCode(), config2.hashCode());
  }
}
