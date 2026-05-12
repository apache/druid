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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

public class LagBasedAutoScalerConfigTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

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
    // When minScaleUpDelay/minScaleDownDelay are not set, they fall back to minTriggerScaleActionFrequencyMillis
    Assert.assertEquals(Duration.millis(600000), config.getMinScaleUpDelay());
    Assert.assertEquals(Duration.millis(600000), config.getMinScaleDownDelay());
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
        null,
        Duration.millis(3000),
        Duration.millis(7000),
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
        null,
        null,
        0.5
    );
    Assert.assertEquals(Double.valueOf(0.5), config.getStopTaskCountRatio());
  }

  @Test
  public void testScaleDelayFallback() throws Exception
  {
    // Neither minScaleUpDelay nor minScaleDownDelay set: both fall back to minTriggerScaleActionFrequencyMillis
    LagBasedAutoScalerConfig baseOnly = new LagBasedAutoScalerConfig(
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
        null,
        null,
        false,
        60000L,
        null,
        null,
        null,
        null
    );
    Assert.assertEquals(60000L, baseOnly.getMinTriggerScaleActionFrequencyMillis());
    Assert.assertEquals(Duration.millis(60000), baseOnly.getMinScaleUpDelay());
    Assert.assertEquals(Duration.millis(60000), baseOnly.getMinScaleDownDelay());

    // Only minScaleUpDelay set
    LagBasedAutoScalerConfig upOnly = new LagBasedAutoScalerConfig(
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
        null,
        null,
        false,
        60000L,
        Duration.millis(15000),
        null,
        null,
        null
    );
    Assert.assertEquals(Duration.millis(15000), upOnly.getMinScaleUpDelay());
    Assert.assertEquals(Duration.millis(60000), upOnly.getMinScaleDownDelay());

    // Only minScaleDownDelay set
    LagBasedAutoScalerConfig downOnly = new LagBasedAutoScalerConfig(
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
        null,
        null,
        false,
        60000L,
        null,
        Duration.millis(30000),
        null,
        null
    );
    Assert.assertEquals(Duration.millis(60000), downOnly.getMinScaleUpDelay());
    Assert.assertEquals(Duration.millis(30000), downOnly.getMinScaleDownDelay());

    // Both set: serde roundtrip preserves values
    LagBasedAutoScalerConfig bothSet = new LagBasedAutoScalerConfig(
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
        null,
        null,
        false,
        null,
        Duration.millis(15000),
        Duration.millis(30000),
        null,
        null
    );
    Assert.assertEquals(Duration.millis(15000), bothSet.getMinScaleUpDelay());
    Assert.assertEquals(Duration.millis(30000), bothSet.getMinScaleDownDelay());
    LagBasedAutoScalerConfig roundTripped = OBJECT_MAPPER.readValue(
        OBJECT_MAPPER.writeValueAsString(bothSet),
        LagBasedAutoScalerConfig.class
    );
    Assert.assertEquals(bothSet, roundTripped);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testScaleDelayFallbackViaSerde() throws Exception
  {
    // JSON with only minTriggerScaleActionFrequencyMillis (no Duration fields):
    // both getMinScaleUpDelay() and getMinScaleDownDelay() should fall back to it.
    String json = "{\"taskCountMax\":10,\"taskCountMin\":1,\"minTriggerScaleActionFrequencyMillis\":45000}";
    LagBasedAutoScalerConfig config = OBJECT_MAPPER.readValue(json, LagBasedAutoScalerConfig.class);
    Assert.assertEquals(45000L, config.getMinTriggerScaleActionFrequencyMillis());
    Assert.assertEquals(Duration.millis(45000), config.getMinScaleUpDelay());
    Assert.assertEquals(Duration.millis(45000), config.getMinScaleDownDelay());

    // JSON with minTriggerScaleActionFrequencyMillis and only minScaleUpDelay:
    // getMinScaleUpDelay() should return the explicit value; getMinScaleDownDelay() falls back.
    String jsonUpOnly = "{\"taskCountMax\":10,\"taskCountMin\":1,"
                        + "\"minTriggerScaleActionFrequencyMillis\":45000,"
                        + "\"minScaleUpDelay\":\"PT10S\"}";
    LagBasedAutoScalerConfig configUpOnly = OBJECT_MAPPER.readValue(jsonUpOnly, LagBasedAutoScalerConfig.class);
    Assert.assertEquals(Duration.standardSeconds(10), configUpOnly.getMinScaleUpDelay());
    Assert.assertEquals(Duration.millis(45000), configUpOnly.getMinScaleDownDelay());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMinTriggerScaleActionFrequencyMillisSerdeCompat() throws Exception
  {
    final long defaultMinTriggerMillis = 600_000L;

    // Backwards-compat: nothing set -> deprecated field gets its default and both direction
    // delays fall back to it.
    {
      LagBasedAutoScalerConfig config = OBJECT_MAPPER.readValue(
          "{\"taskCountMax\":10,\"taskCountMin\":1}",
          LagBasedAutoScalerConfig.class
      );
      Assert.assertEquals(defaultMinTriggerMillis, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.millis(defaultMinTriggerMillis), config.getMinScaleUpDelay());
      Assert.assertEquals(Duration.millis(defaultMinTriggerMillis), config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Backwards-compat: legacy spec sets only the deprecated field. Both direction delays fall
    // back to it.
    {
      LagBasedAutoScalerConfig config = OBJECT_MAPPER.readValue(
          "{\"taskCountMax\":10,\"taskCountMin\":1,\"minTriggerScaleActionFrequencyMillis\":900000}",
          LagBasedAutoScalerConfig.class
      );
      Assert.assertEquals(900_000L, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.millis(900_000), config.getMinScaleUpDelay());
      Assert.assertEquals(Duration.millis(900_000), config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Forwards-compat: direction delays set, deprecated field omitted. Deprecated field defaults.
    {
      LagBasedAutoScalerConfig config = OBJECT_MAPPER.readValue(
          "{\"taskCountMax\":10,\"taskCountMin\":1,"
          + "\"minScaleUpDelay\":\"PT2M\",\"minScaleDownDelay\":\"PT15M\"}",
          LagBasedAutoScalerConfig.class
      );
      Assert.assertEquals(defaultMinTriggerMillis, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.standardMinutes(2), config.getMinScaleUpDelay());
      Assert.assertEquals(Duration.standardMinutes(15), config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Forwards-compat: deprecated field AND direction delays set (overlapping migration window).
    // Direction delays win for their own direction; the deprecated field is preserved.
    {
      LagBasedAutoScalerConfig config = OBJECT_MAPPER.readValue(
          "{\"taskCountMax\":10,\"taskCountMin\":1,"
          + "\"minTriggerScaleActionFrequencyMillis\":900000,"
          + "\"minScaleUpDelay\":\"PT2M\",\"minScaleDownDelay\":\"PT15M\"}",
          LagBasedAutoScalerConfig.class
      );
      Assert.assertEquals(900_000L, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.standardMinutes(2), config.getMinScaleUpDelay());
      Assert.assertEquals(Duration.standardMinutes(15), config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Mixed: only minScaleUpDelay + deprecated field set. Down falls back to the deprecated field.
    {
      LagBasedAutoScalerConfig config = OBJECT_MAPPER.readValue(
          "{\"taskCountMax\":10,\"taskCountMin\":1,"
          + "\"minTriggerScaleActionFrequencyMillis\":900000,"
          + "\"minScaleUpDelay\":\"PT2M\"}",
          LagBasedAutoScalerConfig.class
      );
      Assert.assertEquals(900_000L, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.standardMinutes(2), config.getMinScaleUpDelay());
      Assert.assertEquals(Duration.millis(900_000), config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Mixed: only minScaleDownDelay + deprecated field set. Up falls back to the deprecated field.
    {
      LagBasedAutoScalerConfig config = OBJECT_MAPPER.readValue(
          "{\"taskCountMax\":10,\"taskCountMin\":1,"
          + "\"minTriggerScaleActionFrequencyMillis\":900000,"
          + "\"minScaleDownDelay\":\"PT15M\"}",
          LagBasedAutoScalerConfig.class
      );
      Assert.assertEquals(900_000L, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.millis(900_000), config.getMinScaleUpDelay());
      Assert.assertEquals(Duration.standardMinutes(15), config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Mixed: deprecated field omitted, only minScaleUpDelay set. Down falls back to the
    // deprecated field's default.
    {
      LagBasedAutoScalerConfig config = OBJECT_MAPPER.readValue(
          "{\"taskCountMax\":10,\"taskCountMin\":1,\"minScaleUpDelay\":\"PT2M\"}",
          LagBasedAutoScalerConfig.class
      );
      Assert.assertEquals(defaultMinTriggerMillis, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.standardMinutes(2), config.getMinScaleUpDelay());
      Assert.assertEquals(Duration.millis(defaultMinTriggerMillis), config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }
  }

  private void assertRoundTrips(LagBasedAutoScalerConfig config) throws Exception
  {
    LagBasedAutoScalerConfig roundTripped = OBJECT_MAPPER.readValue(
        OBJECT_MAPPER.writeValueAsString(config),
        LagBasedAutoScalerConfig.class
    );
    Assert.assertEquals(config, roundTripped);
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
        null,
        null,
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
        null,
        null,
        AggregateFunction.AVERAGE,
        0.5
    );

    Assert.assertNotEquals(config1, config2);
    Assert.assertNotEquals(config1.hashCode(), config2.hashCode());
  }
}
