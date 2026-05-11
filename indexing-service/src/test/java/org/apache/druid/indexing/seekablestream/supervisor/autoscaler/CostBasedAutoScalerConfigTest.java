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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig.DEFAULT_IDLE_WEIGHT;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig.DEFAULT_LAG_WEIGHT;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig.DEFAULT_MIN_SCALE_DELAY;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig.DEFAULT_SCALE_ACTION_PERIOD_MILLIS;

@SuppressWarnings("TextBlockMigration")
public class CostBasedAutoScalerConfigTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testSerdeWithAllProperties() throws Exception
  {
    String json = "{\n"
                  + "  \"autoScalerStrategy\": \"costBased\",\n"
                  + "  \"enableTaskAutoScaler\": true,\n"
                  + "  \"taskCountMax\": 100,\n"
                  + "  \"taskCountMin\": 5,\n"
                  + "  \"taskCountStart\": 10,\n"
                  + "  \"minTriggerScaleActionFrequencyMillis\": 600000,\n"
                  + "  \"stopTaskCountRatio\": 0.8,\n"
                  + "  \"scaleActionPeriodMillis\": 60000,\n"
                  + "  \"lagWeight\": 0.6,\n"
                  + "  \"idleWeight\": 0.4,\n"
                  + "  \"minScaleUpDelay\": \"PT5M\",\n"
                  + "  \"minScaleDownDelay\": \"PT10M\",\n"
                  + "  \"scaleDownDuringTaskRolloverOnly\": true\n"
                  + "}";

    CostBasedAutoScalerConfig config = mapper.readValue(json, CostBasedAutoScalerConfig.class);

    Assert.assertTrue(config.getEnableTaskAutoScaler());
    Assert.assertEquals(100, config.getTaskCountMax());
    Assert.assertEquals(5, config.getTaskCountMin());
    Assert.assertEquals(Integer.valueOf(10), config.getTaskCountStart());
    Assert.assertEquals(Double.valueOf(0.8), config.getStopTaskCountRatio());
    Assert.assertEquals(60000L, config.getScaleActionPeriodMillis());
    Assert.assertEquals(0.6, config.getLagWeight(), 0.001);
    Assert.assertEquals(0.4, config.getIdleWeight(), 0.001);
    Assert.assertEquals(Duration.standardMinutes(5), config.getMinScaleUpDelay());
    Assert.assertEquals(Duration.standardMinutes(10), config.getMinScaleDownDelay());
    Assert.assertTrue(config.isScaleDownOnTaskRolloverOnly());
    Assert.assertFalse(config.shouldUseTaskCountBoundariesOnScaleUp());
    Assert.assertTrue(config.shouldUseTaskCountBoundariesOnScaleDown());

    // Test serialization back to JSON
    String serialized = mapper.writeValueAsString(config);
    CostBasedAutoScalerConfig deserialized = mapper.readValue(serialized, CostBasedAutoScalerConfig.class);

    Assert.assertEquals(config, deserialized);
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String json = "{\n"
                  + "  \"autoScalerStrategy\": \"costBased\",\n"
                  + "  \"enableTaskAutoScaler\": true,\n"
                  + "  \"taskCountMax\": 50,\n"
                  + "  \"taskCountMin\": 2\n"
                  + "}";

    CostBasedAutoScalerConfig config = mapper.readValue(json, CostBasedAutoScalerConfig.class);

    Assert.assertTrue(config.getEnableTaskAutoScaler());
    Assert.assertEquals(50, config.getTaskCountMax());
    Assert.assertEquals(2, config.getTaskCountMin());

    // Check defaults
    Assert.assertEquals(DEFAULT_SCALE_ACTION_PERIOD_MILLIS, config.getScaleActionPeriodMillis());
    Assert.assertEquals(DEFAULT_LAG_WEIGHT, config.getLagWeight(), 0.001);
    Assert.assertEquals(DEFAULT_IDLE_WEIGHT, config.getIdleWeight(), 0.001);
    // minScaleUpDelay and minScaleDownDelay each have their own independent default
    Assert.assertEquals(Duration.millis(DEFAULT_SCALE_ACTION_PERIOD_MILLIS), config.getMinScaleUpDelay());
    Assert.assertEquals(DEFAULT_MIN_SCALE_DELAY, config.getMinScaleDownDelay());
    Assert.assertFalse(config.isScaleDownOnTaskRolloverOnly());
    Assert.assertFalse(config.shouldUseTaskCountBoundariesOnScaleUp());
    Assert.assertTrue(config.shouldUseTaskCountBoundariesOnScaleDown());
    Assert.assertNull(config.getTaskCountStart());
    Assert.assertNull(config.getStopTaskCountRatio());
  }

  @Test
  public void testSerdeWithDisabledAutoScaler() throws Exception
  {
    String json = "{\n"
                  + "  \"autoScalerStrategy\": \"costBased\",\n"
                  + "  \"enableTaskAutoScaler\": false\n"
                  + "}";

    CostBasedAutoScalerConfig config = mapper.readValue(json, CostBasedAutoScalerConfig.class);

    Assert.assertFalse(config.getEnableTaskAutoScaler());
    // When disabled, taskCountMax and taskCountMin default to 0
    Assert.assertEquals(0, config.getTaskCountMax());
    Assert.assertEquals(0, config.getTaskCountMin());
  }

  @Test(expected = RuntimeException.class)
  public void testValidation_MissingTaskCountMax()
  {
    CostBasedAutoScalerConfig.builder()
                             .taskCountMin(5)
                             .enableTaskAutoScaler(true)
                             .build();
  }

  @Test(expected = RuntimeException.class)
  public void testValidation_MissingTaskCountMin()
  {
    CostBasedAutoScalerConfig.builder()
                             .taskCountMax(100)
                             .enableTaskAutoScaler(true)
                             .build();
  }

  @Test(expected = RuntimeException.class)
  public void testValidation_MaxLessThanMin()
  {
    CostBasedAutoScalerConfig.builder()
                             .taskCountMax(5)
                             .taskCountMin(10)
                             .enableTaskAutoScaler(true)
                             .build();
  }

  @Test(expected = RuntimeException.class)
  public void testValidation_TaskCountStartOutOfRange()
  {
    CostBasedAutoScalerConfig.builder()
                             .taskCountMax(100)
                             .taskCountMin(5)
                             .taskCountStart(200)
                             .enableTaskAutoScaler(true)
                             .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidation_InvalidStopTaskCountRatio()
  {
    CostBasedAutoScalerConfig.builder()
                             .taskCountMax(100)
                             .taskCountMin(5)
                             .stopTaskCountRatio(1.5)
                             .enableTaskAutoScaler(true)
                             .build();
  }

  @Test
  public void testBuilder()
  {
    CostBasedAutoScalerConfig config = CostBasedAutoScalerConfig.builder()
                                                                .taskCountMax(100)
                                                                .taskCountMin(5)
                                                                .taskCountStart(10)
                                                                .enableTaskAutoScaler(true)
                                                                .stopTaskCountRatio(0.8)
                                                                .scaleActionPeriodMillis(60000L)
                                                                .lagWeight(0.6)
                                                                .idleWeight(0.4)
                                                                .useTaskCountBoundariesOnScaleUp(true)
                                                                .useTaskCountBoundariesOnScaleDown(true)
                                                                .minScaleUpDelay(Duration.standardMinutes(5))
                                                                .minScaleDownDelay(Duration.standardMinutes(10))
                                                                .scaleDownDuringTaskRolloverOnly(true)
                                                                .build();

    Assert.assertTrue(config.getEnableTaskAutoScaler());
    Assert.assertEquals(100, config.getTaskCountMax());
    Assert.assertEquals(5, config.getTaskCountMin());
    Assert.assertEquals(Integer.valueOf(10), config.getTaskCountStart());
    Assert.assertEquals(Double.valueOf(0.8), config.getStopTaskCountRatio());
    Assert.assertEquals(60000L, config.getScaleActionPeriodMillis());
    Assert.assertEquals(0.6, config.getLagWeight(), 0.001);
    Assert.assertEquals(0.4, config.getIdleWeight(), 0.001);
    Assert.assertTrue(config.shouldUseTaskCountBoundariesOnScaleUp());
    Assert.assertTrue(config.shouldUseTaskCountBoundariesOnScaleDown());
    Assert.assertEquals(Duration.standardMinutes(5), config.getMinScaleUpDelay());
    Assert.assertEquals(Duration.standardMinutes(10), config.getMinScaleDownDelay());
    Assert.assertTrue(config.isScaleDownOnTaskRolloverOnly());
  }

  @Test
  public void testScaleDelayDefaults() throws Exception
  {
    // Neither set: each direction gets its own independent default
    CostBasedAutoScalerConfig defaults = CostBasedAutoScalerConfig.builder()
                                                                  .taskCountMax(10)
                                                                  .taskCountMin(1)
                                                                  .build();
    Assert.assertEquals(Duration.millis(DEFAULT_SCALE_ACTION_PERIOD_MILLIS), defaults.getMinScaleUpDelay());
    Assert.assertEquals(DEFAULT_MIN_SCALE_DELAY, defaults.getMinScaleDownDelay());

    // Only minScaleUpDelay set: up uses explicit value, down uses its default
    CostBasedAutoScalerConfig upOnly = CostBasedAutoScalerConfig.builder()
                                                                .taskCountMax(10)
                                                                .taskCountMin(1)
                                                                .minScaleUpDelay(Duration.standardMinutes(5))
                                                                .build();
    Assert.assertEquals(Duration.standardMinutes(5), upOnly.getMinScaleUpDelay());
    Assert.assertEquals(DEFAULT_MIN_SCALE_DELAY, upOnly.getMinScaleDownDelay());

    // Only minScaleDownDelay set: down uses explicit value, up uses its own default (does not fall back to down)
    CostBasedAutoScalerConfig downOnly = CostBasedAutoScalerConfig.builder()
                                                                  .taskCountMax(10)
                                                                  .taskCountMin(1)
                                                                  .minScaleDownDelay(Duration.standardMinutes(20))
                                                                  .build();
    Assert.assertEquals(Duration.millis(DEFAULT_SCALE_ACTION_PERIOD_MILLIS), downOnly.getMinScaleUpDelay());
    Assert.assertEquals(Duration.standardMinutes(20), downOnly.getMinScaleDownDelay());

    // Both set: serde roundtrip preserves values
    CostBasedAutoScalerConfig bothSet = CostBasedAutoScalerConfig.builder()
                                                                 .taskCountMax(10)
                                                                 .taskCountMin(1)
                                                                 .minScaleUpDelay(Duration.standardMinutes(5))
                                                                 .minScaleDownDelay(Duration.standardMinutes(20))
                                                                 .build();
    Assert.assertEquals(Duration.standardMinutes(5), bothSet.getMinScaleUpDelay());
    Assert.assertEquals(Duration.standardMinutes(20), bothSet.getMinScaleDownDelay());
    CostBasedAutoScalerConfig roundTripped = mapper.readValue(
        mapper.writeValueAsString(bothSet),
        CostBasedAutoScalerConfig.class
    );
    Assert.assertEquals(bothSet, roundTripped);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMinTriggerScaleActionFrequencyMillisSerdeCompat() throws Exception
  {
    final long defaultMinTriggerMillis = DEFAULT_SCALE_ACTION_PERIOD_MILLIS;
    final Duration defaultUp = Duration.millis(DEFAULT_SCALE_ACTION_PERIOD_MILLIS);
    final Duration defaultDown = DEFAULT_MIN_SCALE_DELAY;

    // Backwards-compat: nothing set -> everything uses its own default.
    {
      CostBasedAutoScalerConfig config = mapper.readValue(
          "{\"autoScalerStrategy\":\"costBased\",\"enableTaskAutoScaler\":true,\"taskCountMax\":10,\"taskCountMin\":1}",
          CostBasedAutoScalerConfig.class
      );
      Assert.assertEquals(defaultMinTriggerMillis, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(defaultUp, config.getMinScaleUpDelay());
      Assert.assertEquals(defaultDown, config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Backwards-compat: legacy spec sets only the deprecated field. Direction delays still use
    // their own defaults (no cross-field fallback in CostBased).
    {
      CostBasedAutoScalerConfig config = mapper.readValue(
          "{\"autoScalerStrategy\":\"costBased\",\"enableTaskAutoScaler\":true,\"taskCountMax\":10,\"taskCountMin\":1,"
          + "\"minTriggerScaleActionFrequencyMillis\":900000}",
          CostBasedAutoScalerConfig.class
      );
      Assert.assertEquals(900_000L, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.millis(900_000L), config.getMinScaleUpDelay());
      Assert.assertEquals(defaultDown, config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Forwards-compat: direction delays set, deprecated field omitted. Deprecated field defaults.
    {
      CostBasedAutoScalerConfig config = mapper.readValue(
          "{\"autoScalerStrategy\":\"costBased\",\"enableTaskAutoScaler\":true,\"taskCountMax\":10,\"taskCountMin\":1,"
          + "\"minScaleUpDelay\":\"PT2M\",\"minScaleDownDelay\":\"PT15M\"}",
          CostBasedAutoScalerConfig.class
      );
      Assert.assertEquals(defaultMinTriggerMillis, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.standardMinutes(2), config.getMinScaleUpDelay());
      Assert.assertEquals(Duration.standardMinutes(15), config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Forwards-compat: deprecated field AND direction delays set (overlapping migration window).
    // All three are honored independently.
    {
      CostBasedAutoScalerConfig config = mapper.readValue(
          "{\"autoScalerStrategy\":\"costBased\",\"enableTaskAutoScaler\":true,\"taskCountMax\":10,\"taskCountMin\":1,"
          + "\"minTriggerScaleActionFrequencyMillis\":900000,"
          + "\"minScaleUpDelay\":\"PT2M\",\"minScaleDownDelay\":\"PT15M\"}",
          CostBasedAutoScalerConfig.class
      );
      Assert.assertEquals(900_000L, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.standardMinutes(2), config.getMinScaleUpDelay());
      Assert.assertEquals(Duration.standardMinutes(15), config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Only minScaleUpDelay set alongside the deprecated field: down uses its own default,
    // not the deprecated field's value.
    {
      CostBasedAutoScalerConfig config = mapper.readValue(
          "{\"autoScalerStrategy\":\"costBased\",\"enableTaskAutoScaler\":true,\"taskCountMax\":10,\"taskCountMin\":1,"
          + "\"minTriggerScaleActionFrequencyMillis\":900000,"
          + "\"minScaleUpDelay\":\"PT2M\"}",
          CostBasedAutoScalerConfig.class
      );
      Assert.assertEquals(900_000L, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.standardMinutes(2), config.getMinScaleUpDelay());
      Assert.assertEquals(defaultDown, config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Only minScaleDownDelay set alongside the deprecated field: up uses its own default.
    {
      CostBasedAutoScalerConfig config = mapper.readValue(
          "{\"autoScalerStrategy\":\"costBased\",\"enableTaskAutoScaler\":true,\"taskCountMax\":10,\"taskCountMin\":1,"
          + "\"minTriggerScaleActionFrequencyMillis\":900000,"
          + "\"minScaleDownDelay\":\"PT15M\"}",
          CostBasedAutoScalerConfig.class
      );
      Assert.assertEquals(900_000L, config.getMinTriggerScaleActionFrequencyMillis());
      Assert.assertEquals(Duration.millis(900_000L), config.getMinScaleUpDelay());
      Assert.assertEquals(Duration.standardMinutes(15), config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }
  }

  private void assertRoundTrips(CostBasedAutoScalerConfig config) throws Exception
  {
    CostBasedAutoScalerConfig roundTripped = mapper.readValue(
        mapper.writeValueAsString(config),
        CostBasedAutoScalerConfig.class
    );
    Assert.assertEquals(config, roundTripped);
  }
}
