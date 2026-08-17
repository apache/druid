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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig.DEFAULT_IDLE_WEIGHT;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig.DEFAULT_LAG_WEIGHT;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig.DEFAULT_MIN_SCALE_DOWN_DELAY;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig.DEFAULT_MIN_SCALE_UP_DELAY;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig.DEFAULT_SCALE_ACTION_PERIOD;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.WeightedCostFunction.DEFAULT_HIGH_LAG_COST_FACTOR;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO;

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
                  + "  \"optimalTaskIdleRatio\": 0.3,\n"
                  + "  \"minScaleUpDelay\": \"PT5M\",\n"
                  + "  \"minScaleDownDelay\": \"PT10M\",\n"
                  + "  \"scaleDownDuringTaskRolloverOnly\": true,\n"
                  + "  \"usePollIdleRatio\": false,\n"
                  + "  \"criticalLagThreshold\": 500000,\n"
                  + "  \"minCostDropPercentForScaling\": 10,\n"
                  + "  \"highLagCostFactor\": 8.0\n"
                  + "}";

    final CostBasedAutoScalerConfig config = mapper.readValue(json, CostBasedAutoScalerConfig.class);

    Assertions.assertTrue(config.getEnableTaskAutoScaler());
    Assertions.assertEquals(100, config.getTaskCountMax());
    Assertions.assertEquals(5, config.getTaskCountMin());
    Assertions.assertEquals(Integer.valueOf(10), config.getTaskCountStart());
    Assertions.assertEquals(Double.valueOf(0.8), config.getStopTaskCountRatio());
    Assertions.assertEquals(60000L, config.getScaleActionPeriodMillis());
    Assertions.assertEquals(0.6, config.getLagWeight(), 0.001);
    Assertions.assertEquals(0.4, config.getIdleWeight(), 0.001);
    Assertions.assertEquals(0.3, config.getOptimalTaskIdleRatio(), 0.001);
    Assertions.assertEquals(Duration.standardMinutes(5), config.getMinScaleUpDelay());
    Assertions.assertEquals(Duration.standardMinutes(10), config.getMinScaleDownDelay());
    Assertions.assertTrue(config.isScaleDownOnTaskRolloverOnly());
    Assertions.assertFalse(config.isUsePollIdleRatio());
    Assertions.assertFalse(config.isUseTaskCountBoundariesOnScaleUp());
    Assertions.assertTrue(config.isUseTaskCountBoundariesOnScaleDown());
    Assertions.assertEquals(Long.valueOf(500000), config.getCriticalLagThreshold());
    Assertions.assertEquals(10, config.getMinCostDropPercentForScaling());
    Assertions.assertEquals(8.0, config.getHighLagCostFactor(), 0.001);

    // Test serialization back to JSON
    final String serialized = mapper.writeValueAsString(config);
    final CostBasedAutoScalerConfig deserialized = mapper.readValue(serialized, CostBasedAutoScalerConfig.class);

    Assertions.assertEquals(config, deserialized);
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

    final CostBasedAutoScalerConfig config = mapper.readValue(json, CostBasedAutoScalerConfig.class);

    Assertions.assertTrue(config.getEnableTaskAutoScaler());
    Assertions.assertEquals(50, config.getTaskCountMax());
    Assertions.assertEquals(2, config.getTaskCountMin());

    // Check defaults
    Assertions.assertEquals(DEFAULT_SCALE_ACTION_PERIOD.getMillis(), config.getScaleActionPeriodMillis());
    Assertions.assertEquals(DEFAULT_LAG_WEIGHT, config.getLagWeight(), 0.001);
    Assertions.assertEquals(DEFAULT_IDLE_WEIGHT, config.getIdleWeight(), 0.001);
    Assertions.assertEquals(OPTIMAL_TASK_IDLE_RATIO, config.getOptimalTaskIdleRatio(), 0.001);
    // minScaleUpDelay and minScaleDownDelay each have their own independent default
    Assertions.assertEquals(DEFAULT_MIN_SCALE_UP_DELAY, config.getMinScaleUpDelay());
    Assertions.assertEquals(DEFAULT_MIN_SCALE_DOWN_DELAY, config.getMinScaleDownDelay());
    Assertions.assertFalse(config.isScaleDownOnTaskRolloverOnly());
    Assertions.assertTrue(config.isUsePollIdleRatio());
    Assertions.assertFalse(config.isUseTaskCountBoundariesOnScaleUp());
    Assertions.assertTrue(config.isUseTaskCountBoundariesOnScaleDown());
    Assertions.assertNull(config.getTaskCountStart());
    Assertions.assertNull(config.getStopTaskCountRatio());
    Assertions.assertNull(config.getCriticalLagThreshold());
    Assertions.assertEquals(0, config.getMinCostDropPercentForScaling());
    Assertions.assertEquals(DEFAULT_HIGH_LAG_COST_FACTOR, config.getHighLagCostFactor(), 0.001);
  }

  @Test
  public void testSerdeWithDisabledAutoScaler() throws Exception
  {
    String json = "{\n"
                  + "  \"autoScalerStrategy\": \"costBased\",\n"
                  + "  \"enableTaskAutoScaler\": false\n"
                  + "}";

    CostBasedAutoScalerConfig config = mapper.readValue(json, CostBasedAutoScalerConfig.class);

    Assertions.assertFalse(config.getEnableTaskAutoScaler());
    // When disabled, taskCountMax and taskCountMin default to 0
    Assertions.assertEquals(0, config.getTaskCountMax());
    Assertions.assertEquals(0, config.getTaskCountMin());
  }

  @Test
  public void testValidation_MissingTaskCountMax()
  {
    Assertions.assertThrows(
        RuntimeException.class,
        () -> CostBasedAutoScalerConfig.builder()
                                       .taskCountMin(5)
                                       .enableTaskAutoScaler(true)
                                       .build()
    );
  }

  @Test
  public void testValidation_MissingTaskCountMin()
  {
    Assertions.assertThrows(
        RuntimeException.class,
        () -> CostBasedAutoScalerConfig.builder()
                                       .taskCountMax(100)
                                       .enableTaskAutoScaler(true)
                                       .build()
    );
  }

  @Test
  public void testValidation_MaxLessThanMin()
  {
    Assertions.assertThrows(
        RuntimeException.class,
        () -> CostBasedAutoScalerConfig.builder()
                                       .taskCountMax(5)
                                       .taskCountMin(10)
                                       .enableTaskAutoScaler(true)
                                       .build()
    );
  }

  @Test
  public void testValidation_TaskCountStartOutOfRange()
  {
    Assertions.assertThrows(
        RuntimeException.class,
        () -> CostBasedAutoScalerConfig.builder()
                                       .taskCountMax(100)
                                       .taskCountMin(5)
                                       .taskCountStart(200)
                                       .enableTaskAutoScaler(true)
                                       .build()
    );
  }

  @Test
  public void testValidation_InvalidStopTaskCountRatio()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CostBasedAutoScalerConfig.builder()
                                       .taskCountMax(100)
                                       .taskCountMin(5)
                                       .stopTaskCountRatio(1.5)
                                       .enableTaskAutoScaler(true)
                                       .build()
    );
  }

  @Test
  public void testValidationZeroOptimalTaskIdleRatio()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CostBasedAutoScalerConfig.builder()
                                       .taskCountMax(100)
                                       .taskCountMin(5)
                                       .optimalTaskIdleRatio(0.0)
                                       .enableTaskAutoScaler(true)
                                       .build()
    );
  }

  @Test
  public void testValidationOneOptimalTaskIdleRatio()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CostBasedAutoScalerConfig.builder()
                                       .taskCountMax(100)
                                       .taskCountMin(5)
                                       .optimalTaskIdleRatio(1.0)
                                       .enableTaskAutoScaler(true)
                                       .build()
    );
  }

  @Test
  public void testBuilder()
  {
    final CostBasedAutoScalerConfig config = CostBasedAutoScalerConfig.builder()
                                                                      .taskCountMax(100)
                                                                      .taskCountMin(5)
                                                                      .taskCountStart(10)
                                                                      .enableTaskAutoScaler(true)
                                                                      .stopTaskCountRatio(0.8)
                                                                      .scaleActionPeriodMillis(60000L)
                                                                      .lagWeight(0.6)
                                                                      .idleWeight(0.4)
                                                                      .optimalTaskIdleRatio(0.3)
                                                                      .useTaskCountBoundariesOnScaleUp(true)
                                                                      .useTaskCountBoundariesOnScaleDown(true)
                                                                      .minScaleUpDelay(Duration.standardMinutes(5))
                                                                      .minScaleDownDelay(Duration.standardMinutes(10))
                                                                      .scaleDownDuringTaskRolloverOnly(true)
                                                                      .usePollIdleRatio(false)
                                                                      .criticalLagThreshold(500000L)
                                                                      .highLagCostFactor(8.0)
                                                                      .build();

    Assertions.assertTrue(config.getEnableTaskAutoScaler());
    Assertions.assertEquals(100, config.getTaskCountMax());
    Assertions.assertEquals(5, config.getTaskCountMin());
    Assertions.assertEquals(Integer.valueOf(10), config.getTaskCountStart());
    Assertions.assertEquals(Double.valueOf(0.8), config.getStopTaskCountRatio());
    Assertions.assertEquals(60000L, config.getScaleActionPeriodMillis());
    Assertions.assertEquals(0.6, config.getLagWeight(), 0.001);
    Assertions.assertEquals(0.4, config.getIdleWeight(), 0.001);
    Assertions.assertEquals(0.3, config.getOptimalTaskIdleRatio(), 0.001);
    Assertions.assertTrue(config.isUseTaskCountBoundariesOnScaleUp());
    Assertions.assertTrue(config.isUseTaskCountBoundariesOnScaleDown());
    Assertions.assertEquals(Duration.standardMinutes(5), config.getMinScaleUpDelay());
    Assertions.assertEquals(Duration.standardMinutes(10), config.getMinScaleDownDelay());
    Assertions.assertTrue(config.isScaleDownOnTaskRolloverOnly());
    Assertions.assertFalse(config.isUsePollIdleRatio());
    Assertions.assertEquals(Long.valueOf(500000), config.getCriticalLagThreshold());
    Assertions.assertEquals(8.0, config.getHighLagCostFactor(), 0.001);
  }

  @Test
  public void testValidation_NegativeCriticalLagAmplificationMultiplier()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CostBasedAutoScalerConfig.builder()
                                       .taskCountMax(100)
                                       .taskCountMin(5)
                                       .highLagCostFactor(-1.0)
                                       .enableTaskAutoScaler(true)
                                       .build()
    );
  }

  @Test
  public void testValidation_ZeroCriticalLagThreshold()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CostBasedAutoScalerConfig.builder()
                                       .taskCountMax(100)
                                       .taskCountMin(5)
                                       .criticalLagThreshold(0L)
                                       .enableTaskAutoScaler(true)
                                       .build()
    );
  }

  @Test
  public void testValidation_NegativeMinCostDropPercentForScaling()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CostBasedAutoScalerConfig.builder()
                                       .taskCountMax(100)
                                       .taskCountMin(5)
                                       .minCostDropPercentForScaling(-1)
                                       .enableTaskAutoScaler(true)
                                       .build()
    );
  }

  @Test
  public void testValidation_MinCostDropPercentForScalingAbove100()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CostBasedAutoScalerConfig.builder()
                                       .taskCountMax(100)
                                       .taskCountMin(5)
                                       .minCostDropPercentForScaling(101)
                                       .enableTaskAutoScaler(true)
                                       .build()
    );
  }

  @Test
  public void testScaleDelayDefaults() throws Exception
  {
    // Neither set: each direction gets its own independent default
    CostBasedAutoScalerConfig defaults = CostBasedAutoScalerConfig.builder()
                                                                  .taskCountMax(10)
                                                                  .taskCountMin(1)
                                                                  .build();
    Assertions.assertEquals(DEFAULT_MIN_SCALE_UP_DELAY, defaults.getMinScaleUpDelay());
    Assertions.assertEquals(DEFAULT_MIN_SCALE_DOWN_DELAY, defaults.getMinScaleDownDelay());

    // Only minScaleUpDelay set: up uses explicit value, down uses its default
    CostBasedAutoScalerConfig upOnly = CostBasedAutoScalerConfig.builder()
                                                                .taskCountMax(10)
                                                                .taskCountMin(1)
                                                                .minScaleUpDelay(Duration.standardMinutes(5))
                                                                .build();
    Assertions.assertEquals(Duration.standardMinutes(5), upOnly.getMinScaleUpDelay());
    Assertions.assertEquals(DEFAULT_MIN_SCALE_DOWN_DELAY, upOnly.getMinScaleDownDelay());

    // Only minScaleDownDelay set: down uses explicit value, up uses its own default (does not fall back to down)
    CostBasedAutoScalerConfig downOnly = CostBasedAutoScalerConfig.builder()
                                                                  .taskCountMax(10)
                                                                  .taskCountMin(1)
                                                                  .minScaleDownDelay(Duration.standardMinutes(20))
                                                                  .build();
    Assertions.assertEquals(DEFAULT_MIN_SCALE_UP_DELAY, downOnly.getMinScaleUpDelay());
    Assertions.assertEquals(Duration.standardMinutes(20), downOnly.getMinScaleDownDelay());

    // Both set: serde roundtrip preserves values
    CostBasedAutoScalerConfig bothSet = CostBasedAutoScalerConfig.builder()
                                                                 .taskCountMax(10)
                                                                 .taskCountMin(1)
                                                                 .minScaleUpDelay(Duration.standardMinutes(5))
                                                                 .minScaleDownDelay(Duration.standardMinutes(20))
                                                                 .build();
    Assertions.assertEquals(Duration.standardMinutes(5), bothSet.getMinScaleUpDelay());
    Assertions.assertEquals(Duration.standardMinutes(20), bothSet.getMinScaleDownDelay());
    CostBasedAutoScalerConfig roundTripped = mapper.readValue(
        mapper.writeValueAsString(bothSet),
        CostBasedAutoScalerConfig.class
    );
    Assertions.assertEquals(bothSet, roundTripped);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testMinTriggerScaleActionFrequencyMillisSerdeCompat() throws Exception
  {
    final long defaultMinTriggerMillis = -1;
    final Duration defaultUp = DEFAULT_MIN_SCALE_UP_DELAY;
    final Duration defaultDown = DEFAULT_MIN_SCALE_DOWN_DELAY;

    // Backwards-compat: nothing set -> everything uses its own default.
    {
      CostBasedAutoScalerConfig config = mapper.readValue(
          "{\"autoScalerStrategy\":\"costBased\",\"enableTaskAutoScaler\":true,\"taskCountMax\":10,\"taskCountMin\":1}",
          CostBasedAutoScalerConfig.class
      );
      Assertions.assertEquals(defaultMinTriggerMillis, config.getMinTriggerScaleActionFrequencyMillis());
      Assertions.assertEquals(defaultUp, config.getMinScaleUpDelay());
      Assertions.assertEquals(defaultDown, config.getMinScaleDownDelay());
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
      Assertions.assertEquals(defaultUp, config.getMinScaleUpDelay());
      Assertions.assertEquals(defaultDown, config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }

    // Forwards-compat: direction delays set, deprecated field omitted. Deprecated field defaults.
    {
      CostBasedAutoScalerConfig config = mapper.readValue(
          "{\"autoScalerStrategy\":\"costBased\",\"enableTaskAutoScaler\":true,\"taskCountMax\":10,\"taskCountMin\":1,"
          + "\"minScaleUpDelay\":\"PT2M\",\"minScaleDownDelay\":\"PT15M\"}",
          CostBasedAutoScalerConfig.class
      );
      Assertions.assertEquals(defaultMinTriggerMillis, config.getMinTriggerScaleActionFrequencyMillis());
      Assertions.assertEquals(Duration.standardMinutes(2), config.getMinScaleUpDelay());
      Assertions.assertEquals(Duration.standardMinutes(15), config.getMinScaleDownDelay());
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
      Assertions.assertEquals(Duration.standardMinutes(2), config.getMinScaleUpDelay());
      Assertions.assertEquals(Duration.standardMinutes(15), config.getMinScaleDownDelay());
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
      Assertions.assertEquals(Duration.standardMinutes(2), config.getMinScaleUpDelay());
      Assertions.assertEquals(defaultDown, config.getMinScaleDownDelay());
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
      Assertions.assertEquals(defaultUp, config.getMinScaleUpDelay());
      Assertions.assertEquals(Duration.standardMinutes(15), config.getMinScaleDownDelay());
      assertRoundTrips(config);
    }
  }

  private void assertRoundTrips(CostBasedAutoScalerConfig config) throws Exception
  {
    CostBasedAutoScalerConfig roundTripped = mapper.readValue(
        mapper.writeValueAsString(config),
        CostBasedAutoScalerConfig.class
    );
    Assertions.assertEquals(config, roundTripped);
  }
}
