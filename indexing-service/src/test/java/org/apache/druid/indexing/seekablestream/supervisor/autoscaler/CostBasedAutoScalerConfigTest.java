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
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig.DEFAULT_MIN_TRIGGER_SCALE_ACTION_FREQUENCY_MILLIS;
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
                  + "  \"highLagThreshold\": 30000,\n"
                  + "  \"minScaleDownDelay\": \"PT10M\",\n"
                  + "  \"scaleDownDuringTaskRolloverOnly\": true\n"
                  + "}";

    CostBasedAutoScalerConfig config = mapper.readValue(json, CostBasedAutoScalerConfig.class);

    Assert.assertTrue(config.getEnableTaskAutoScaler());
    Assert.assertEquals(100, config.getTaskCountMax());
    Assert.assertEquals(5, config.getTaskCountMin());
    Assert.assertEquals(Integer.valueOf(10), config.getTaskCountStart());
    Assert.assertEquals(600000L, config.getMinTriggerScaleActionFrequencyMillis());
    Assert.assertEquals(Double.valueOf(0.8), config.getStopTaskCountRatio());
    Assert.assertEquals(60000L, config.getScaleActionPeriodMillis());
    Assert.assertEquals(0.6, config.getLagWeight(), 0.001);
    Assert.assertEquals(0.4, config.getIdleWeight(), 0.001);
    Assert.assertEquals(Duration.standardMinutes(10), config.getMinScaleDownDelay());
    Assert.assertTrue(config.isScaleDownOnTaskRolloverOnly());
    Assert.assertEquals(30000, config.getHighLagThreshold());

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
    Assert.assertEquals(
        DEFAULT_MIN_TRIGGER_SCALE_ACTION_FREQUENCY_MILLIS,
        config.getMinTriggerScaleActionFrequencyMillis()
    );
    Assert.assertEquals(DEFAULT_LAG_WEIGHT, config.getLagWeight(), 0.001);
    Assert.assertEquals(DEFAULT_IDLE_WEIGHT, config.getIdleWeight(), 0.001);
    Assert.assertEquals(DEFAULT_MIN_SCALE_DELAY, config.getMinScaleDownDelay());
    Assert.assertFalse(config.isScaleDownOnTaskRolloverOnly());
    Assert.assertNull(config.getTaskCountStart());
    Assert.assertNull(config.getStopTaskCountRatio());
    // When highLagThreshold is not set, it defaults to -1 (burst scale-up disabled)
    Assert.assertEquals(-1, config.getHighLagThreshold());
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
                                                                .minTriggerScaleActionFrequencyMillis(600000L)
                                                                .stopTaskCountRatio(0.8)
                                                                .scaleActionPeriodMillis(60000L)
                                                                .lagWeight(0.6)
                                                                .idleWeight(0.4)
                                                                .minScaleDownDelay(Duration.standardMinutes(10))
                                                                .scaleDownDuringTaskRolloverOnly(true)
                                                                .highLagThreshold(30000)
                                                                .build();

    Assert.assertTrue(config.getEnableTaskAutoScaler());
    Assert.assertEquals(100, config.getTaskCountMax());
    Assert.assertEquals(5, config.getTaskCountMin());
    Assert.assertEquals(Integer.valueOf(10), config.getTaskCountStart());
    Assert.assertEquals(600000L, config.getMinTriggerScaleActionFrequencyMillis());
    Assert.assertEquals(Double.valueOf(0.8), config.getStopTaskCountRatio());
    Assert.assertEquals(60000L, config.getScaleActionPeriodMillis());
    Assert.assertEquals(0.6, config.getLagWeight(), 0.001);
    Assert.assertEquals(0.4, config.getIdleWeight(), 0.001);
    Assert.assertEquals(Duration.standardMinutes(10), config.getMinScaleDownDelay());
    Assert.assertTrue(config.isScaleDownOnTaskRolloverOnly());
    Assert.assertEquals(30000, config.getHighLagThreshold());
  }
}
