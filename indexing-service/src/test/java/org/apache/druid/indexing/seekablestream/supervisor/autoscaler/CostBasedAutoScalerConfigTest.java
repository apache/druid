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
import org.junit.Assert;
import org.junit.Test;

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
                  + "  \"metricsCollectionIntervalMillis\": 30000,\n"
                  + "  \"metricsCollectionRangeMillis\": 600000,\n"
                  + "  \"scaleActionStartDelayMillis\": 300000,\n"
                  + "  \"scaleActionPeriodMillis\": 60000,\n"
                  + "  \"lagWeight\": 0.6,\n"
                  + "  \"idleWeight\": 0.4,\n"
                  + "  \"distancePenaltyExponent\": 3.0\n"
                  + "}";

    CostBasedAutoScalerConfig config = mapper.readValue(json, CostBasedAutoScalerConfig.class);

    Assert.assertTrue(config.getEnableTaskAutoScaler());
    Assert.assertEquals(100, config.getTaskCountMax());
    Assert.assertEquals(5, config.getTaskCountMin());
    Assert.assertEquals(Integer.valueOf(10), config.getTaskCountStart());
    Assert.assertEquals(600000L, config.getMinTriggerScaleActionFrequencyMillis());
    Assert.assertEquals(Double.valueOf(0.8), config.getStopTaskCountRatio());
    Assert.assertEquals(30000L, config.getMetricsCollectionIntervalMillis());
    Assert.assertEquals(600000L, config.getMetricsCollectionRangeMillis());
    Assert.assertEquals(300000L, config.getScaleActionStartDelayMillis());
    Assert.assertEquals(60000L, config.getScaleActionPeriodMillis());
    Assert.assertEquals(0.6, config.getLagWeight(), 0.001);
    Assert.assertEquals(0.4, config.getIdleWeight(), 0.001);

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
    Assert.assertEquals(60000L, config.getMetricsCollectionIntervalMillis());
    Assert.assertEquals(600000L, config.getMetricsCollectionRangeMillis());
    Assert.assertEquals(300000L, config.getScaleActionStartDelayMillis());
    Assert.assertEquals(600000L, config.getScaleActionPeriodMillis());
    Assert.assertEquals(1200000L, config.getMinTriggerScaleActionFrequencyMillis());
    Assert.assertEquals(0.25, config.getLagWeight(), 0.001);
    Assert.assertEquals(0.75, config.getIdleWeight(), 0.001);
    // changeWeight is not used in the cost function anymore
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
}
