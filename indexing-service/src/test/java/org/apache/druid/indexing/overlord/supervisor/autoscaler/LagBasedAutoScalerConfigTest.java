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
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"enableTaskAutoScaler\": false\n"
                     + "}";

    LagBasedAutoScalerConfig config = OBJECT_MAPPER.readValue(
        OBJECT_MAPPER.writeValueAsString(
            OBJECT_MAPPER.readValue(
                jsonStr,
                LagBasedAutoScalerConfig.class
            )
        ), LagBasedAutoScalerConfig.class
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
    Assert.assertNull(config.getStopTaskCountPercent());
    Assert.assertEquals(0, config.getTaskCountMax());
    Assert.assertEquals(0, config.getTaskCountMin());
    Assert.assertNull(config.getTaskCountStart());
  }

  @Test
  public void testSerdeWithOverrides() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"lagCollectionIntervalMillis\": 111,\n"
                     + "  \"lagCollectionRangeMillis\": 222,\n"
                     + "  \"scaleActionStartDelayMillis\": 333,\n"
                     + "  \"scaleActionPeriodMillis\": 444,\n"
                     + "  \"scaleOutThreshold\": 555,\n"
                     + "  \"scaleInThreshold\": 666,\n"
                     + "  \"triggerScaleOutFractionThreshold\": 0.12,\n"
                     + "  \"triggerScaleInFractionThreshold\": 0.34,\n"
                     + "  \"taskCountMax\": 10,\n"
                     + "  \"taskCountMin\": 5,\n"
                     + "  \"taskCountStart\": 7,\n"
                     + "  \"scaleInStep\": 3,\n"
                     + "  \"scaleOutStep\": 4,\n"
                     + "  \"enableTaskAutoScaler\": true,\n"
                     + "  \"minTriggerScaleActionFrequencyMillis\": 777,\n"
                     + "  \"lagAggregate\": \"AVERAGE\",\n"
                     + "  \"stopTaskCountPercent\": 0.5\n"
                     + "}";

    LagBasedAutoScalerConfig config = OBJECT_MAPPER.readValue(
        OBJECT_MAPPER.writeValueAsString(
            OBJECT_MAPPER.readValue(
                jsonStr,
                LagBasedAutoScalerConfig.class
            )
        ), LagBasedAutoScalerConfig.class
    );

    Assert.assertTrue(config.getEnableTaskAutoScaler());
    Assert.assertEquals(111, config.getLagCollectionIntervalMillis());
    Assert.assertEquals(222, config.getLagCollectionRangeMillis());
    Assert.assertEquals(333, config.getScaleActionStartDelayMillis());
    Assert.assertEquals(444, config.getScaleActionPeriodMillis());
    Assert.assertEquals(555, config.getScaleOutThreshold());
    Assert.assertEquals(666, config.getScaleInThreshold());
    Assert.assertEquals(0.12, config.getTriggerScaleOutFractionThreshold(), 0.00001);
    Assert.assertEquals(0.34, config.getTriggerScaleInFractionThreshold(), 0.00001);
    Assert.assertEquals(10, config.getTaskCountMax());
    Assert.assertEquals(5, config.getTaskCountMin());
    Assert.assertEquals(Integer.valueOf(7), config.getTaskCountStart());
    Assert.assertEquals(3, config.getScaleInStep());
    Assert.assertEquals(4, config.getScaleOutStep());
    Assert.assertEquals(777, config.getMinTriggerScaleActionFrequencyMillis());
    Assert.assertEquals(AggregateFunction.AVERAGE, config.getLagAggregate());
    Assert.assertEquals(Double.valueOf(0.5), config.getStopTaskCountPercent());
  }

  @Test
  public void testEnabledTaskCountChecks()
  {
    // Should throw if taskCountMax or taskCountMin is missing
    try {
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
      );
      Assert.fail("Expected exception for null taskCountMax or taskCountMin");
    }
    catch (RuntimeException ex) {
      Assert.assertTrue(ex.getMessage().contains("taskCountMax or taskCountMin can't be null"));
    }

    // Should throw if taskCountMax < taskCountMin
    try {
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
      );
      Assert.fail("Expected exception for taskCountMax < taskCountMin");
    }
    catch (RuntimeException ex) {
      Assert.assertTrue(ex.getMessage().contains("taskCountMax can't lower than taskCountMin"));
    }

    // Should throw if taskCountStart out of range
    try {
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
      );
      Assert.fail("Expected exception for taskCountStart out of range");
    }
    catch (RuntimeException ex) {
      Assert.assertTrue(ex.getMessage().contains("taskCountMin <= taskCountStart <= taskCountMax"));
    }
  }
}
