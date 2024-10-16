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

package org.apache.druid.indexing.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class QuartzCronSchedulerConfigTest
{
  @Test
  public void testCustomSchedule()
  {
    final QuartzCronSchedulerConfig config = new QuartzCronSchedulerConfig("0 15 10 * * ? *");
    assertEquals("0 15 10 * * ? *", config.getSchedule());
    assertEquals("0 15 10 * * ? *", config.getCron().asString());
  }

  @Test
  public void testOneOffYearSchedule()
  {
    final QuartzCronSchedulerConfig config = new QuartzCronSchedulerConfig("0 0 0 31 12 ? 2024-2025");
    assertEquals("0 0 0 31 12 ? 2024-2025", config.getSchedule());
    assertEquals("0 0 0 31 12 ? 2024-2025", config.getCron().asString());
  }

  @Test
  public void testOneOffDaySchedule()
  {
    final QuartzCronSchedulerConfig config = new QuartzCronSchedulerConfig("0 30 10-13 ? * WED,FRI");
    assertEquals("0 30 10-13 ? * WED,FRI", config.getSchedule());
    assertEquals("0 30 10-13 ? * 4,6", config.getCron().asString());
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final QuartzCronSchedulerConfig config = new QuartzCronSchedulerConfig("*/30 * * * * ?");
    final String json = objectMapper.writeValueAsString(config);

    final CronSchedulerConfig deserializedConfig = objectMapper.readValue(json, CronSchedulerConfig.class);
    assertTrue(deserializedConfig instanceof QuartzCronSchedulerConfig);

    final QuartzCronSchedulerConfig deserializedQuartzConfig = (QuartzCronSchedulerConfig) deserializedConfig;
    assertEquals(config.getSchedule(), deserializedQuartzConfig.getSchedule());
    assertEquals(config.getCron().asString(), deserializedQuartzConfig.getCron().asString());
  }

  @Test
  public void testInvalidCronExpression()
  {
    assertThrows(IllegalArgumentException.class, () -> new QuartzCronSchedulerConfig("0 15 10 * *"));
  }

  @Test
  public void testDailyMacroNotSupported()
  {
    assertThrows(IllegalArgumentException.class, () -> new QuartzCronSchedulerConfig("@daily"));
  }
}
