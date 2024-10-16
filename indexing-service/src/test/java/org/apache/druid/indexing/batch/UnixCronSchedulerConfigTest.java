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

public class UnixCronSchedulerConfigTest {

  @Test
  public void testYearlyMacroTranslation() {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@yearly");
    assertEquals("@yearly", config.getSchedule());
    assertEquals("0 0 1 1 *", config.getCron().asString());
  }

  @Test
  public void testMonthlyMacroTranslation() {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@monthly");
    assertEquals("@monthly", config.getSchedule());
    assertEquals("0 0 1 * *", config.getCron().asString());
  }

  @Test
  public void testWeeklyMacroTranslation() {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@weekly");
    assertEquals("@weekly", config.getSchedule());
    assertEquals("0 0 * * 0", config.getCron().asString());
  }

  @Test
  public void testDailyMacroTranslation() {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@daily");
    assertEquals("@daily", config.getSchedule());
    assertEquals("0 0 * * *", config.getCron().asString());
  }

  @Test
  public void testHourlyMacroTranslation() {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@hourly");
    assertEquals("@hourly", config.getSchedule());
    assertEquals("0 * * * *", config.getCron().asString());
  }

  @Test
  public void testCustomCronExpression() {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("0 15 10 * *");
    assertEquals("0 15 10 * *", config.getSchedule());
    assertEquals("0 15 10 * *", config.getCron().asString());
  }

  @Test
  public void testInvalidUnixCronExpression() {
    assertThrows(IllegalArgumentException.class, () -> new UnixCronSchedulerConfig("invalid-cron"));
  }

  @Test
  public void testSerde() throws JsonProcessingException {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@daily");
    final String json = objectMapper.writeValueAsString(config);

    final UnixCronSchedulerConfig deserializedConfig = objectMapper.readValue(json, UnixCronSchedulerConfig.class);
    assertEquals(config.getSchedule(), deserializedConfig.getSchedule());
    assertEquals(config.getCron().asString(), deserializedConfig.getCron().asString());
  }
}
