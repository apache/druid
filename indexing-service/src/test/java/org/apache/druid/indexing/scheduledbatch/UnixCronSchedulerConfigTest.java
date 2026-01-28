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

package org.apache.druid.indexing.scheduledbatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UnixCronSchedulerConfigTest
{
  private static final DateTime DATE_2024_01_01 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeZone.UTC);

  @Test
  public void testHourlyMacro()
  {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@hourly");

    assertEquals(DATE_2024_01_01.plusHours(1), config.getNextTaskStartTimeAfter(DATE_2024_01_01));
    assertEquals(Duration.standardHours(1), config.getDurationUntilNextTaskStartTimeAfter(DATE_2024_01_01));
  }

  @Test
  public void testDailyMacro()
  {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@daily");

    assertEquals(DATE_2024_01_01.plusDays(1), config.getNextTaskStartTimeAfter(DATE_2024_01_01));
    assertEquals(Duration.standardDays(1), config.getDurationUntilNextTaskStartTimeAfter(DATE_2024_01_01));
  }

  @Test
  public void testWeeklyMacro()
  {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@weekly");

    assertEquals(DATE_2024_01_01.withDayOfWeek(7), config.getNextTaskStartTimeAfter(DATE_2024_01_01));
    assertEquals(Duration.standardDays(6), config.getDurationUntilNextTaskStartTimeAfter(DATE_2024_01_01));
  }

  @Test
  public void testMonthlyMacro()
  {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@monthly");

    assertEquals(DATE_2024_01_01.plusMonths(1).withDayOfMonth(1), config.getNextTaskStartTimeAfter(DATE_2024_01_01));
    assertEquals(Duration.standardDays(31), config.getDurationUntilNextTaskStartTimeAfter(DATE_2024_01_01));
  }

  @Test
  public void testYearlyMacro()
  {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@yearly");

    assertEquals(DATE_2024_01_01.plusYears(1).withDayOfYear(1), config.getNextTaskStartTimeAfter(DATE_2024_01_01));
    assertEquals(Duration.standardDays(366), config.getDurationUntilNextTaskStartTimeAfter(DATE_2024_01_01));
  }

  @Test
  public void test30MinutesSchedule()
  {
    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("*/30 * * * *");

    assertEquals(DATE_2024_01_01.plusMinutes(30), config.getNextTaskStartTimeAfter(DATE_2024_01_01));
    assertEquals(Duration.standardMinutes(30), config.getDurationUntilNextTaskStartTimeAfter(DATE_2024_01_01));
  }


  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final UnixCronSchedulerConfig config = new UnixCronSchedulerConfig("@daily");
    final String json = objectMapper.writeValueAsString(config);

    final CronSchedulerConfig deserializedConfig = objectMapper.readValue(json, CronSchedulerConfig.class);
    assertTrue(deserializedConfig instanceof UnixCronSchedulerConfig);
    assertEquals(config, deserializedConfig);
  }

  @Test
  public void testInvalidUnixCronExpression()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new UnixCronSchedulerConfig("0 15 10 * * ? *")
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "Unix schedule[0 15 10 * * ? *] is invalid: [Cron expression contains 7 parts but we expect one of [5]]"
        )
    );
  }
}
