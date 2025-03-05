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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class QuartzCronSchedulerConfigTest
{
  private static final DateTime DATE_2024_01_01 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeZone.UTC);

  @Test
  public void testGetNextTaskSubmissionTime()
  {
    final QuartzCronSchedulerConfig config = new QuartzCronSchedulerConfig("0 * * * * ?");

    assertEquals(DATE_2024_01_01.plusMinutes(1), config.getNextTaskStartTimeAfter(DATE_2024_01_01));
    assertEquals(Duration.standardMinutes(1), config.getDurationUntilNextTaskStartTimeAfter(DATE_2024_01_01));
  }

  @Test
  public void testCustomSchedule()
  {
    final QuartzCronSchedulerConfig config = new QuartzCronSchedulerConfig("0 15 10 * * ? *");

    assertEquals(DATE_2024_01_01.withTime(10, 15, 0, 0), config.getNextTaskStartTimeAfter(DATE_2024_01_01));
    assertEquals(Duration.standardMinutes(615), config.getDurationUntilNextTaskStartTimeAfter(DATE_2024_01_01));
  }

  @Test
  public void testOneOffYearSchedule()
  {
    final QuartzCronSchedulerConfig config = new QuartzCronSchedulerConfig("0 0 0 31 12 ? 2024-2025");

    assertEquals(DATE_2024_01_01.withDate(2024, 12, 31), config.getNextTaskStartTimeAfter(DATE_2024_01_01));
    assertEquals(Duration.standardDays(365), config.getDurationUntilNextTaskStartTimeAfter(DATE_2024_01_01));
  }

  @Test
  public void testScheduleWithNoFutureExecutions()
  {
    final QuartzCronSchedulerConfig config = new QuartzCronSchedulerConfig("0 0 0 31 12 ? 2020-2021");

    assertNull(config.getNextTaskStartTimeAfter(DATE_2024_01_01));
    assertNull(config.getDurationUntilNextTaskStartTimeAfter(DATE_2024_01_01));
  }

  @Test
  public void testOneOffDaySchedule()
  {
    final QuartzCronSchedulerConfig config = new QuartzCronSchedulerConfig("0 30 10-13 ? * WED,FRI");

    assertEquals(
        DATE_2024_01_01.withDayOfWeek(3).withTime(10, 30, 0, 0),
        config.getNextTaskStartTimeAfter(DATE_2024_01_01)
    );
    assertEquals(
        Duration.standardHours(58).plus(Duration.standardMinutes(30)),
        config.getDurationUntilNextTaskStartTimeAfter(DATE_2024_01_01)
    );
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();

    final QuartzCronSchedulerConfig config = new QuartzCronSchedulerConfig("*/30 * * * * ?");
    final String json = objectMapper.writeValueAsString(config);

    final CronSchedulerConfig deserializedConfig = objectMapper.readValue(json, CronSchedulerConfig.class);
    assertTrue(deserializedConfig instanceof QuartzCronSchedulerConfig);
    assertEquals(config, deserializedConfig);
  }

  @Test
  public void testInvalidCronExpression()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new QuartzCronSchedulerConfig("0 15 10 * *")
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "Quartz schedule[0 15 10 * *] is invalid: [Cron expression contains 5 parts but we expect one of [6, 7]]"
        )
    );
  }

  @Test
  public void testMacroExpressionsNotSupported()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new QuartzCronSchedulerConfig("@daily")
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "Quartz schedule[@daily] is invalid: [Nicknames not supported!]"
        )
    );
  }
}
