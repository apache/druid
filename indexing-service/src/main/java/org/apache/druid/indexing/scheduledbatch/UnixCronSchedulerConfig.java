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

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.InvalidInput;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Parses a Unix cron expression and computes the next execution time based on a given reference time.
 */
public class UnixCronSchedulerConfig implements CronSchedulerConfig
{
  public static final String TYPE = "unix";

  private static final CronParser UNIX_PARSER = createUnixCronParserWithMacros();
  private final ExecutionTime executionTime;

  private static CronParser createUnixCronParserWithMacros()
  {
    final CronDefinitionBuilder unixDefnWithMacros = CronDefinitionBuilder.defineCron();
    CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)
                         .getFieldDefinitions()
                         .forEach(unixDefnWithMacros::register);
    return new CronParser(
        unixDefnWithMacros.withSupportedNicknameHourly()
                          .withSupportedNicknameMidnight()
                          .withSupportedNicknameDaily()
                          .withSupportedNicknameWeekly()
                          .withSupportedNicknameMonthly()
                          .withSupportedNicknameAnnually()
                          .withSupportedNicknameYearly()
                          .instance()
    );
  }

  @JsonProperty
  private final String schedule;

  @JsonCreator
  public UnixCronSchedulerConfig(@JsonProperty("schedule") final String schedule)
  {
    try {
      this.executionTime = ExecutionTime.forCron(UNIX_PARSER.parse(schedule));
      this.schedule = schedule;
    }
    catch (IllegalArgumentException e) {
      throw InvalidInput.exception("Unix schedule[%s] is invalid: [%s]", schedule, e.getMessage());
    }
  }

  @Nullable
  @Override
  public DateTime getNextTaskStartTimeAfter(final DateTime referenceTime)
  {
    return CronSchedulerUtils.getNextTaskStartTimeAfter(executionTime, referenceTime);
  }

  @Nullable
  @Override
  public Duration getDurationUntilNextTaskStartTimeAfter(final DateTime referenceTime)
  {
    return CronSchedulerUtils.getDurationUntilNextTaskStartTimeAfter(executionTime, referenceTime);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UnixCronSchedulerConfig that = (UnixCronSchedulerConfig) o;
    return Objects.equals(schedule, that.schedule);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(schedule);
  }

  @Override
  public String toString()
  {
    return "UnixCronSchedulerConfig{" +
           "schedule='" + schedule + '\'' +
           '}';
  }
}
