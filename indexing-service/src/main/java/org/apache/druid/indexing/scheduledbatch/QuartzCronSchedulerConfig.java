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
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Objects;

public class QuartzCronSchedulerConfig implements CronSchedulerConfig
{
  public static final String TYPE = "quartz";

  private static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

  @JsonProperty
  private final String schedule;

  private final ExecutionTime executionTime;

  @JsonCreator
  public QuartzCronSchedulerConfig(@JsonProperty("schedule") final String schedule)
  {
    this.executionTime = ExecutionTime.forCron(CRON_PARSER.parse(schedule));
    this.schedule = schedule;
  }

  @Nullable
  @Override
  public DateTime getNextTaskSubmissionTime(final DateTime dateTime)
  {
    return CronSchedulerUtils.getNextTaskSubmissionTime(executionTime, dateTime);
  }

  @Nullable
  @Override
  public Duration getTimeUntilNextTaskSubmission(final DateTime dateTime)
  {
    return CronSchedulerUtils.getTimeUntilNextTaskSubmission(executionTime, dateTime);
  }

  @Override
  public String toString()
  {
    return "QuartzCronSchedulerConfig{" +
           "schedule='" + schedule + '\'' +
           '}';
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
    QuartzCronSchedulerConfig that = (QuartzCronSchedulerConfig) o;
    return Objects.equals(schedule, that.schedule);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(schedule);
  }
}

