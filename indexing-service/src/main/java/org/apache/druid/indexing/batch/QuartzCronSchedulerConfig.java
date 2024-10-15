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

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QuartzCronSchedulerConfig implements CronSchedulerConfig
{
  public static final String TYPE = "quartz";

  @JsonProperty
  private final String schedule;
  private final Cron cron;

  @JsonCreator
  public QuartzCronSchedulerConfig(@JsonProperty("schedule") final String schedule)
  {
    final String cronExpression = translateMacroToCronExpression(schedule);
    final CronParser cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
    this.cron = cronParser.parse(cronExpression);
    this.schedule = schedule;
  }

  public String getSchedule()
  {
    return schedule;
  }

  @Override
  public Cron getCron()
  {
    return cron;
  }

  private static String translateMacroToCronExpression(final String schedule)
  {
    switch (schedule) {
      case "@yearly":
      case "@annually":
        return "0 0 0 1 1 ? *";  // Every year at midnight on January 1st
      case "@monthly":
        return "0 0 0 1 * ?";    // Every month at midnight on the 1st
      case "@weekly":
        return "0 0 0 ? * 1";    // Every week on Sunday at midnight
      case "@daily":
      case "@midnight":
        return "0 0 0 * * ?";    // Every day at midnight
      case "@hourly":
        return "0 0 * * * ?";    // Every hour on the hour
      default:
        return schedule;   // Return as is if no macro is found
    }
  }
}

