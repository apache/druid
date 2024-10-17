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

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UnixCronSchedulerConfig implements CronSchedulerConfig
{
  public static final String TYPE = "unix";

  @JsonProperty
  private final String schedule;

  private final Cron cron;

  private static final CronParser CRON_PARSER = createUnixCronParserWithMacros();

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
  
  @JsonCreator
  public UnixCronSchedulerConfig(@JsonProperty("schedule") final String schedule)
  {
    this.cron = CRON_PARSER.parse(schedule);
    this.schedule = schedule;
  }

  @Override
  public Cron getCron()
  {
    return cron;
  }

  public String getSchedule()
  {
    return schedule;
  }
}
