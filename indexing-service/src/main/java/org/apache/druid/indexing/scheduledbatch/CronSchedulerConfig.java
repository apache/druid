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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;

/**
 * Interface representing a configuration for scheduling tasks based on cron expressions.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = UnixCronSchedulerConfig.TYPE, value = UnixCronSchedulerConfig.class),
    @JsonSubTypes.Type(name = QuartzCronSchedulerConfig.TYPE, value = QuartzCronSchedulerConfig.class),
})
public interface CronSchedulerConfig
{
  /**
   * Gets the next task submission time after the specified {@code referenceTime}, or {@code null} if no future
   * execution time exists based on the configured schedule (e.g., if the cron expression only specifies dates in the past).
   */
  @Nullable
  DateTime getNextTaskStartTimeAfter(DateTime referenceTime);

  /**
   * Gets the duration until the next task submission after the specified {@code referenceTime}, if no future
   * execution time exists based on the configured schedule (e.g., if the cron expression only specifies dates in the past).
   */
  @Nullable
  Duration getDurationUntilNextTaskStartTimeAfter(DateTime referenceTime);
}
