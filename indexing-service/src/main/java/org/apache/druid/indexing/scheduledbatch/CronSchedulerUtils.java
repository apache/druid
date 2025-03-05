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

import com.cronutils.model.time.ExecutionTime;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

public class CronSchedulerUtils
{
  /**
   * Computes the next task submission time after the specified {@code referenceTime}.
   * <p>
   * Returns {@code null} if no future execution time exists based on the given {@code executionTime},
   * which can occur if the schedule does not define any future occurrences.
   * </p>
   */
  @Nullable
  static DateTime getNextTaskStartTimeAfter(final ExecutionTime executionTime, final DateTime referenceTime)
  {
    final Optional<ZonedDateTime> zonedDateTime = executionTime.nextExecution(convertToZonedDateTime(referenceTime));
    if (zonedDateTime.isPresent()) {
      final ZonedDateTime zdt = zonedDateTime.get();
      final Instant instant = zdt.toInstant();
      return new DateTime(instant.toEpochMilli(), DateTimes.inferTzFromString(zdt.getZone().getId()));
    } else {
      return null;
    }
  }

  /**
   * Computes the duration until the next task submission time after the specified {@code referenceTime}.
   * <p>
   * Returns {@code null} if no future execution time exists based on the given {@code executionTime},
   * which can occur if the schedule does not define any future occurrences.
   * </p>
   */
  @Nullable
  static Duration getDurationUntilNextTaskStartTimeAfter(final ExecutionTime executionTime, final DateTime referenceTime)
  {
    final Optional<java.time.Duration> duration = executionTime.timeToNextExecution(convertToZonedDateTime(referenceTime));
    return duration.map(value -> Duration.millis(value.toMillis())).orElse(null);
  }

  private static ZonedDateTime convertToZonedDateTime(final DateTime jodaDateTime)
  {
    return ZonedDateTime.ofInstant(
        Instant.ofEpochMilli(jodaDateTime.getMillis()),
        ZoneId.of(jodaDateTime.getZone().getID())
    );
  }
}
