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

package org.apache.druid.server.coordination.startup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Locale;

/**
 * Eagerly loads column metadata for segments whose intervals overlap a recent sliding window; all others load lazily.
 * <p>
 * Balances bootstrap time and first-query performance by eagerly loading only "hot" segments. The window is
 * computed as {@code [now - period, now]} at Historical startup.
 */
public class LoadEagerlyBeforePeriod implements HistoricalStartupCacheLoadStrategy
{
  private static final Logger log = new Logger(LoadEagerlyBeforePeriod.class);
  public static final String STRATEGY_NAME = "loadEagerlyBeforePeriod";

  private final Interval eagerLoadingInterval;

  @VisibleForTesting
  @JsonCreator
  public LoadEagerlyBeforePeriod(
      @JsonProperty("period") Period eagerLoadingPeriod
  )
  {
    if (eagerLoadingPeriod == null) {
      throw DruidException
          .forPersona(DruidException.Persona.OPERATOR)
          .ofCategory(DruidException.Category.INVALID_INPUT)
          .build(
              "druid.segmentCache.startupLoadStrategy.period must be configured for Historical startup strategy[%s].",
              STRATEGY_NAME
          );
    }

    DateTime now = DateTimes.nowUtc();
    this.eagerLoadingInterval = new Interval(now.minus(eagerLoadingPeriod), now);

    log.info("Using [%s] strategy with Interval[%s]", STRATEGY_NAME, eagerLoadingInterval);
  }

  @VisibleForTesting
  public Interval getEagerLoadingInterval()
  {
    return this.eagerLoadingInterval;
  }

  @Override
  public boolean shouldLoadLazily(DataSegment segment)
  {
    return !segment.getInterval().overlaps(eagerLoadingInterval);
  }

  @Override
  public String toString()
  {
    return String.format(Locale.ROOT, "{type=%s,interval=%s}", STRATEGY_NAME, getEagerLoadingInterval());
  }
}
