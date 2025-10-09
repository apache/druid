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

package org.apache.druid.iceberg.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

public class IcebergTimeWindowFilter implements IcebergFilter
{
  @JsonProperty
  private final String filterColumn;

  @JsonProperty
  private final Duration lookbackDuration;

  @JsonProperty
  private final Duration lookaheadDuration;

  @JsonProperty
  private final DateTime baseTime;

  @JsonCreator
  public IcebergTimeWindowFilter(
      @JsonProperty("filterColumn") String filterColumn,
      @JsonProperty("lookbackDuration") Duration lookbackDuration,
      @JsonProperty("lookaheadDuration") Duration lookaheadDuration,
      @JsonProperty("baseTime") DateTime baseTime
  )
  {
    Preconditions.checkNotNull(filterColumn, "You must specify a filter column on the timeWindow filter");
    this.filterColumn = filterColumn;
    this.lookbackDuration = Configs.valueOrDefault(lookbackDuration, new Period("P1D").toStandardDuration());
    this.lookaheadDuration = Configs.valueOrDefault(lookaheadDuration, Duration.ZERO);
    this.baseTime = Configs.valueOrDefault(baseTime, DateTimes.nowUtc());
  }

  @Override
  public TableScan filter(TableScan tableScan)
  {
    return tableScan.filter(getFilterExpression());
  }

  @Override
  public Expression getFilterExpression()
  {
    // Convert milliseconds to microseconds because Iceberg TimestampType uses microsecond precision
    long lookbackDurationinMicros = (baseTime.getMillis() - lookbackDuration.getMillis()) * 1000L;
    long lookforwardDurationinMicros = (baseTime.getMillis() + lookaheadDuration.getMillis()) * 1000L;
    return Expressions.and(
        Expressions.greaterThanOrEqual(
            filterColumn,
            Literal.of(lookbackDurationinMicros)
                                 .to(Types.TimestampType.withZone())
                                 .value()
        ),
        Expressions.lessThanOrEqual(
            filterColumn,
            Literal.of(lookforwardDurationinMicros)
                                 .to(Types.TimestampType.withZone())
                                 .value()
        )
    );
  }
}
