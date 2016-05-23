/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.granularity;

import com.google.common.collect.ImmutableMap;
import org.joda.time.Period;

import java.util.Map;

public class QueryGranularities
{
  static final Map<String, PeriodGranularity> CALENDRIC_GRANULARITIES = ImmutableMap.of(
      "YEAR", new PeriodGranularity(new Period("P1Y"), null, null),
      "MONTH", new PeriodGranularity(new Period("P1M"), null, null),
      "QUARTER", new PeriodGranularity(new Period("P3M"), null, null),
      "WEEK", new PeriodGranularity(new Period("P1W"), null, null)
  );
  public static final QueryGranularity NONE = new NoneGranularity();
  public static final QueryGranularity ALL = new AllGranularity();
  public static final QueryGranularity MINUTE = QueryGranularity.fromString("MINUTE");
  public static final QueryGranularity HOUR = QueryGranularity.fromString("HOUR");
  public static final QueryGranularity DAY = QueryGranularity.fromString("DAY");
  public static final QueryGranularity SECOND = QueryGranularity.fromString("SECOND");
  public static final QueryGranularity WEEK = QueryGranularity.fromString("WEEK");
  public static final QueryGranularity MONTH = QueryGranularity.fromString("MONTH");
  public static final QueryGranularity QUARTER = QueryGranularity.fromString("QUARTER");
  public static final QueryGranularity YEAR = QueryGranularity.fromString("YEAR");
}
