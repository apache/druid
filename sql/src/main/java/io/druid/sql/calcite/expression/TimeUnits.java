/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.expression;

import com.google.common.collect.ImmutableMap;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import org.apache.calcite.avatica.util.TimeUnitRange;

import java.util.Map;

public class TimeUnits
{
  private static final Map<TimeUnitRange, QueryGranularity> QUERY_GRANULARITY_MAP = ImmutableMap.<TimeUnitRange, QueryGranularity>builder()
      .put(TimeUnitRange.SECOND, QueryGranularities.SECOND)
      .put(TimeUnitRange.MINUTE, QueryGranularities.MINUTE)
      .put(TimeUnitRange.HOUR, QueryGranularities.HOUR)
      .put(TimeUnitRange.DAY, QueryGranularities.DAY)
      .put(TimeUnitRange.WEEK, QueryGranularities.WEEK)
      .put(TimeUnitRange.MONTH, QueryGranularities.MONTH)
      .put(TimeUnitRange.QUARTER, QueryGranularities.QUARTER)
      .put(TimeUnitRange.YEAR, QueryGranularities.YEAR)
      .build();

  // Note that QUARTER is not supported here.
  private static final Map<TimeUnitRange, String> EXTRACT_FORMAT_MAP = ImmutableMap.<TimeUnitRange, String>builder()
      .put(TimeUnitRange.SECOND, "s")
      .put(TimeUnitRange.MINUTE, "m")
      .put(TimeUnitRange.HOUR, "H")
      .put(TimeUnitRange.DAY, "d")
      .put(TimeUnitRange.WEEK, "w")
      .put(TimeUnitRange.MONTH, "M")
      .put(TimeUnitRange.YEAR, "Y")
      .build();

  /**
   * Returns the Druid QueryGranularity corresponding to a Calcite TimeUnitRange, or null if there is none.
   *
   * @param timeUnitRange timeUnit
   *
   * @return queryGranularity, or null
   */
  public static QueryGranularity toQueryGranularity(final TimeUnitRange timeUnitRange)
  {
    return QUERY_GRANULARITY_MAP.get(timeUnitRange);
  }

  /**
   * Returns the Joda format string corresponding to extracting on a Calcite TimeUnitRange, or null if there is none.
   *
   * @param timeUnitRange timeUnit
   *
   * @return queryGranularity, or null
   */
  public static String toDateTimeFormat(final TimeUnitRange timeUnitRange)
  {
    return EXTRACT_FORMAT_MAP.get(timeUnitRange);
  }
}
