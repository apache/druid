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

package org.apache.druid.query.groupby.having;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;

import java.math.BigDecimal;
import java.util.Map;
import java.util.regex.Pattern;

/**
 */
class HavingSpecMetricComparator
{
  static final Pattern LONG_PAT = Pattern.compile("[-|+]?\\d+");

  static int compare(String aggregationName, Number value, Map<String, AggregatorFactory> aggregators, Object metricValueObj)
  {
    if (metricValueObj != null) {
      if (aggregators != null && aggregators.containsKey(aggregationName)) {
        metricValueObj = aggregators.get(aggregationName).finalizeComputation(metricValueObj);
      }

      if (metricValueObj instanceof Long || metricValueObj instanceof Integer) {
        // Cast int metrics to longs, it's fine since longs can represent all int values.
        long n = ((Number) metricValueObj).longValue();

        if (value instanceof Long || value instanceof Integer) {
          return Longs.compare(n, value.longValue());
        } else if (value instanceof Double || value instanceof Float) {
          // Invert the comparison since we're providing n, value backwards.
          return -compareDoubleToLong(value.doubleValue(), n);
        } else {
          throw new ISE("Number was[%s]?!?", value.getClass().getName());
        }
      } else if (metricValueObj instanceof Double || metricValueObj instanceof Float) {
        // Cast floats to doubles, it's fine since doubles can represent all float values.
        double n = ((Number) metricValueObj).doubleValue();

        if (value instanceof Long || value instanceof Integer) {
          return compareDoubleToLong(n, value.longValue());
        } else if (value instanceof Double || value instanceof Float) {
          return Doubles.compare(n, value.doubleValue());
        } else {
          throw new ISE("Number was[%s]?!?", value.getClass().getName());
        }
      } else if (metricValueObj instanceof String) {
        String metricValueStr = (String) metricValueObj;
        if (LONG_PAT.matcher(metricValueStr).matches()) {
          long l = Long.parseLong(metricValueStr);
          return Long.compare(l, value.longValue());
        } else {
          double d = Double.parseDouble(metricValueStr);
          return Double.compare(d, value.doubleValue());
        }
      } else if (aggregators != null && aggregators.containsKey(aggregationName)) {
        // Use custom comparator in case of custom aggregation types
        AggregatorFactory aggregatorFactory = aggregators.get(aggregationName);
        return aggregatorFactory.getComparator()
                                .compare(
                                    aggregatorFactory.deserialize(metricValueObj),
                                    aggregatorFactory.deserialize(value)
                                );
      } else {
        throw new ISE("Unknown type of metric value: %s", metricValueObj);
      }
    } else {
      return Double.compare(0, value.doubleValue());
    }
  }

  private static int compareDoubleToLong(final double a, final long b)
  {
    // Use BigDecimal when comparing integers vs floating points, a convenient way to handle all cases (like
    // fractional values, values out of range of max long/max int) without worrying about them ourselves.
    return BigDecimal.valueOf(a).compareTo(BigDecimal.valueOf(b));
  }
}
