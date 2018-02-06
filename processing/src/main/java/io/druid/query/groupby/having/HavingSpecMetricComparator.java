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

package io.druid.query.groupby.having;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.data.input.Row;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;

import java.math.BigDecimal;
import java.util.Map;
import java.util.regex.Pattern;

/**
 */
class HavingSpecMetricComparator
{
  static final Pattern LONG_PAT = Pattern.compile("[-|+]?\\d+");

  static int compare(Row row, String aggregationName, Number value, Map<String, AggregatorFactory> aggregators)
  {

    Object metricValueObj = row.getRaw(aggregationName);

    if (metricValueObj != null) {
      if (aggregators != null && aggregators.containsKey(aggregationName)) {
        metricValueObj = aggregators.get(aggregationName).finalizeComputation(metricValueObj);
      }

      if (metricValueObj instanceof Long || metricValueObj instanceof Integer) {
        long n = ((Number) metricValueObj).longValue();

        if (value instanceof Long || value instanceof Integer) {
          return Longs.compare(n, value.longValue());
        } else if (value instanceof Double || value instanceof Float) {
          return BigDecimal.valueOf(n).compareTo(BigDecimal.valueOf(value.doubleValue()));
        } else {
          throw new ISE("Number was[%s]?!?", value.getClass().getName());
        }
      } else if (metricValueObj instanceof Float) {
        float n = (Float) metricValueObj;
        return Floats.compare(n, value.floatValue());
      } else if (metricValueObj instanceof Double) {
        double n = (Double) metricValueObj;
        return Doubles.compare(n, value.doubleValue());
      } else if (metricValueObj instanceof String) {
        String metricValueStr = (String) metricValueObj;
        if (LONG_PAT.matcher(metricValueStr).matches()) {
          long l = Long.parseLong(metricValueStr);
          return Long.compare(l, value.longValue());
        } else {
          double d = Double.parseDouble(metricValueStr);
          return Double.compare(d, value.doubleValue());
        }
      } else {
        throw new ISE("Unknown type of metric value: %s", metricValueObj);
      }
    } else {
      return Double.compare(0, value.doubleValue());
    }
  }
}
