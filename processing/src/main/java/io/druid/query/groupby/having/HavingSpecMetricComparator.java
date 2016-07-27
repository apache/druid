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
import com.google.common.primitives.Longs;
import io.druid.data.input.Row;

import java.util.regex.Pattern;

/**
 */
class HavingSpecMetricComparator
{
  static final Pattern LONG_PAT = Pattern.compile("[-|+]?\\d+");

  static int compare(Row row, String aggregationName, Number value)
  {
    Object metricValueObj = row.getRaw(aggregationName);
    if (metricValueObj instanceof Number) {
      if (metricValueObj instanceof Long) {
        long l = (Long) metricValueObj;
        return Longs.compare(l, value.longValue());
      }
      if (metricValueObj instanceof Float) {
        float f = (Float) metricValueObj;
        return Floats.compare(f, value.floatValue());
      }
      if (metricValueObj instanceof Double) {
        double d = (Double) metricValueObj;
        return Doubles.compare(d, value.doubleValue());
      }
    } else if (metricValueObj instanceof String) {
      if (metricValueObj instanceof String) {
        String metricValueStr = (String) metricValueObj;
        Long l = Longs.tryParse(metricValueStr);
        if (l != null) {
          return Longs.compare(l, value.longValue());
        }
      }
    }

    // default
    float f = row.getFloatMetric(aggregationName);
    return Float.compare(f, value.floatValue());
  }
}
