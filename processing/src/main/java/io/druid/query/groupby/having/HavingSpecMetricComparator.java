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
import io.druid.query.aggregation.AggregatorFactory;

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

      if (metricValueObj instanceof Long) {
        long l = ((Long) metricValueObj).longValue();
        return Longs.compare(l, value.longValue());
      } else if (metricValueObj instanceof Float) {
        float l = ((Float) metricValueObj).floatValue();
        return Floats.compare(l, value.floatValue());
      } else if (metricValueObj instanceof Double) {
        double l = ((Double) metricValueObj).longValue();
        return Doubles.compare(l, value.doubleValue());
      } else if (metricValueObj instanceof Integer) {
        int l = ((Integer) metricValueObj).intValue();
        return Ints.compare(l, value.intValue());
      } else if (metricValueObj instanceof String) {
        String metricValueStr = (String) metricValueObj;
        if (LONG_PAT.matcher(metricValueStr).matches()) {
          long l = row.getLongMetric(aggregationName);
          return Longs.compare(l, value.longValue());
        }
      }
    }

    float f = row.getFloatMetric(aggregationName);
    return Floats.compare(f, value.floatValue());
  }
}
