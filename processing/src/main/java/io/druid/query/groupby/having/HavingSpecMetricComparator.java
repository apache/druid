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

import io.druid.data.input.Row;
import io.druid.java.util.common.ISE;

import java.util.regex.Pattern;

/**
 */
class HavingSpecMetricComparator
{
  static final Pattern LONG_PAT = Pattern.compile("[-|+]?\\d+");

  static int compare(Row row, String aggregationName, Number value)
  {
    Object metricValueObj = row.getRaw(aggregationName);
    if (metricValueObj != null) {
      if (metricValueObj instanceof Long) {
        return Long.compare((Long) metricValueObj, value.longValue());
      } else if (metricValueObj instanceof Float) {
        return Float.compare((Float) metricValueObj, value.floatValue());
      } else if (metricValueObj instanceof Double) {
        return Double.compare((Double) metricValueObj, value.doubleValue());
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
