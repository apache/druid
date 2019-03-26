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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * The "&lt;" operator in a "having" clause. This is similar to SQL's "having aggregation &lt; value",
 * except that an aggregation in SQL is an expression instead of an aggregation name as in Druid.
 */
public class LessThanHavingSpec extends BaseHavingSpec
{
  private final String aggregationName;
  private final Number value;

  private volatile Map<String, AggregatorFactory> aggregators;

  public LessThanHavingSpec(
      @JsonProperty("aggregation") String aggName,
      @JsonProperty("value") Number value
  )
  {
    this.aggregationName = aggName;
    this.value = value;
  }

  @JsonProperty("aggregation")
  public String getAggregationName()
  {
    return aggregationName;
  }

  @JsonProperty("value")
  public Number getValue()
  {
    return value;
  }

  @Override
  public void setAggregators(Map<String, AggregatorFactory> aggregators)
  {
    this.aggregators = aggregators;
  }

  @Override
  public boolean eval(Row row)
  {
    Object metricVal = row.getRaw(aggregationName);
    if (metricVal == null || value == null) {
      return false;
    }
    return HavingSpecMetricComparator.compare(aggregationName, value, aggregators, metricVal) < 0;
  }

  /**
   * This method treats internal value as double mainly for ease of test.
   */
  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LessThanHavingSpec that = (LessThanHavingSpec) o;

    if (value != null && that.value != null) {
      return Double.compare(value.doubleValue(), that.value.doubleValue()) == 0;
    }

    if (value == null && that.value == null) {
      return true;
    }

    return false;
  }

  @Override
  public int hashCode()
  {
    int result = aggregationName != null ? aggregationName.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("LessThanHavingSpec");
    sb.append("{aggregationName='").append(aggregationName).append('\'');
    sb.append(", value=").append(value);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public byte[] getCacheKey()
  {
    try {
      final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      outputStream.write(HavingSpecUtil.CACHE_TYPE_ID_LESS_THAN);
      if (!Strings.isNullOrEmpty(aggregationName)) {
        outputStream.write(StringUtils.toUtf8(aggregationName));
      }
      outputStream.write(HavingSpecUtil.STRING_SEPARATOR);
      outputStream.write(StringUtils.toUtf8(String.valueOf(value)));
      if (aggregators == null) {
        return outputStream.toByteArray();
      }
      for (Map.Entry<String, AggregatorFactory> entry : aggregators.entrySet()) {
        final String key = entry.getKey();
        final AggregatorFactory val = entry.getValue();
        if (!Strings.isNullOrEmpty(key)) {
          outputStream.write(StringUtils.toUtf8(key));
        }
        outputStream.write(HavingSpecUtil.STRING_SEPARATOR);
        byte[] aggregatorBytes = val.getCacheKey();
        outputStream.write(aggregatorBytes);
      }
      return outputStream.toByteArray();
    }
    catch (IOException ex) {
      // If ByteArrayOutputStream.write has problems, that is a very bad thing
      throw new RuntimeException(ex);
    }
  }
}
