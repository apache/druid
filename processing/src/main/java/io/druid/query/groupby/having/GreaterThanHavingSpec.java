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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.Map;

/**
 * The "&gt;" operator in a "having" clause. This is similar to SQL's "having aggregation &gt; value",
 * except that an aggregation in SQL is an expression instead of an aggregation name as in Druid.
 */
public class GreaterThanHavingSpec extends BaseHavingSpec
{
  private final String aggregationName;
  private final Number value;

  private volatile Map<String, AggregatorFactory> aggregators;

  @JsonCreator
  public GreaterThanHavingSpec(
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
    return HavingSpecMetricComparator.compare(aggregationName, value, aggregators, metricVal) > 0;
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

    GreaterThanHavingSpec that = (GreaterThanHavingSpec) o;

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
    sb.append("GreaterThanHavingSpec");
    sb.append("{aggregationName='").append(aggregationName).append('\'');
    sb.append(", value=").append(value);
    sb.append('}');
    return sb.toString();
  }
}
