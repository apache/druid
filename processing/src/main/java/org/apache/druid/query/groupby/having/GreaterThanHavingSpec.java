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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;

import java.util.Map;

/**
 * The "&gt;" operator in a "having" clause. This is similar to SQL's "having aggregation &gt; value",
 * except that an aggregation in SQL is an expression instead of an aggregation name as in Druid.
 */
public class GreaterThanHavingSpec implements HavingSpec
{
  private final String aggregationName;
  private final Number value;

  private volatile Map<String, AggregatorFactory> aggregators;
  private volatile int columnNumber;

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
  public void setQuery(GroupByQuery query)
  {
    columnNumber = query.getResultRowPositionLookup().getInt(aggregationName);
    aggregators = HavingSpecUtil.computeAggregatorsMap(query.getAggregatorSpecs());
  }

  @Override
  public boolean eval(ResultRow row)
  {
    if (columnNumber < 0) {
      return false;
    }

    Object metricVal = row.get(columnNumber);
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

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(HavingSpecUtil.CACHE_TYPE_ID_GREATER_THAN)
        .appendString(aggregationName)
        .appendByteArray(StringUtils.toUtf8(String.valueOf(value)))
        .build();
  }
}
