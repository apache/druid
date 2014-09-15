/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import io.druid.data.input.Row;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * The "=" operator in a "having" clause. This is similar to SQL's "having aggregation = value",
 * except that in SQL an aggregation is an expression instead of an aggregation name as in Druid.
 */
public class EqualToHavingSpec implements HavingSpec
{
  private static final byte CACHE_KEY = 0x3;

  private String aggregationName;
  private Number value;

  @JsonCreator
  public EqualToHavingSpec(
      @JsonProperty("aggregation") String aggName,
      @JsonProperty("value") Number value
  )
  {
    this.aggregationName = aggName;
    this.value = value;
  }

  @JsonProperty("value")
  public Number getValue()
  {
    return value;
  }

  @JsonProperty("aggregation")
  public String getAggregationName()
  {
    return aggregationName;
  }

  @Override
  public boolean eval(Row row)
  {
    float metricValue = row.getFloatMetric(aggregationName);

    return Float.compare(value.floatValue(), metricValue) == 0;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] aggBytes = aggregationName.getBytes(Charsets.UTF_8);
    final byte[] valBytes = Bytes.toArray(Arrays.asList(value));
    return ByteBuffer.allocate(1 + aggBytes.length + valBytes.length)
                     .put(CACHE_KEY)
                     .put(aggBytes)
                     .put(valBytes)
                     .array();
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

    EqualToHavingSpec that = (EqualToHavingSpec) o;

    if (aggregationName != null ? !aggregationName.equals(that.aggregationName) : that.aggregationName != null) {
      return false;
    }

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
    sb.append("EqualToHavingSpec");
    sb.append("{aggregationName='").append(aggregationName).append('\'');
    sb.append(", value=").append(value);
    sb.append('}');
    return sb.toString();
  }
}
