package com.metamx.druid.query.group.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.druid.input.Row;

/**
 * The "=" operator in a "having" clause. This is similar to SQL's "having aggregation = value",
 * except that in SQL an aggregation is an expression instead of an aggregation name as in Druid.
 */
public class EqualToHavingSpec implements HavingSpec
{
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
