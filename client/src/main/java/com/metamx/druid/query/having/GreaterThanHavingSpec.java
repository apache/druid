package com.metamx.druid.query.having;

import com.metamx.druid.input.Row;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * User: dyuan
 */
public class GreaterThanHavingSpec implements HavingSpec
{
  private String aggregationName;
  private Number value;

  @JsonCreator
  public GreaterThanHavingSpec(
    @JsonProperty("aggregation") String aggName,
    @JsonProperty("value") Number value)
  {
    this.aggregationName = aggName;
    this.value = value;
  }

  @JsonProperty("aggregation")
  public String getAggregationName()
  {
    return aggregationName;
  }

  /**
   * This method treats internal value as double mainly for ease of test.
   */
  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GreaterThanHavingSpec that = (GreaterThanHavingSpec) o;

    if(value != null && that.value != null) {
      return Double.compare(value.doubleValue(), that.value.doubleValue()) == 0;
    }

    if(value == null && that.value == null) return true;

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

  @JsonProperty("value")
  public Number getValue()
  {
    return value;
  }

  @Override
  public boolean eval(Row row)
  {
    float metricValue = row.getFloatMetric(aggregationName);

    return Float.compare(metricValue, value.floatValue()) > 0;
  }
}
