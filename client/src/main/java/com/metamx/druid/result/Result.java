package com.metamx.druid.result;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonUnwrapped;
import org.joda.time.DateTime;

/**
 */
public class Result<T> implements Comparable<Result<T>>
{
  private final DateTime timestamp;
  private final T value;

  @JsonCreator
  public Result(
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("result") T value
  )
  {
    this.timestamp = timestamp;
    this.value = value;
  }

  @Override
  public int compareTo(Result<T> tResult)
  {
    return timestamp.compareTo(tResult.timestamp);
  }

  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty("result")
  public T getValue()
  {
    return value;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Result result = (Result) o;

    if (timestamp != null ? !timestamp.equals(result.timestamp) : result.timestamp != null) {
      return false;
    }
    if (value != null ? !value.equals(result.value) : result.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = timestamp != null ? timestamp.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Result{" +
           "timestamp=" + timestamp +
           ", value=" + value +
           '}';
  }
}