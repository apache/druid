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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import java.util.function.Function;

/**
 */
public class Result<T> implements Comparable<Result<T>>
{
  public static String MISSING_SEGMENTS_KEY = "missingSegments";

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

  public <U> Result<U> map(Function<? super T, ? extends U> mapper)
  {
    return new Result<>(timestamp, mapper.apply(value));
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

    if (timestamp != null ? !(timestamp.isEqual(result.timestamp) && timestamp.getZone().getOffset(timestamp) == result.timestamp.getZone().getOffset(result.timestamp)) : result.timestamp != null) {
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
