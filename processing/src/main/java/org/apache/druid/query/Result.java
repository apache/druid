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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.guice.annotations.PublicApi;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Function;

/**
 * JSON-serializable row in the form of a (timestamp, value), where the value an be
 * of any type. {@code Result}s are equal based on the timezone-adjusted timestamp,
 * and equality of the value, and comparable based only on timestamp.
 */
@PublicApi
public class Result<T> implements Comparable<Result<T>>
{
  private final DateTime timestamp;
  private final T value;

  @JsonCreator
  public Result(@JsonProperty("timestamp") @Nullable DateTime timestamp, @JsonProperty("result") T value)
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
    // timestamp is null for grandTotal which should come last.
    return Comparator.nullsLast(DateTime::compareTo).compare(this.timestamp, tResult.timestamp);
  }

  @JsonProperty
  @Nullable
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

    @SuppressWarnings("unchecked")
    Result<T> result = (Result<T>) o;

    if (timestamp != null && result.timestamp != null) {
      if (!timestamp.isEqual(result.timestamp)
          && timestamp.getZone().getOffset(timestamp) == result.timestamp.getZone().getOffset(result.timestamp)) {
        return false;
      }
    } else if (timestamp == null ^ result.timestamp == null) {
      return false;
    }

    return Objects.equals(value, result.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timestamp, value);
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
