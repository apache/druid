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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
