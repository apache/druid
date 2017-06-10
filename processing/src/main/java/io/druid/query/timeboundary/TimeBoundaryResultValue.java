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

package io.druid.query.timeboundary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.druid.java.util.common.IAE;
import org.joda.time.DateTime;

import java.util.Map;

/**
 */
public class TimeBoundaryResultValue
{
  private final Object value;

  @JsonCreator
  public TimeBoundaryResultValue(
      Object value
  )
  {
    this.value = value;
  }

  @JsonValue
  public Object getBaseObject()
  {
    return value;
  }

  public DateTime getMaxTime()
  {
    if (value instanceof Map) {
      return getDateTimeValue(((Map) value).get(TimeBoundaryQuery.MAX_TIME));
    } else {
      return getDateTimeValue(value);
    }
  }

  public DateTime getMinTime()
  {
    if (value instanceof Map) {
      return getDateTimeValue(((Map) value).get(TimeBoundaryQuery.MIN_TIME));
    } else {
      throw new IAE("MinTime not supported!");
    }
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

    TimeBoundaryResultValue that = (TimeBoundaryResultValue) o;

    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return value != null ? value.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return "TimeBoundaryResultValue{" +
           "value=" + value +
           '}';
  }

  private DateTime getDateTimeValue(Object val)
  {
    if (val == null) {
      return null;
    }

    if (val instanceof DateTime) {
      return (DateTime) val;
    } else if (val instanceof String || val instanceof Long) {
      return new DateTime(val);
    } else {
      throw new IAE("Cannot get time from type[%s]", val.getClass());
    }
  }
}
