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

package io.druid.query.timeboundary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.metamx.common.IAE;
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
    } else if (val instanceof String) {
      return new DateTime(val);
    } else {
      throw new IAE("Cannot get time from type[%s]", val.getClass());
    }
  }
}
