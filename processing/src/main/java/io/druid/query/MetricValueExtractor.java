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
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;

/**
 */
public class MetricValueExtractor
{
  private final Map<String, Object> value;

  @JsonCreator
  public MetricValueExtractor(
      Map<String, Object> value
  )
  {
    this.value = value;
  }

  @JsonValue
  public Map<String, Object> getBaseObject()
  {
    return value;
  }

  public Float getFloatMetric(String name)
  {
    final Object retVal = value.get(name);
    return retVal == null ? null : ((Number) retVal).floatValue();
  }

  public Double getDoubleMetric(String name)
  {
    final Object retVal = value.get(name);
    return retVal == null ? null : ((Number) retVal).doubleValue();
  }

  public Long getLongMetric(String name)
  {
    final Object retVal = value.get(name);
    return retVal == null ? null : ((Number) retVal).longValue();
  }

  public Object getMetric(String name)
  {
    return value.get(name);
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

    MetricValueExtractor that = (MetricValueExtractor) o;

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
    return "MetricValueExtractor{" +
           "value=" + value +
           '}';
  }
}
