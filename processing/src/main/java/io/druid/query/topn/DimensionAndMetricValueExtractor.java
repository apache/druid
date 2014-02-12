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

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.druid.query.MetricValueExtractor;

import java.util.Map;

/**
 */
public class DimensionAndMetricValueExtractor extends MetricValueExtractor
{
  private final Map<String, Object> value;

  @JsonCreator
  public DimensionAndMetricValueExtractor(Map<String, Object> value)
  {
    super(value);

    this.value = value;
  }

  public String getStringDimensionValue(String dimension)
  {
    return (String) value.get(dimension);
  }

  public Object getDimensionValue(String dimension)
  {
    return value.get(dimension);
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

    DimensionAndMetricValueExtractor that = (DimensionAndMetricValueExtractor) o;

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
    return "DimensionAndMetricValueExtractor{" +
           "value=" + value +
           '}';
  }
}
