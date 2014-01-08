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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNResultValue implements Iterable<DimensionAndMetricValueExtractor>
{
  private final List<DimensionAndMetricValueExtractor> value;

  @JsonCreator
  public TopNResultValue(
      List<?> value
  )
  {
    this.value = (value == null) ? Lists.<DimensionAndMetricValueExtractor>newArrayList() : Lists.transform(
        value,
        new Function<Object, DimensionAndMetricValueExtractor>()
        {
          @Override
          public DimensionAndMetricValueExtractor apply(@Nullable Object input)
          {
            if (input instanceof Map) {
              return new DimensionAndMetricValueExtractor((Map) input);
            } else if (input instanceof DimensionAndMetricValueExtractor) {
              return (DimensionAndMetricValueExtractor) input;
            } else {
              throw new IAE("Unknown type for input[%s]", input.getClass());
            }
          }
        }
    );
  }

  @JsonValue
  public List<DimensionAndMetricValueExtractor> getValue()
  {
    return value;
  }

  @Override
  public Iterator<DimensionAndMetricValueExtractor> iterator()
  {
    return value.iterator();
  }

  @Override
  public String toString()
  {
    return "TopNResultValue{" +
           "value=" + value +
           '}';
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

    TopNResultValue that = (TopNResultValue) o;

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
}
