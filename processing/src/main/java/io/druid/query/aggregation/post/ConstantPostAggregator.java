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

package io.druid.query.aggregation.post;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
public class ConstantPostAggregator implements PostAggregator
{
  private final String name;
  private final Number constantValue;

  @JsonCreator
  public ConstantPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("value") Number constantValue,
      @JsonProperty("constantValue") Number backwardsCompatibleValue
  )
  {
    this.name = name;
    this.constantValue = constantValue == null ? backwardsCompatibleValue : constantValue;
    Preconditions.checkNotNull(this.constantValue);
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet();
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator()
    {
      @Override
      public int compare(Object o1, Object o2)
      {
        return 0;
      }
    };
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    return constantValue;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty("value")
  public Number getConstantValue()
  {
    return constantValue;
  }

  @Override
  public String toString()
  {
    return "ConstantPostAggregator{" +
           "name='" + name + '\'' +
           ", constantValue=" + constantValue +
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

    ConstantPostAggregator that = (ConstantPostAggregator) o;

    if (constantValue != null && that.constantValue != null) {
      if (constantValue.doubleValue() != that.constantValue.doubleValue()) {
        return false;
      }
    } else if (constantValue != that.constantValue) {
      return false;
    }

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (constantValue != null ? constantValue.hashCode() : 0);
    return result;
  }

}
