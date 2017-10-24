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

package io.druid.query.aggregation.datasketches.quantiles;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.cache.CacheKeyBuilder;

import com.yahoo.sketches.quantiles.DoublesSketch;

public class DoublesSketchQuantilePostAggregator implements PostAggregator
{

  private static final byte CACHE_KEY = 113;

  private final String name;
  private final PostAggregator field;
  private final double fraction;

  @JsonCreator
  public DoublesSketchQuantilePostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("fraction") final double fraction)
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
    this.fraction = fraction;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }

  @JsonProperty
  public double getFraction()
  {
    return fraction;
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    final DoublesSketch sketch = (DoublesSketch) field.compute(combinedAggregators);
    return sketch.getQuantile(fraction);
  }

  @Override
  public Comparator<Double> getComparator()
  {
    return new Comparator<Double>()
    {
      @Override
      public int compare(Double a, Double b)
      {
        return Doubles.compare(a, b);
      }
    };
  }

  @Override
  public Set<String> getDependentFields()
  {
    return field.getDependentFields();
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
        "name='" + name + '\'' +
        ", field=" + field +
        ", fraction=" + fraction +
        "}";
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DoublesSketchQuantilePostAggregator that = (DoublesSketchQuantilePostAggregator) o;
    if (!name.equals(that.name)) {
      return false;
    }
    if (fraction != that.fraction) {
      return false;
    }
    return field.equals(that.field);
  }

  @Override
  public int hashCode()
  {
    return (name.hashCode() * 31 + field.hashCode()) * 31 + Double.hashCode(fraction);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(CACHE_KEY).appendCacheable(field).appendDouble(fraction).build();
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> map)
  {
    return this;
  }

}
