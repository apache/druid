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

package org.apache.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.primitives.Doubles;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.column.ValueType;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DoublesSketchToRankPostAggregator implements PostAggregator
{

  private final String name;
  private final PostAggregator field;
  private final double value;

  @JsonCreator
  public DoublesSketchToRankPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("value") final double value)
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
    this.value = value;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public ValueType getType()
  {
    return ValueType.DOUBLE;
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }

  @JsonProperty
  public double getValue()
  {
    return value;
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    final DoublesSketch sketch = (DoublesSketch) field.compute(combinedAggregators);
    return sketch.getRank(value);
  }

  @Override
  public Comparator<Double> getComparator()
  {
    return Doubles::compare;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.QUANTILES_DOUBLES_SKETCH_TO_RANK_CACHE_TYPE_ID)
        .appendCacheable(field).appendDouble(value).build();
  }

  @Override
  public PostAggregator decorate(final Map<String, AggregatorFactory> map)
  {
    return this;
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
        ", value=" + value +
        "}";
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
    DoublesSketchToRankPostAggregator that = (DoublesSketchToRankPostAggregator) o;
    return Double.compare(that.value, value) == 0 &&
           name.equals(that.name) &&
           field.equals(that.field);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field, value);
  }
}
