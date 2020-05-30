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

package org.apache.druid.query.aggregation.momentsketch.aggregator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.momentsketch.MomentSketchWrapper;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class MomentSketchMaxPostAggregator implements PostAggregator
{
  private final String name;
  private final PostAggregator field;

  public static final String TYPE_NAME = "momentSketchMax";

  @JsonCreator
  public MomentSketchMaxPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field
  )
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
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

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    final MomentSketchWrapper sketch = (MomentSketchWrapper) field.compute(combinedAggregators);
    return sketch.getMax();
  }

  @Override
  public Comparator<Double> getComparator()
  {
    return Doubles::compare;
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
    final MomentSketchMaxPostAggregator that = (MomentSketchMaxPostAggregator) o;
    if (!name.equals(that.name)) {
      return false;
    }
    return field.equals(that.field);
  }

  @Override
  public int hashCode()
  {
    return (name.hashCode() * 31 + field.hashCode());
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(
        PostAggregatorIds.MOMENTS_SKETCH_TO_MAX_CACHE_TYPE_ID
    ).appendCacheable(field);
    return builder.build();
  }

  @Override
  public PostAggregator decorate(final Map<String, AggregatorFactory> map)
  {
    return this;
  }
}
