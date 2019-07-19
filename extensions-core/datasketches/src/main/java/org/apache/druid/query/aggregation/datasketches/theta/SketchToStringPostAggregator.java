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

package org.apache.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Returns a human-readable summary of a given Theta sketch.
 * This is a string returned by toString() method of the sketch.
 * This can be useful for debugging.
 */
public class SketchToStringPostAggregator implements PostAggregator
{

  private final String name;
  private final PostAggregator field;

  @JsonCreator
  public SketchToStringPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field
  )
  {
    this.name = name;
    this.field = field;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return field.getDependentFields();
  }

  @Override
  public Comparator<String> getComparator()
  {
    return Comparator.nullsFirst(Comparator.naturalOrder());
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    final SketchHolder holder = (SketchHolder) field.compute(combinedAggregators);
    return holder.getSketch().toString();
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public PostAggregator decorate(final Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
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
    if (!(o instanceof SketchToStringPostAggregator)) {
      return false;
    }

    final SketchToStringPostAggregator that = (SketchToStringPostAggregator) o;

    if (!name.equals(that.name)) {
      return false;
    }

    return field.equals(that.field);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.THETA_SKETCH_TO_STRING)
        .appendString(name)
        .appendCacheable(field)
        .build();
  }

}
