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

package io.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.PostAggregatorIds;
import io.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
public class SketchEstimatePostAggregator implements PostAggregator
{

  private final String name;
  private final PostAggregator field;
  private final Integer errorBoundsStdDev;

  @JsonCreator
  public SketchEstimatePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("field") PostAggregator field,
      @JsonProperty("errorBoundsStdDev") Integer errorBoundsStdDev
  )
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
    this.errorBoundsStdDev = errorBoundsStdDev;
  }

  @Override
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = Sets.newHashSet();
    dependentFields.addAll(field.getDependentFields());
    return dependentFields;
  }

  @Override
  public Comparator getComparator()
  {
    if (errorBoundsStdDev == null) {
      return Ordering.natural();
    } else {
      return new Comparator()
      {
        @Override
        public int compare(Object o1, Object o2)
        {
          return Doubles.compare(
              ((SketchEstimateWithErrorBounds) o1).getEstimate(),
              ((SketchEstimateWithErrorBounds) o2).getEstimate()
          );
        }
      };
    }
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    SketchHolder holder = (SketchHolder)field.compute(combinedAggregators);
    if (errorBoundsStdDev != null) {
      return holder.getEstimateWithErrorBounds(errorBoundsStdDev);
    } else {
      return holder.getEstimate();
    }
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }

  @JsonProperty
  public Integer getErrorBoundsStdDev()
  {
    return errorBoundsStdDev;
  }

  @Override
  public String toString()
  {
    return "SketchEstimatePostAggregator{" +
        "name='" + name + '\'' +
        ", field=" + field +
        ", errorBoundsStdDev=" + errorBoundsStdDev +
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

    SketchEstimatePostAggregator that = (SketchEstimatePostAggregator) o;

    if (!name.equals(that.name)) {
      return false;
    }
    
    if (errorBoundsStdDev == null ^ that.errorBoundsStdDev == null) {
      // one of the two stddevs (not both) are null
      return false;
    }
    
    if (errorBoundsStdDev != null && that.errorBoundsStdDev != null && 
        errorBoundsStdDev.intValue() != that.errorBoundsStdDev.intValue()) {
      // neither stddevs are null, Integer values don't match
      return false;
    }

    return field.equals(that.field);

  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + field.hashCode();
    result = 31 * result + (errorBoundsStdDev != null ? errorBoundsStdDev.hashCode() : 0);
    return result;
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(PostAggregatorIds.DATA_SKETCHES_SKETCH_ESTIMATE)
        .appendCacheable(field);
    return errorBoundsStdDev == null ? builder.build() : builder.appendInt(errorBoundsStdDev).build();
  }
}
