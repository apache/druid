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

package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.PostAggregatorIds;
import io.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

@JsonTypeName("quantile")
public class QuantilePostAggregator extends ApproximateHistogramPostAggregator
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Double.compare(((Number) o).doubleValue(), ((Number) o1).doubleValue());
    }
  };

  private final float probability;
  private final String fieldName;

  @JsonCreator
  public QuantilePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("probability") float probability
  )
  {
    super(name, fieldName);
    this.probability = probability;
    this.fieldName = fieldName;

    if (probability < 0 || probability > 1) {
      throw new IAE("Illegal probability[%s], must be strictly between 0 and 1", probability);
    }
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Object compute(Map<String, Object> values)
  {
    final ApproximateHistogram ah = (ApproximateHistogram) values.get(fieldName);
    return ah.getQuantiles(new float[]{probability})[0];
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @JsonProperty
  public float getProbability()
  {
    return probability;
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

    final QuantilePostAggregator that = (QuantilePostAggregator) o;

    if (Float.compare(that.probability, probability) != 0) {
      return false;
    }
    return fieldName != null ? fieldName.equals(that.fieldName) : that.fieldName == null;
  }

  @Override
  public int hashCode()
  {
    int result = (probability != +0.0f ? Float.floatToIntBits(probability) : 0);
    result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "QuantilePostAggregator{" +
           "probability=" + probability +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.HISTOGRAM_QUANTILE)
        .appendString(fieldName)
        .appendFloat(probability)
        .build();
  }
}
