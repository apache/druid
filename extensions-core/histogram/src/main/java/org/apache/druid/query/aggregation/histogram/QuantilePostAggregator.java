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

package org.apache.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonTypeName("quantile")
public class QuantilePostAggregator extends ApproximateHistogramPostAggregator
{
  // this doesn't need to handle nulls because the values come from ApproximateHistogram
  static final Comparator COMPARATOR = Comparator.comparingDouble(o -> ((Number) o).doubleValue());

  private final float probability;

  @JsonCreator
  public QuantilePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("probability") float probability
  )
  {
    super(name, fieldName);
    this.probability = probability;

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
    Object value = values.get(fieldName);
    if (value instanceof ApproximateHistogram) {
      final ApproximateHistogram ah = (ApproximateHistogram) value;
      return ah.getQuantiles(new float[]{probability})[0];
    } else {
      final FixedBucketsHistogram fbh = (FixedBucketsHistogram) value;
      float x = fbh.percentilesFloat(new double[]{probability * 100.0})[0];
      return x;
    }
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
  public String toString()
  {
    return "QuantilePostAggregator{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", probability=" + probability +
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    QuantilePostAggregator that = (QuantilePostAggregator) o;
    return Float.compare(that.probability, probability) == 0;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), probability);
  }
}
