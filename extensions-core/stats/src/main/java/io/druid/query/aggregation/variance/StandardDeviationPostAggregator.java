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

package io.druid.query.aggregation.variance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.PostAggregatorIds;
import io.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeName("stddev")
public class StandardDeviationPostAggregator implements PostAggregator
{
  protected final String name;
  protected final String fieldName;
  protected final String estimator;

  protected final boolean isVariancePop;

  @JsonCreator
  public StandardDeviationPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("estimator") String estimator
  )
  {
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName is null");
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.estimator = estimator;
    this.isVariancePop = VarianceAggregatorCollector.isVariancePop(estimator);
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Comparator<Double> getComparator()
  {
    return ArithmeticPostAggregator.DEFAULT_COMPARATOR;
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    return Math.sqrt(((VarianceAggregatorCollector) combinedAggregators.get(fieldName)).getVariance(isVariancePop));
  }

  @Override
  @JsonProperty("name")
  public String getName()
  {
    return name;
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @JsonProperty("fieldName")
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty("estimator")
  public String getEstimator()
  {
    return estimator;
  }

  @Override
  public String toString()
  {
    return "StandardDeviationPostAggregator{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", estimator='" + estimator + '\'' +
           ", isVariancePop=" + isVariancePop +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.VARIANCE_STANDARD_DEVIATION)
        .appendString(fieldName)
        .appendString(estimator)
        .appendBoolean(isVariancePop)
        .build();
  }
}
