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

package io.druid.query.aggregation.teststats;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.PostAggregatorIds;
import io.druid.query.cache.CacheKeyBuilder;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * Created by chunchen on 4/5/17.
 */
@JsonTypeName("pvalue2tailedztest")
public class PvaluefromZscorePostAggregator implements PostAggregator {
  private final String name;
  private final PostAggregator field;

  @JsonCreator
  public PvaluefromZscorePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("field") PostAggregator field
  ) {
    Preconditions.checkNotNull(name, "Must have a valid, non-null post-aggregator");
    this.name = name;
    this.field = field;
  }

  @Override
  public Set<String> getDependentFields() {

    Set<String> dependentFields = Sets.newHashSet();

    dependentFields.addAll(field.getDependentFields());

    return dependentFields;
  }

  @Override
  public Comparator getComparator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators) {

    double zScore =
        ((Number) field.compute(combinedAggregators)).doubleValue();

    zScore = Math.abs(zScore);
    return 2 * (1 - cumulativeProbability(zScore));
  }

  private double cumulativeProbability(double x) {
    try {
      NormalDistribution normDist = new NormalDistribution();
      return normDist.cumulativeProbability(x);
    } catch (Exception ex) {
      return Double.NaN;
    }
  }

  @Override
  @JsonProperty
  public String getName() {
    return name;
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators) {
    return this;
  }

  @JsonProperty
  public PostAggregator getField() {
    return field;
  }

  @Override
  public String toString() {
    return "PvaluefromZscorePostAggregator{" +
        "name'" + name + '\'' +
        ", field=" + field +
        "}";
  }

  @Override
  public byte[] getCacheKey() {
    return new CacheKeyBuilder(
        PostAggregatorIds.PVALUE_FROM_ZTEST)
        .appendCacheable(field)
        .build();
  }
}
