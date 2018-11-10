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

package org.apache.druid.query.aggregation.teststats;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.druid.query.Queries;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@JsonTypeName("pvalue2tailedZtest")
public class PvaluefromZscorePostAggregator implements PostAggregator
{
  private final String name;
  private final PostAggregator zScore;

  @JsonCreator
  public PvaluefromZscorePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("zScore") PostAggregator zScore
  )
  {
    Preconditions.checkNotNull(
        name,
        "Must have a valid, non-null post-aggregator"
    );
    Preconditions.checkNotNull(
        zScore,
        "Must have a valid, non-null post-aggregator"
    );
    this.name = name;
    this.zScore = zScore;
  }

  @Override
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = new HashSet<>();

    dependentFields.addAll(zScore.getDependentFields());

    return dependentFields;
  }

  @Override
  public Comparator getComparator()
  {
    return ArithmeticPostAggregator.DEFAULT_COMPARATOR;
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {

    double zScoreValue = ((Number) zScore.compute(combinedAggregators))
        .doubleValue();

    zScoreValue = Math.abs(zScoreValue);
    return 2 * (1 - cumulativeProbability(zScoreValue));
  }

  private double cumulativeProbability(double x)
  {
    try {
      NormalDistribution normDist = new NormalDistribution();
      return normDist.cumulativeProbability(x);
    }
    catch (IllegalArgumentException ex) {
      return Double.NaN;
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
    return new PvaluefromZscorePostAggregator(
        name,
        Iterables.getOnlyElement(Queries.decoratePostAggregators(
            Collections.singletonList(zScore), aggregators))
    );
  }

  @JsonProperty
  public PostAggregator getZscore()
  {
    return zScore;
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

    PvaluefromZscorePostAggregator that = (PvaluefromZscorePostAggregator) o;

    if (!name.equals(that.name)) {
      return false;
    }

    return (zScore.equals(that.zScore));
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + zScore.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "PvaluefromZscorePostAggregator{" +
           "name='" + name + '\'' +
           ", zScore=" + zScore + '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.PVALUE_FROM_ZTEST)
        .appendCacheable(zScore).build();
  }
}
