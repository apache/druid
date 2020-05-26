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
import org.apache.druid.query.Queries;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 1. calculating zscore using two-sample Z-Test. IOW,
 * using z-test statistic for testing the difference of
 * two population proportions.
 * 2. converting binary variables (e.g. success or not) to continuous variables (e.g. conversion rate).
 * <p>
 * Please refer to http://math.mercyhurst.edu/~griff/courses/m109/Lectures/old/Sum_06/sect8.1.pdf
 * and http://facweb.cs.depaul.edu/sjost/csc423/documents/test-descriptions/indep-z.pdf
 * for more details.
 */
@JsonTypeName("zscore2sample")
public class ZtestPostAggregator implements PostAggregator
{
  private final String name;
  private final PostAggregator successCount1;
  private final PostAggregator sample1Size;
  private final PostAggregator successCount2;
  private final PostAggregator sample2Size;

  @JsonCreator
  public ZtestPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("successCount1") PostAggregator successCount1,
      @JsonProperty("sample1Size") PostAggregator sample1Size,
      @JsonProperty("successCount2") PostAggregator successCount2,
      @JsonProperty("sample2Size") PostAggregator sample2Size
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null post-aggregator name");
    Preconditions.checkNotNull(successCount1, "success count from sample 1 can not be null");
    Preconditions.checkNotNull(sample1Size, "sample size of population 1 can not null");
    Preconditions.checkNotNull(successCount2, "success count from sample 2 can not be null");
    Preconditions.checkNotNull(sample2Size, "sample size of population 2 can not be null");

    this.name = name;
    this.successCount1 = successCount1;
    this.sample1Size = sample1Size;
    this.successCount2 = successCount2;
    this.sample2Size = sample2Size;
  }

  @Override
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = new LinkedHashSet<>();
    dependentFields.addAll(successCount1.getDependentFields());
    dependentFields.addAll(sample1Size.getDependentFields());
    dependentFields.addAll(successCount2.getDependentFields());
    dependentFields.addAll(sample2Size.getDependentFields());

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
    Object sc1 = successCount1.compute(combinedAggregators);
    Object ss1 = sample1Size.compute(combinedAggregators);
    Object sc2 = successCount2.compute(combinedAggregators);
    Object ss2 = sample2Size.compute(combinedAggregators);
    if (!(sc1 instanceof Number) || !(sc2 instanceof Number) || !(ss1 instanceof Number) || !(ss2 instanceof Number)) {
      return null;
    }
    return zScoreTwoSamples(
        ((Number) sc1).doubleValue(),
        ((Number) ss1).doubleValue(),
        ((Number) sc2).doubleValue(),
        ((Number) ss2).doubleValue()
    );
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public ZtestPostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return new ZtestPostAggregator(
        name,
        Iterables
            .getOnlyElement(Queries.decoratePostAggregators(Collections.singletonList(successCount1), aggregators)),
        Iterables.getOnlyElement(Queries.decoratePostAggregators(Collections.singletonList(sample1Size), aggregators)),
        Iterables
            .getOnlyElement(Queries.decoratePostAggregators(Collections.singletonList(successCount2), aggregators)),
        Iterables.getOnlyElement(Queries.decoratePostAggregators(Collections.singletonList(sample2Size), aggregators))
    );
  }

  /**
   * 1. calculating zscore for two-sample Z test. IOW, using z-test statistic
   * for testing the difference of two population proportions. 2. converting
   * binary variables (e.g. success or not) to continuous variables (e.g.
   * conversion rate).
   *
   * @param s1count - success count of population 1
   * @param p1count - sample size of population 1
   * @param s2count - the success count of population 2
   * @param p2count - sample size of population 2
   */
  private double zScoreTwoSamples(double s1count, double p1count, double s2count, double p2count)
  {
    double convertRate1;
    double convertRate2;
    Preconditions.checkState(s1count >= 0, "success count can't be negative.");
    Preconditions.checkState(s2count >= 0, "success count can't be negative.");
    Preconditions.checkState(p1count >= s1count, "sample size can't be smaller than the success count.");
    Preconditions.checkState(p2count >= s2count, "sample size can't be smaller than the success count.");

    try {
      convertRate1 = s1count / p1count;
      convertRate2 = s2count / p2count;

      return (convertRate1 - convertRate2) /
             Math.sqrt((convertRate1 * (1 - convertRate1) / p1count) +
                       (convertRate2 * (1 - convertRate2) / p2count));
    }
    catch (IllegalArgumentException ex) {
      return 0;
    }
  }

  @JsonProperty
  public PostAggregator getSuccessCount1()
  {
    return successCount1;
  }

  @JsonProperty
  public PostAggregator getSample1Size()
  {
    return sample1Size;
  }

  @JsonProperty
  public PostAggregator getSuccessCount2()
  {
    return successCount2;
  }

  @JsonProperty
  public PostAggregator getSample2Size()
  {
    return sample2Size;
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

    ZtestPostAggregator that = (ZtestPostAggregator) o;

    if (!name.equals(that.name)) {
      return false;
    }

    return (successCount1.equals(that.successCount1) &&
            sample1Size.equals(that.sample1Size) &&
            successCount2.equals(that.successCount2) && sample2Size.equals(that.sample2Size));
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + successCount1.hashCode();
    result = 31 * result + sample1Size.hashCode();
    result = 31 * result + successCount2.hashCode();
    result = 31 * result + sample2Size.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "ZtestPostAggregator{" +
           "name='" + name + '\'' +
           ", successCount1='" + successCount1 + '\'' +
           ", sample1Size='" + sample1Size + '\'' +
           ", successCount2='" + successCount2 + '\'' +
           ", sample2size='" + sample2Size +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(
        PostAggregatorIds.ZTEST)
        .appendCacheable(successCount1)
        .appendCacheable(sample1Size)
        .appendCacheable(successCount2)
        .appendCacheable(sample2Size)
        .build();
  }
}
