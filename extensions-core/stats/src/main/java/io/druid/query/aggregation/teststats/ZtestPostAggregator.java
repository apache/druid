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
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.PostAggregatorIds;
import io.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
   * 1. calculating zscore using two-sample Z-Test. IOW,
   *    using z-test statistic for testing the difference of
   *    two population proportions.
   * 2. converting binary variables (e.g. success or not) to continuous variables (e.g. conversion rate).
   *
   Please refer to http://math.mercyhurst.edu/~griff/courses/m109/Lectures/old/Sum_06/sect8.1.pdf
   for more details.
   http://facweb.cs.depaul.edu/sjost/csc423/documents/test-descriptions/indep-z.pdf
*/
@JsonTypeName("zscore2sample")
public class ZtestPostAggregator implements PostAggregator {
  private final String name;
  private final List<PostAggregator> fields;

  @JsonCreator
  public ZtestPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fields") List<PostAggregator> fields
  ) {
    Preconditions.checkNotNull(name, "Must have a valid, non-null post-aggregator name");
    Preconditions.checkArgument(fields.size() == 4, "Must have 4 fields; " +
        "\"fields\": <total success count of sample 1>, <sample size of population 1>," +
        " <total success counts of sample 2>, <sample size of population 2>");
    this.name = name;
    this.fields = fields;
  }

  @Override
  public Set<String> getDependentFields() {
    Set<String> dependentFields = Sets.newLinkedHashSet();
    for (PostAggregator field : fields) {
      dependentFields.addAll(field.getDependentFields());
    }
    return dependentFields;
  }

  @Override
  public Comparator getComparator() {
    return ArithmeticPostAggregator.DEFAULT_COMPARATOR;
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators) {

    PostAggregator[] args = new PostAggregator[fields.size()];
    for (int j = 0; j < fields.size(); j++) {
      args[j] = fields.get(j);
    }

    return zScoreTwoSamples(
        ((Number) args[0].compute(combinedAggregators)).doubleValue(),
        ((Number) args[1].compute(combinedAggregators)).doubleValue(),
        ((Number) args[2].compute(combinedAggregators)).doubleValue(),
        ((Number) args[3].compute(combinedAggregators)).doubleValue());
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
  public List<PostAggregator> getFields() {
    return fields;
  }

  /**
   * 1. calculating zscore for two-sample Z test. IOW,
   *    using z-test statistic for testing the difference of two population proportions.
   * 2. converting binary variables (e.g. success or not) to continuous variables (e.g. conversion rate).
   *
   * @param the success count of population 1
   * @param param sample size of population 1
   * @param the success count of population 2
   * @param sample size of population 2
   */
  private double zScoreTwoSamples(Double s1count, Double p1count, Double s2count, Double p2count) {
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
    } catch (Exception ex) {
      return 0;
    }
  }

  @Override
  public String toString() {
    return "ZtestPostAggregator{" +
        "name='"
        + name
        + '\''
        + ", fields="
        + fields
        + "}";
  }

  @Override
  public byte[] getCacheKey() {
    return new CacheKeyBuilder(
        PostAggregatorIds.ZTEST)
        .appendCacheables(fields)
        .build();
  }
}
