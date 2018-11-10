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

package org.apache.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LegacyTopNMetricSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "numeric", value = NumericTopNMetricSpec.class),
    @JsonSubTypes.Type(name = "lexicographic", value = LexicographicTopNMetricSpec.class),
    @JsonSubTypes.Type(name = "alphaNumeric", value = AlphaNumericTopNMetricSpec.class),
    @JsonSubTypes.Type(name = "inverted", value = InvertedTopNMetricSpec.class),
    @JsonSubTypes.Type(name = "dimension", value = DimensionTopNMetricSpec.class),
})
public interface TopNMetricSpec extends Cacheable
{
  void verifyPreconditions(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs);

  Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs);

  TopNResultBuilder getResultBuilder(
      DateTime timestamp,
      DimensionSpec dimSpec,
      int threshold,
      Comparator comparator,
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  );

  <T> TopNMetricSpecBuilder<T> configureOptimizer(TopNMetricSpecBuilder<T> builder);

  void initTopNAlgorithmSelector(TopNAlgorithmSelector selector);

  String getMetricName(DimensionSpec dimSpec);

  boolean canBeOptimizedUnordered();
}
