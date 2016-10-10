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

package io.druid.jackson;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.hash.Hashing;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.HistogramAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.aggregation.post.JavaScriptPostAggregator;
import io.druid.query.aggregation.post.ExpressionPostAggregator;
import io.druid.segment.serde.ComplexMetrics;

/**
 */
public class AggregatorsModule extends SimpleModule
{
  public AggregatorsModule()
  {
    super("AggregatorFactories");

    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(Hashing.murmur3_128()));
    }

    setMixInAnnotation(AggregatorFactory.class, AggregatorFactoryMixin.class);
    setMixInAnnotation(PostAggregator.class, PostAggregatorMixin.class);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "count", value = CountAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longSum", value = LongSumAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleSum", value = DoubleSumAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleMax", value = DoubleMaxAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleMin", value = DoubleMinAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longMax", value = LongMaxAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longMin", value = LongMinAggregatorFactory.class),
      @JsonSubTypes.Type(name = "javascript", value = JavaScriptAggregatorFactory.class),
      @JsonSubTypes.Type(name = "histogram", value = HistogramAggregatorFactory.class),
      @JsonSubTypes.Type(name = "hyperUnique", value = HyperUniquesAggregatorFactory.class),
      @JsonSubTypes.Type(name = "cardinality", value = CardinalityAggregatorFactory.class),
      @JsonSubTypes.Type(name = "filtered", value = FilteredAggregatorFactory.class)
  })
  public static interface AggregatorFactoryMixin
  {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "expression", value = ExpressionPostAggregator.class),
      @JsonSubTypes.Type(name = "arithmetic", value = ArithmeticPostAggregator.class),
      @JsonSubTypes.Type(name = "fieldAccess", value = FieldAccessPostAggregator.class),
      @JsonSubTypes.Type(name = "constant", value = ConstantPostAggregator.class),
      @JsonSubTypes.Type(name = "javascript", value = JavaScriptPostAggregator.class),
      @JsonSubTypes.Type(name = "hyperUniqueCardinality", value = HyperUniqueFinalizingPostAggregator.class)
  })
  public static interface PostAggregatorMixin
  {
  }
}
