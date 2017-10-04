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
import io.druid.hll.HyperLogLogHash;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.FloatMaxAggregatorFactory;
import io.druid.query.aggregation.FloatMinAggregatorFactory;
import io.druid.query.aggregation.FloatSumAggregatorFactory;
import io.druid.query.aggregation.HistogramAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import io.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import io.druid.query.aggregation.first.LongFirstAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.aggregation.hyperloglog.PreComputedHyperUniquesSerde;
import io.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import io.druid.query.aggregation.last.FloatLastAggregatorFactory;
import io.druid.query.aggregation.last.LongLastAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.DoubleGreatestPostAggregator;
import io.druid.query.aggregation.post.DoubleLeastPostAggregator;
import io.druid.query.aggregation.post.ExpressionPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import io.druid.query.aggregation.post.JavaScriptPostAggregator;
import io.druid.query.aggregation.post.LongGreatestPostAggregator;
import io.druid.query.aggregation.post.LongLeastPostAggregator;
import io.druid.segment.serde.ComplexMetrics;

/**
 */
public class AggregatorsModule extends SimpleModule
{
  public AggregatorsModule()
  {
    super("AggregatorFactories");

    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(HyperLogLogHash.getDefault()));
    }

    if (ComplexMetrics.getSerdeForType("preComputedHyperUnique") == null) {
      ComplexMetrics.registerSerde("preComputedHyperUnique", new PreComputedHyperUniquesSerde(HyperLogLogHash.getDefault()));
    }

    setMixInAnnotation(AggregatorFactory.class, AggregatorFactoryMixin.class);
    setMixInAnnotation(PostAggregator.class, PostAggregatorMixin.class);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "count", value = CountAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longSum", value = LongSumAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleSum", value = DoubleSumAggregatorFactory.class),
      @JsonSubTypes.Type(name = "floatSum", value = FloatSumAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleMax", value = DoubleMaxAggregatorFactory.class),
      @JsonSubTypes.Type(name = "floatMin", value = FloatMinAggregatorFactory.class),
      @JsonSubTypes.Type(name = "floatMax", value = FloatMaxAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleMin", value = DoubleMinAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longMax", value = LongMaxAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longMin", value = LongMinAggregatorFactory.class),
      @JsonSubTypes.Type(name = "javascript", value = JavaScriptAggregatorFactory.class),
      @JsonSubTypes.Type(name = "histogram", value = HistogramAggregatorFactory.class),
      @JsonSubTypes.Type(name = "hyperUnique", value = HyperUniquesAggregatorFactory.class),
      @JsonSubTypes.Type(name = "cardinality", value = CardinalityAggregatorFactory.class),
      @JsonSubTypes.Type(name = "filtered", value = FilteredAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longFirst", value = LongFirstAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleFirst", value = DoubleFirstAggregatorFactory.class),
      @JsonSubTypes.Type(name = "floatFirst", value = FloatFirstAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longLast", value = LongLastAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleLast", value = DoubleLastAggregatorFactory.class),
      @JsonSubTypes.Type(name = "floatLast", value = FloatLastAggregatorFactory.class)
  })
  public static interface AggregatorFactoryMixin
  {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "expression", value = ExpressionPostAggregator.class),
      @JsonSubTypes.Type(name = "arithmetic", value = ArithmeticPostAggregator.class),
      @JsonSubTypes.Type(name = "fieldAccess", value = FieldAccessPostAggregator.class),
      @JsonSubTypes.Type(name = "finalizingFieldAccess", value = FinalizingFieldAccessPostAggregator.class),
      @JsonSubTypes.Type(name = "constant", value = ConstantPostAggregator.class),
      @JsonSubTypes.Type(name = "javascript", value = JavaScriptPostAggregator.class),
      @JsonSubTypes.Type(name = "hyperUniqueCardinality", value = HyperUniqueFinalizingPostAggregator.class),
      @JsonSubTypes.Type(name = "doubleGreatest", value = DoubleGreatestPostAggregator.class),
      @JsonSubTypes.Type(name = "doubleLeast", value = DoubleLeastPostAggregator.class),
      @JsonSubTypes.Type(name = "longGreatest", value = LongGreatestPostAggregator.class),
      @JsonSubTypes.Type(name = "longLeast", value = LongLeastPostAggregator.class)
  })
  public static interface PostAggregatorMixin
  {
  }
}
