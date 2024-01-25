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

package org.apache.druid.jackson;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMaxAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.GroupingAggregatorFactory;
import org.apache.druid.query.aggregation.HistogramAggregatorFactory;
import org.apache.druid.query.aggregation.JavaScriptAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongDoubleComplexMetricSerde;
import org.apache.druid.query.aggregation.SerializablePairLongFloatComplexMetricSerde;
import org.apache.druid.query.aggregation.SerializablePairLongLongComplexMetricSerde;
import org.apache.druid.query.aggregation.SerializablePairLongStringComplexMetricSerde;
import org.apache.druid.query.aggregation.SingleValueAggregatorFactory;
import org.apache.druid.query.aggregation.any.DoubleAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.FloatAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.LongAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.StringAnyAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstFoldingAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.aggregation.hyperloglog.PreComputedHyperUniquesSerde;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.FloatLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastFoldingAggregatorFactory;
import org.apache.druid.query.aggregation.mean.DoubleMeanAggregatorFactory;
import org.apache.druid.query.aggregation.mean.DoubleMeanHolder;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.DoubleGreatestPostAggregator;
import org.apache.druid.query.aggregation.post.DoubleLeastPostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.JavaScriptPostAggregator;
import org.apache.druid.query.aggregation.post.LongGreatestPostAggregator;
import org.apache.druid.query.aggregation.post.LongLeastPostAggregator;
import org.apache.druid.segment.serde.ComplexMetrics;

public class AggregatorsModule extends SimpleModule
{
  public AggregatorsModule()
  {
    super("AggregatorFactories");

    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());
    ComplexMetrics.registerSerde(PreComputedHyperUniquesSerde.TYPE_NAME, new PreComputedHyperUniquesSerde());
    ComplexMetrics.registerSerde(
        SerializablePairLongStringComplexMetricSerde.TYPE_NAME,
        new SerializablePairLongStringComplexMetricSerde()
    );

    ComplexMetrics.registerSerde(
        SerializablePairLongFloatComplexMetricSerde.TYPE_NAME,
        new SerializablePairLongFloatComplexMetricSerde()
    );
    ComplexMetrics.registerSerde(
        SerializablePairLongDoubleComplexMetricSerde.TYPE_NAME,
        new SerializablePairLongDoubleComplexMetricSerde()
    );
    ComplexMetrics.registerSerde(
        SerializablePairLongLongComplexMetricSerde.TYPE_NAME,
        new SerializablePairLongLongComplexMetricSerde()
    );

    setMixInAnnotation(AggregatorFactory.class, AggregatorFactoryMixin.class);
    setMixInAnnotation(PostAggregator.class, PostAggregatorMixin.class);

    addSerializer(DoubleMeanHolder.class, DoubleMeanHolder.Serializer.INSTANCE);
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
      @JsonSubTypes.Type(name = "stringFirst", value = StringFirstAggregatorFactory.class),
      @JsonSubTypes.Type(name = "stringFirstFold", value = StringFirstFoldingAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longLast", value = LongLastAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleLast", value = DoubleLastAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleMean", value = DoubleMeanAggregatorFactory.class),
      @JsonSubTypes.Type(name = "floatLast", value = FloatLastAggregatorFactory.class),
      @JsonSubTypes.Type(name = "stringLast", value = StringLastAggregatorFactory.class),
      @JsonSubTypes.Type(name = "stringLastFold", value = StringLastFoldingAggregatorFactory.class),
      @JsonSubTypes.Type(name = "longAny", value = LongAnyAggregatorFactory.class),
      @JsonSubTypes.Type(name = "floatAny", value = FloatAnyAggregatorFactory.class),
      @JsonSubTypes.Type(name = "doubleAny", value = DoubleAnyAggregatorFactory.class),
      @JsonSubTypes.Type(name = "stringAny", value = StringAnyAggregatorFactory.class),
      @JsonSubTypes.Type(name = "grouping", value = GroupingAggregatorFactory.class),
      @JsonSubTypes.Type(name = "expression", value = ExpressionLambdaAggregatorFactory.class),
      @JsonSubTypes.Type(name = "singleValue", value = SingleValueAggregatorFactory.class)
  })
  public interface AggregatorFactoryMixin
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
  public interface PostAggregatorMixin
  {
  }
}
