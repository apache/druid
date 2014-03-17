/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.jackson;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.hash.Hashing;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.HistogramAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.aggregation.post.JavaScriptPostAggregator;
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
      @JsonSubTypes.Type(name = "max", value = MaxAggregatorFactory.class),
      @JsonSubTypes.Type(name = "min", value = MinAggregatorFactory.class),
      @JsonSubTypes.Type(name = "javascript", value = JavaScriptAggregatorFactory.class),
      @JsonSubTypes.Type(name = "histogram", value = HistogramAggregatorFactory.class),
      @JsonSubTypes.Type(name = "hyperUnique", value = HyperUniquesAggregatorFactory.class)
  })
  public static interface AggregatorFactoryMixin
  {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(value = {
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
