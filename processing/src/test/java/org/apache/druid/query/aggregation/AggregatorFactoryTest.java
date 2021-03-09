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

package org.apache.druid.query.aggregation;

import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.any.DoubleAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.FloatAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.LongAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.StringAnyAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.FloatLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.aggregation.mean.DoubleMeanAggregatorFactory;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class AggregatorFactoryTest extends InitializedNullHandlingTest
{

  @Test
  public void testMergeAggregators()
  {
    Assert.assertNull(AggregatorFactory.mergeAggregators(null));
    Assert.assertNull(AggregatorFactory.mergeAggregators(ImmutableList.of()));

    List<AggregatorFactory[]> aggregatorsToBeMerged = new ArrayList<>();

    aggregatorsToBeMerged.add(null);
    Assert.assertNull(AggregatorFactory.mergeAggregators(aggregatorsToBeMerged));

    AggregatorFactory[] emptyAggFactory = new AggregatorFactory[0];

    aggregatorsToBeMerged.clear();
    aggregatorsToBeMerged.add(emptyAggFactory);
    Assert.assertArrayEquals(emptyAggFactory, AggregatorFactory.mergeAggregators(aggregatorsToBeMerged));

    aggregatorsToBeMerged.clear();
    aggregatorsToBeMerged.add(emptyAggFactory);
    aggregatorsToBeMerged.add(null);
    Assert.assertNull(AggregatorFactory.mergeAggregators(aggregatorsToBeMerged));

    aggregatorsToBeMerged.clear();
    AggregatorFactory[] af1 = new AggregatorFactory[]{
        new LongMaxAggregatorFactory("name", "fieldName1")
    };
    AggregatorFactory[] af2 = new AggregatorFactory[]{
        new LongMaxAggregatorFactory("name", "fieldName2")
    };
    Assert.assertArrayEquals(
        new AggregatorFactory[]{
            new LongMaxAggregatorFactory("name", "name")
        },
        AggregatorFactory.mergeAggregators(ImmutableList.of(af1, af2))
    );

    aggregatorsToBeMerged.clear();
    af1 = new AggregatorFactory[]{
        new LongMaxAggregatorFactory("name", "fieldName1")
    };
    af2 = new AggregatorFactory[]{
        new DoubleMaxAggregatorFactory("name", "fieldName2")
    };
    Assert.assertNull(AggregatorFactory.mergeAggregators(ImmutableList.of(af1, af2))
    );
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new CountAggregatorFactory("count"),
                  new JavaScriptAggregatorFactory(
                      "js",
                      ImmutableList.of("col"),
                      "function(a,b) { return a + b; }",
                      "function() { return 0; }",
                      "function(a,b) { return a + b }",
                      new JavaScriptConfig(true)
                  ),
                  // long aggs
                  new LongSumAggregatorFactory("longSum", "long-col"),
                  new LongMinAggregatorFactory("longMin", "long-col"),
                  new LongMaxAggregatorFactory("longMax", "long-col"),
                  new LongFirstAggregatorFactory("longFirst", "long-col"),
                  new LongLastAggregatorFactory("longLast", "long-col"),
                  new LongAnyAggregatorFactory("longAny", "long-col"),
                  // double aggs
                  new DoubleSumAggregatorFactory("doubleSum", "double-col"),
                  new DoubleMinAggregatorFactory("doubleMin", "double-col"),
                  new DoubleMaxAggregatorFactory("doubleMax", "double-col"),
                  new DoubleFirstAggregatorFactory("doubleFirst", "double-col"),
                  new DoubleLastAggregatorFactory("doubleLast", "double-col"),
                  new DoubleAnyAggregatorFactory("doubleAny", "double-col"),
                  new DoubleMeanAggregatorFactory("doubleMean", "double-col"),
                  // float aggs
                  new FloatSumAggregatorFactory("floatSum", "float-col"),
                  new FloatMinAggregatorFactory("floatMin", "float-col"),
                  new FloatMaxAggregatorFactory("floatMax", "float-col"),
                  new FloatFirstAggregatorFactory("floatFirst", "float-col"),
                  new FloatLastAggregatorFactory("floatLast", "float-col"),
                  new FloatAnyAggregatorFactory("floatAny", "float-col"),
                  // string aggregators
                  new StringFirstAggregatorFactory("stringFirst", "col", 1024),
                  new StringLastAggregatorFactory("stringLast", "col", 1024),
                  new StringAnyAggregatorFactory("stringAny", "col", 1024),
                  // sketch aggs
                  new CardinalityAggregatorFactory("cardinality", ImmutableList.of(DefaultDimensionSpec.of("some-col")), false),
                  new HyperUniquesAggregatorFactory("hyperUnique", "hyperunique"),
                  new HistogramAggregatorFactory("histogram", "histogram", ImmutableList.of(0.25f, 0.5f, 0.75f)),
                  // delegate aggs
                  new FilteredAggregatorFactory(
                      new HyperUniquesAggregatorFactory("filtered", "hyperunique"),
                      new SelectorDimFilter("col", "hello", null)
                  ),
                  new SuppressedAggregatorFactory(
                      new HyperUniquesAggregatorFactory("suppressed", "hyperunique")
                  )
              )
              .postAggregators(
                  new FinalizingFieldAccessPostAggregator("count-finalize", "count"),
                  new FinalizingFieldAccessPostAggregator("js-finalize", "js"),
                  // long aggs
                  new FinalizingFieldAccessPostAggregator("longSum-finalize", "longSum"),
                  new FinalizingFieldAccessPostAggregator("longMin-finalize", "longMin"),
                  new FinalizingFieldAccessPostAggregator("longMax-finalize", "longMax"),
                  new FinalizingFieldAccessPostAggregator("longFirst-finalize", "longFirst"),
                  new FinalizingFieldAccessPostAggregator("longLast-finalize", "longLast"),
                  new FinalizingFieldAccessPostAggregator("longAny-finalize", "longAny"),
                  // double
                  new FinalizingFieldAccessPostAggregator("doubleSum-finalize", "doubleSum"),
                  new FinalizingFieldAccessPostAggregator("doubleMin-finalize", "doubleMin"),
                  new FinalizingFieldAccessPostAggregator("doubleMax-finalize", "doubleMax"),
                  new FinalizingFieldAccessPostAggregator("doubleFirst-finalize", "doubleFirst"),
                  new FinalizingFieldAccessPostAggregator("doubleLast-finalize", "doubleLast"),
                  new FinalizingFieldAccessPostAggregator("doubleAny-finalize", "doubleAny"),
                  new FinalizingFieldAccessPostAggregator("doubleMean-finalize", "doubleMean"),
                  // finalized floats
                  new FinalizingFieldAccessPostAggregator("floatSum-finalize", "floatSum"),
                  new FinalizingFieldAccessPostAggregator("floatMin-finalize", "floatMin"),
                  new FinalizingFieldAccessPostAggregator("floatMax-finalize", "floatMax"),
                  new FinalizingFieldAccessPostAggregator("floatFirst-finalize", "floatFirst"),
                  new FinalizingFieldAccessPostAggregator("floatLast-finalize", "floatLast"),
                  new FinalizingFieldAccessPostAggregator("floatAny-finalize", "floatAny"),
                  // finalized strings
                  new FinalizingFieldAccessPostAggregator("stringFirst-finalize", "stringFirst"),
                  new FinalizingFieldAccessPostAggregator("stringLast-finalize", "stringLast"),
                  new FinalizingFieldAccessPostAggregator("stringAny-finalize", "stringAny"),
                  // finalized sketch
                  new FinalizingFieldAccessPostAggregator("cardinality-finalize", "cardinality"),
                  new FinalizingFieldAccessPostAggregator("hyperUnique-finalize", "hyperUnique"),
                  new FinalizingFieldAccessPostAggregator("histogram-finalize", "histogram"),
                  // finalized delegate
                  new FinalizingFieldAccessPostAggregator("filtered-finalize", "filtered"),
                  new FinalizingFieldAccessPostAggregator("suppressed-finalize", "suppressed")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    // aggs
                    .add("count", ValueType.LONG)
                    .add("js", ValueType.FLOAT)
                    .add("longSum", ValueType.LONG)
                    .add("longMin", ValueType.LONG)
                    .add("longMax", ValueType.LONG)
                    .add("longFirst", ValueType.LONG)
                    .add("longLast", ValueType.LONG)
                    .add("longAny", ValueType.LONG)
                    .add("doubleSum", ValueType.DOUBLE)
                    .add("doubleMin", ValueType.DOUBLE)
                    .add("doubleMax", ValueType.DOUBLE)
                    .add("doubleFirst", ValueType.DOUBLE)
                    .add("doubleLast", ValueType.DOUBLE)
                    .add("doubleAny", ValueType.DOUBLE)
                    .add("doubleMean", null)
                    .add("floatSum", ValueType.FLOAT)
                    .add("floatMin", ValueType.FLOAT)
                    .add("floatMax", ValueType.FLOAT)
                    .add("floatFirst", ValueType.FLOAT)
                    .add("floatLast", ValueType.FLOAT)
                    .add("floatAny", ValueType.FLOAT)
                    .add("stringFirst", null)
                    .add("stringLast", null)
                    .add("stringAny", ValueType.STRING)
                    .add("cardinality", null)
                    .add("hyperUnique", null)
                    .add("histogram", ValueType.COMPLEX)
                    .add("filtered", null)
                    .add("suppressed", null)
                    // postaggs
                    .add("count-finalize", ValueType.LONG)
                    .add("js-finalize", ValueType.FLOAT)
                    .add("longSum-finalize", ValueType.LONG)
                    .add("longMin-finalize", ValueType.LONG)
                    .add("longMax-finalize", ValueType.LONG)
                    .add("longFirst-finalize", ValueType.LONG)
                    .add("longLast-finalize", ValueType.LONG)
                    .add("longAny-finalize", ValueType.LONG)
                    .add("doubleSum-finalize", ValueType.DOUBLE)
                    .add("doubleMin-finalize", ValueType.DOUBLE)
                    .add("doubleMax-finalize", ValueType.DOUBLE)
                    .add("doubleFirst-finalize", ValueType.DOUBLE)
                    .add("doubleLast-finalize", ValueType.DOUBLE)
                    .add("doubleAny-finalize", ValueType.DOUBLE)
                    .add("doubleMean-finalize", ValueType.DOUBLE)
                    .add("floatSum-finalize", ValueType.FLOAT)
                    .add("floatMin-finalize", ValueType.FLOAT)
                    .add("floatMax-finalize", ValueType.FLOAT)
                    .add("floatFirst-finalize", ValueType.FLOAT)
                    .add("floatLast-finalize", ValueType.FLOAT)
                    .add("floatAny-finalize", ValueType.FLOAT)
                    .add("stringFirst-finalize", ValueType.STRING)
                    .add("stringLast-finalize", ValueType.STRING)
                    .add("stringAny-finalize", ValueType.STRING)
                    .add("cardinality-finalize", ValueType.DOUBLE)
                    .add("hyperUnique-finalize", ValueType.DOUBLE)
                    .add("histogram-finalize", ValueType.COMPLEX)
                    .add("filtered-finalize", ValueType.DOUBLE)
                    .add("suppressed-finalize", ValueType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
