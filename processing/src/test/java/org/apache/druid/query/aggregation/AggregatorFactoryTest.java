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

import com.google.common.collect.ImmutableList;
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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
                  new LongFirstAggregatorFactory("longFirst", "long-col", null),
                  new LongLastAggregatorFactory("longLast", "long-col", null),
                  new LongAnyAggregatorFactory("longAny", "long-col"),
                  // double aggs
                  new DoubleSumAggregatorFactory("doubleSum", "double-col"),
                  new DoubleMinAggregatorFactory("doubleMin", "double-col"),
                  new DoubleMaxAggregatorFactory("doubleMax", "double-col"),
                  new DoubleFirstAggregatorFactory("doubleFirst", "double-col", null),
                  new DoubleLastAggregatorFactory("doubleLast", "double-col", null),
                  new DoubleAnyAggregatorFactory("doubleAny", "double-col"),
                  new DoubleMeanAggregatorFactory("doubleMean", "double-col"),
                  // float aggs
                  new FloatSumAggregatorFactory("floatSum", "float-col"),
                  new FloatMinAggregatorFactory("floatMin", "float-col"),
                  new FloatMaxAggregatorFactory("floatMax", "float-col"),
                  new FloatFirstAggregatorFactory("floatFirst", "float-col", null),
                  new FloatLastAggregatorFactory("floatLast", "float-col", null),
                  new FloatAnyAggregatorFactory("floatAny", "float-col"),
                  // string aggregators
                  new StringFirstAggregatorFactory("stringFirst", "col", null, 1024),
                  new StringLastAggregatorFactory("stringLast", "col", null, 1024),
                  new StringAnyAggregatorFactory("stringAny", "col", 1024, true),
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
                    .add("count", ColumnType.LONG)
                    .add("js", ColumnType.FLOAT)
                    .add("longSum", ColumnType.LONG)
                    .add("longMin", ColumnType.LONG)
                    .add("longMax", ColumnType.LONG)
                    .add("longFirst", null)
                    .add("longLast", null)
                    .add("longAny", ColumnType.LONG)
                    .add("doubleSum", ColumnType.DOUBLE)
                    .add("doubleMin", ColumnType.DOUBLE)
                    .add("doubleMax", ColumnType.DOUBLE)
                    .add("doubleFirst", null)
                    .add("doubleLast", null)
                    .add("doubleAny", ColumnType.DOUBLE)
                    .add("doubleMean", null)
                    .add("floatSum", ColumnType.FLOAT)
                    .add("floatMin", ColumnType.FLOAT)
                    .add("floatMax", ColumnType.FLOAT)
                    .add("floatFirst", null)
                    .add("floatLast", null)
                    .add("floatAny", ColumnType.FLOAT)
                    .add("stringFirst", null)
                    .add("stringLast", null)
                    .add("stringAny", ColumnType.STRING)
                    .add("cardinality", null)
                    .add("hyperUnique", null)
                    .add("histogram", null)
                    .add("filtered", null)
                    .add("suppressed", null)
                    // postaggs
                    .add("count-finalize", ColumnType.LONG)
                    .add("js-finalize", ColumnType.FLOAT)
                    .add("longSum-finalize", ColumnType.LONG)
                    .add("longMin-finalize", ColumnType.LONG)
                    .add("longMax-finalize", ColumnType.LONG)
                    .add("longFirst-finalize", ColumnType.LONG)
                    .add("longLast-finalize", ColumnType.LONG)
                    .add("longAny-finalize", ColumnType.LONG)
                    .add("doubleSum-finalize", ColumnType.DOUBLE)
                    .add("doubleMin-finalize", ColumnType.DOUBLE)
                    .add("doubleMax-finalize", ColumnType.DOUBLE)
                    .add("doubleFirst-finalize", ColumnType.DOUBLE)
                    .add("doubleLast-finalize", ColumnType.DOUBLE)
                    .add("doubleAny-finalize", ColumnType.DOUBLE)
                    .add("doubleMean-finalize", ColumnType.DOUBLE)
                    .add("floatSum-finalize", ColumnType.FLOAT)
                    .add("floatMin-finalize", ColumnType.FLOAT)
                    .add("floatMax-finalize", ColumnType.FLOAT)
                    .add("floatFirst-finalize", ColumnType.FLOAT)
                    .add("floatLast-finalize", ColumnType.FLOAT)
                    .add("floatAny-finalize", ColumnType.FLOAT)
                    .add("stringFirst-finalize", ColumnType.STRING)
                    .add("stringLast-finalize", ColumnType.STRING)
                    .add("stringAny-finalize", ColumnType.STRING)
                    .add("cardinality-finalize", ColumnType.DOUBLE)
                    .add("hyperUnique-finalize", ColumnType.DOUBLE)
                    .add("histogram-finalize", HistogramAggregatorFactory.TYPE_VISUAL)
                    .add("filtered-finalize", ColumnType.DOUBLE)
                    .add("suppressed-finalize", ColumnType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }

  @Test
  public void testWithName()
  {
    List<AggregatorFactory> aggregatorFactories = Arrays.asList(
        new CountAggregatorFactory("col"),
        new JavaScriptAggregatorFactory(
            "col",
            ImmutableList.of("col"),
            "function(a,b) { return a + b; }",
            "function() { return 0; }",
            "function(a,b) { return a + b }",
            new JavaScriptConfig(true)
        ),
        // long aggs
        new LongSumAggregatorFactory("col", "long-col"),
        new LongMinAggregatorFactory("col", "long-col"),
        new LongMaxAggregatorFactory("col", "long-col"),
        new LongFirstAggregatorFactory("col", "long-col", null),
        new LongLastAggregatorFactory("col", "long-col", null),
        new LongAnyAggregatorFactory("col", "long-col"),
        // double aggs
        new DoubleSumAggregatorFactory("col", "double-col"),
        new DoubleMinAggregatorFactory("col", "double-col"),
        new DoubleMaxAggregatorFactory("col", "double-col"),
        new DoubleFirstAggregatorFactory("col", "double-col", null),
        new DoubleLastAggregatorFactory("col", "double-col", null),
        new DoubleAnyAggregatorFactory("col", "double-col"),
        new DoubleMeanAggregatorFactory("col", "double-col"),
        // float aggs
        new FloatSumAggregatorFactory("col", "float-col"),
        new FloatMinAggregatorFactory("col", "float-col"),
        new FloatMaxAggregatorFactory("col", "float-col"),
        new FloatFirstAggregatorFactory("col", "float-col", null),
        new FloatLastAggregatorFactory("col", "float-col", null),
        new FloatAnyAggregatorFactory("col", "float-col"),
        // string aggregators
        new StringFirstAggregatorFactory("col", "col", null, 1024),
        new StringLastAggregatorFactory("col", "col", null, 1024),
        new StringAnyAggregatorFactory("col", "col", 1024, true),
        new StringAnyAggregatorFactory("col", "col", 1024, false),
        // sketch aggs
        new CardinalityAggregatorFactory("col", ImmutableList.of(DefaultDimensionSpec.of("some-col")), false),
        new HyperUniquesAggregatorFactory("col", "hyperunique"),
        new HistogramAggregatorFactory("col", "histogram", ImmutableList.of(0.25f, 0.5f, 0.75f)),
        // delegate aggs
        new FilteredAggregatorFactory(
            new HyperUniquesAggregatorFactory("col", "hyperunique"),
            new SelectorDimFilter("col", "hello", null),
            "col"
        ),
        new SuppressedAggregatorFactory(
            new HyperUniquesAggregatorFactory("col", "hyperunique")
        )
    );

    for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
      Assert.assertEquals(aggregatorFactory, aggregatorFactory.withName("col"));
      Assert.assertEquals("newTest", aggregatorFactory.withName("newTest").getName());
    }
  }
}
