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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.SimpleTopNOptimizationInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;

public class TopNMetricSpecOptimizationsTest extends InitializedNullHandlingTest
{
  private static final List<AggregatorFactory> AGGS = Lists.newArrayList(
      Iterables.concat(
          QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS,
          Lists.newArrayList(
              new DoubleMaxAggregatorFactory("maxIndex", "index"),
              new DoubleMinAggregatorFactory("minIndex", "index")
          )
      )
  );

  @Test
  public void testShouldOptimizeLexicographic()
  {
    // query interval is greater than segment interval, no filters, can ignoreAfterThreshold
    int cardinality = 1234;
    int threshold = 4;
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(threshold)
        .intervals("2018-05-30T00:00:00Z/2018-05-31T00:00:00Z")
        .aggregators(AGGS)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    DimensionSelector dimSelector = makeFakeDimSelector(cardinality);

    BaseTopNAlgorithm.AggregatorArrayProvider arrayProviderToTest = new BaseTopNAlgorithm.AggregatorArrayProvider(
        dimSelector,
        query,
        makeCursorInspector("2018-05-30T00:00:00Z", "2018-05-30T01:00:00Z", cardinality),
        cardinality
    );

    arrayProviderToTest.ignoreAfterThreshold();
    Pair<Integer, Integer> thePair = arrayProviderToTest.computeStartEnd(cardinality);
    Assert.assertEquals(new Integer(0), thePair.lhs);
    Assert.assertEquals(new Integer(threshold), thePair.rhs);
  }

  @Test
  public void testAlsoShouldOptimizeLexicographic()
  {
    // query interval is same as segment interval, no filters, can ignoreAfterThreshold
    int cardinality = 1234;
    int threshold = 4;
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(threshold)
        .intervals("2018-05-30T00:00:00Z/2018-05-30T01:00:00Z")
        .aggregators(AGGS)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    DimensionSelector dimSelector = makeFakeDimSelector(cardinality);


    BaseTopNAlgorithm.AggregatorArrayProvider arrayProviderToTest = new BaseTopNAlgorithm.AggregatorArrayProvider(
        dimSelector,
        query,
        makeCursorInspector("2018-05-30T00:00:00Z", "2018-05-30T01:00:00Z", cardinality),
        cardinality
    );

    arrayProviderToTest.ignoreAfterThreshold();
    Pair<Integer, Integer> thePair = arrayProviderToTest.computeStartEnd(cardinality);
    Assert.assertEquals(new Integer(0), thePair.lhs);
    Assert.assertEquals(new Integer(threshold), thePair.rhs);
  }

  @Test
  public void testShouldNotOptimizeLexicographic()
  {
    // query interval is smaller than segment interval, no filters, can ignoreAfterThreshold
    int cardinality = 1234;
    int threshold = 4;
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(threshold)
        .intervals("2018-05-30T00:00:00Z/2018-05-30T01:00:00Z")
        .aggregators(AGGS)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    DimensionSelector dimSelector = makeFakeDimSelector(cardinality);


    BaseTopNAlgorithm.AggregatorArrayProvider arrayProviderToTest = new BaseTopNAlgorithm.AggregatorArrayProvider(
        dimSelector,
        query,
        makeCursorInspector("2018-05-30T00:00:00Z", "2018-05-31T00:00:00Z", cardinality),
        cardinality
    );

    arrayProviderToTest.ignoreAfterThreshold();
    Pair<Integer, Integer> thePair = arrayProviderToTest.computeStartEnd(cardinality);
    Assert.assertEquals(new Integer(0), thePair.lhs);
    Assert.assertEquals(new Integer(cardinality), thePair.rhs);
  }

  @Test
  public void testAlsoShouldNotOptimizeLexicographic()
  {
    // query interval is larger than segment interval, but has filters, can ignoreAfterThreshold
    int cardinality = 1234;
    int threshold = 4;
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .filters(QueryRunnerTestHelper.QUALITY_DIMENSION, "entertainment")
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(threshold)
        .intervals("2018-05-30T00:00:00Z/2018-05-31T00:00:00Z")
        .aggregators(AGGS)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    DimensionSelector dimSelector = makeFakeDimSelector(cardinality);

    BaseTopNAlgorithm.AggregatorArrayProvider arrayProviderToTest = new BaseTopNAlgorithm.AggregatorArrayProvider(
        dimSelector,
        query,
        makeCursorInspector("2018-05-30T00:00:00Z", "2018-05-30T01:00:00Z", cardinality),
        cardinality
    );

    arrayProviderToTest.ignoreAfterThreshold();
    Pair<Integer, Integer> thePair = arrayProviderToTest.computeStartEnd(cardinality);
    Assert.assertEquals(new Integer(0), thePair.lhs);
    Assert.assertEquals(new Integer(cardinality), thePair.rhs);
  }

  @Test
  public void testAgainShouldNotOptimizeLexicographic()
  {
    // query interval is larger than segment interval, no filters, can NOT ignoreAfterThreshold
    int cardinality = 1234;
    int threshold = 4;
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(threshold)
        .intervals("2018-05-30T00:00:00Z/2018-05-31T00:00:00Z")
        .aggregators(AGGS)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    DimensionSelector dimSelector = makeFakeDimSelector(cardinality);

    BaseTopNAlgorithm.AggregatorArrayProvider arrayProviderToTest = new BaseTopNAlgorithm.AggregatorArrayProvider(
        dimSelector,
        query,
        makeCursorInspector("2018-05-30T00:00:00Z", "2018-05-30T01:00:00Z", cardinality),
        cardinality
    );

    Pair<Integer, Integer> thePair = arrayProviderToTest.computeStartEnd(cardinality);
    Assert.assertEquals(new Integer(0), thePair.lhs);
    Assert.assertEquals(new Integer(cardinality), thePair.rhs);
  }

  private TopNCursorInspector makeCursorInspector(String start, String end, int cardinality)
  {
    return new TopNCursorInspector(
        new ColumnInspector()
        {
          @Nullable
          @Override
          public ColumnCapabilities getColumnCapabilities(String column)
          {
            return null;
          }
        },
        new SimpleTopNOptimizationInspector(true),
        Intervals.of(start + "/" + end),
        cardinality
    );
  }

  private DimensionSelector makeFakeDimSelector(int cardinality)
  {

    DimensionSelector dimSelector = new DimensionSelector()
    {
      @Override
      public int getValueCardinality()
      {
        return cardinality;
      }

      // stubs below this line not important for tests
      @Override
      public IndexedInts getRow()
      {
        return null;
      }

      @Override
      public ValueMatcher makeValueMatcher(@Nullable String value)
      {
        return null;
      }

      @Override
      public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
      {
        return null;
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        return null;
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return false;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return null;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Nullable
      @Override
      public Object getObject()
      {
        return null;
      }

      @Override
      public Class classOfObject()
      {
        return null;
      }
    };

    return dimSelector;
  }
}
