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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;

public class TopNMetricSpecOptimizationsTest
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

    StorageAdapter adapter =
        makeFakeStorageAdapter("2018-05-30T00:00:00Z", "2018-05-30T01:00:00Z", cardinality);
    DimensionSelector dimSelector = makeFakeDimSelector(cardinality);

    BaseTopNAlgorithm.AggregatorArrayProvider arrayProviderToTest = new BaseTopNAlgorithm.AggregatorArrayProvider(
        dimSelector,
        query,
        cardinality,
        adapter
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

    StorageAdapter adapter =
        makeFakeStorageAdapter("2018-05-30T00:00:00Z", "2018-05-30T01:00:00Z", cardinality);
    DimensionSelector dimSelector = makeFakeDimSelector(cardinality);


    BaseTopNAlgorithm.AggregatorArrayProvider arrayProviderToTest = new BaseTopNAlgorithm.AggregatorArrayProvider(
        dimSelector,
        query,
        cardinality,
        adapter
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

    StorageAdapter adapter =
        makeFakeStorageAdapter("2018-05-30T00:00:00Z", "2018-05-31T00:00:00Z", cardinality);
    DimensionSelector dimSelector = makeFakeDimSelector(cardinality);


    BaseTopNAlgorithm.AggregatorArrayProvider arrayProviderToTest = new BaseTopNAlgorithm.AggregatorArrayProvider(
        dimSelector,
        query,
        cardinality,
        adapter
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

    StorageAdapter adapter =
        makeFakeStorageAdapter("2018-05-30T00:00:00Z", "2018-05-30T01:00:00Z", cardinality);
    DimensionSelector dimSelector = makeFakeDimSelector(cardinality);

    BaseTopNAlgorithm.AggregatorArrayProvider arrayProviderToTest = new BaseTopNAlgorithm.AggregatorArrayProvider(
        dimSelector,
        query,
        cardinality,
        adapter
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


    StorageAdapter adapter =
        makeFakeStorageAdapter("2018-05-30T00:00:00Z", "2018-05-30T01:00:00Z", cardinality);

    DimensionSelector dimSelector = makeFakeDimSelector(cardinality);

    BaseTopNAlgorithm.AggregatorArrayProvider arrayProviderToTest = new BaseTopNAlgorithm.AggregatorArrayProvider(
        dimSelector,
        query,
        cardinality,
        adapter
    );

    Pair<Integer, Integer> thePair = arrayProviderToTest.computeStartEnd(cardinality);
    Assert.assertEquals(new Integer(0), thePair.lhs);
    Assert.assertEquals(new Integer(cardinality), thePair.rhs);
  }

  private StorageAdapter makeFakeStorageAdapter(String start, String end, int cardinality)
  {
    StorageAdapter adapter = new StorageAdapter()
    {
      @Override
      public Interval getInterval()
      {
        return Intervals.of(start + "/" + end);
      }

      @Override
      public int getDimensionCardinality(String column)
      {
        return cardinality;
      }

      @Override
      public DateTime getMinTime()
      {
        return DateTimes.of(start);
      }


      @Override
      public DateTime getMaxTime()
      {
        return DateTimes.of(end);
      }

      // stubs below this line not important for tests

      @Override
      public Indexed<String> getAvailableDimensions()
      {
        return null;
      }

      @Override
      public Iterable<String> getAvailableMetrics()
      {
        return null;
      }

      @Nullable
      @Override
      public Comparable getMinValue(String column)
      {
        return null;
      }

      @Nullable
      @Override
      public Comparable getMaxValue(String column)
      {
        return null;
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return null;
      }

      @Nullable
      @Override
      public String getColumnTypeName(String column)
      {
        return null;
      }

      @Override
      public int getNumRows()
      {
        return 0;
      }

      @Override
      public DateTime getMaxIngestedEventTime()
      {
        return null;
      }

      @Override
      public Metadata getMetadata()
      {
        return null;
      }

      @Override
      public Sequence<Cursor> makeCursors(
          @Nullable Filter filter,
          Interval interval,
          VirtualColumns virtualColumns,
          Granularity gran,
          boolean descending,
          @Nullable QueryMetrics<?> queryMetrics
      )
      {
        return null;
      }
    };

    return adapter;
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
      public ValueMatcher makeValueMatcher(Predicate<String> predicate)
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
