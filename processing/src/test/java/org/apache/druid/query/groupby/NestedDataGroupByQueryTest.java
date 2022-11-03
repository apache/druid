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

package org.apache.druid.query.groupby;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.nary.TrinaryFn;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class NestedDataGroupByQueryTest extends InitializedNullHandlingTest
{
  private static final Logger LOG = new Logger(NestedDataGroupByQueryTest.class);
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final Closer closer;
  private final GroupByQueryConfig config;
  private final QueryContexts.Vectorize vectorize;
  private final AggregationTestHelper helper;
  private final TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>> segmentsGenerator;
  private final String segmentsName;

  private boolean cannotVectorize = false;

  public NestedDataGroupByQueryTest(
      GroupByQueryConfig config,
      TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>> segmentGenerator,
      String vectorize
  )
  {
    NestedDataModule.registerHandlersAndSerde();
    this.config = config;
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        NestedDataModule.getJacksonModulesList(),
        config,
        tempFolder
    );
    this.segmentsGenerator = segmentGenerator;
    this.segmentsName = segmentGenerator.toString();
    this.closer = Closer.create();
  }

  public Map<String, Object> getContext()
  {
    return ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize.toString(),
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, "true"
    );
  }

  @Parameterized.Parameters(name = "config = {0}, segments = {1}, vectorize = {2}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    final List<TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>>> segmentsGenerators =
        NestedDataTestUtils.getSegmentGenerators();

    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>> generatorFn : segmentsGenerators) {
        for (String vectorize : new String[]{"false", "true", "force"}) {
          constructors.add(new Object[]{config, generatorFn, vectorize});
        }
      }
    }
    return constructors;
  }

  @Before
  public void setup()
  {
    if (!"segments".equals(segmentsName)) {
      if (GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy())) {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(
            "GroupBy v1 does not support dimension selectors with unknown cardinality."
        );
      } else if (vectorize == QueryContexts.Vectorize.FORCE) {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(
            "Cannot vectorize!"
        );
      }
    }
  }

  @After
  public void teardown() throws IOException
  {
    closer.close();
  }

  @Test
  public void testGroupBySomeField()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(new NestedFieldVirtualColumn("nest", "$.x", "v0"))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(helper, tempFolder, closer), groupQuery);

    List<ResultRow> results = seq.toList();
    verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        ImmutableList.of(
            new Object[]{null, 8L},
            new Object[]{"100", 2L},
            new Object[]{"200", 2L},
            new Object[]{"300", 4L}
        )
    );
  }

  @Test
  public void testGroupBySomeFieldWithFilter()
  {
    List<String> vals = new ArrayList<>();
    vals.add(NullHandling.defaultStringValue());
    vals.add("100");
    vals.add("200");
    vals.add("300");
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(new NestedFieldVirtualColumn("nest", "$.x", "v0"))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .setDimFilter(new InDimFilter("v0", vals, null))
                                          .build();


    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(helper, tempFolder, closer), groupQuery);

    List<ResultRow> results = seq.toList();
    verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        ImmutableList.of(
            new Object[]{null, 8L},
            new Object[]{"100", 2L},
            new Object[]{"200", 2L},
            new Object[]{"300", 4L}
        )
    );
  }

  @Test
  public void testGroupByNoFieldWithFilter()
  {
    List<String> vals = new ArrayList<>();
    vals.add(NullHandling.defaultStringValue());
    vals.add("100");
    vals.add("200");
    vals.add("300");
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(new NestedFieldVirtualColumn("nest", "$.fake", "v0", ColumnType.STRING))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .setDimFilter(new InDimFilter("v0", vals, null))
                                          .build();


    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(helper, tempFolder, closer), groupQuery);

    List<ResultRow> results = seq.toList();
    verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        ImmutableList.of(new Object[]{null, 16L})
    );
  }

  @Test
  public void testGroupBySomeFieldWithNonExistentAgg()
  {
    List<String> vals = new ArrayList<>();
    vals.add(NullHandling.defaultStringValue());
    vals.add("100");
    vals.add("200");
    vals.add("300");
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn("nest", "$.nope", "v0", ColumnType.STRING),
                                              new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.STRING),
                                              new NestedFieldVirtualColumn("nest", "$.fake", "v2", ColumnType.DOUBLE)
                                          )
                                          .setAggregatorSpecs(new LongSumAggregatorFactory("a0", "v2"))
                                          .setDimFilter(new InDimFilter("v1", vals, null))
                                          .setContext(getContext())
                                          .build();


    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(helper, tempFolder, closer), groupQuery);

    List<ResultRow> results = seq.toList();

    verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        ImmutableList.of(new Object[]{null, NullHandling.defaultLongValue()})
    );
  }

  @Test
  public void testGroupByNonExistentVirtualColumn()
  {
    if (GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy())) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "GroupBy v1 does not support dimension selectors with unknown cardinality."
      );
    }
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v1"))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn("fake", "$.fake", "v0", ColumnType.STRING),
                                              new ExpressionVirtualColumn(
                                                  "v1",
                                                  "concat(v0, 'foo')",
                                                  ColumnType.STRING,
                                                  TestExprMacroTable.INSTANCE
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(helper, tempFolder, closer), groupQuery);

    List<ResultRow> results = seq.toList();
    verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        NullHandling.sqlCompatible()
        ? ImmutableList.of(new Object[]{null, 16L})
        : ImmutableList.of(new Object[]{"foo", 16L})
    );
  }

  private static void verifyResults(RowSignature rowSignature, List<ResultRow> results, List<Object[]> expected)
  {
    LOG.info("results:\n%s", results);
    Assert.assertEquals(expected.size(), results.size());
    for (int i = 0; i < expected.size(); i++) {
      final Object[] resultRow = results.get(i).getArray();
      Assert.assertEquals(expected.get(i).length, resultRow.length);
      for (int j = 0; j < resultRow.length; j++) {
        if (rowSignature.getColumnType(j).map(t -> t.anyOf(ValueType.DOUBLE, ValueType.FLOAT)).orElse(false)) {
          Assert.assertEquals((Double) expected.get(i)[j], (Double) resultRow[j], 0.01);
        } else {
          Assert.assertEquals(expected.get(i)[j], resultRow[j]);
        }
      }
    }
  }
}
