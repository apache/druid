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
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
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
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.segment.virtual.NestedMergeVirtualColumn;
import org.apache.druid.segment.virtual.NestedObjectVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public class NestedDataGroupByQueryTest extends InitializedNullHandlingTest
{
  private static final Logger LOG = new Logger(NestedDataGroupByQueryTest.class);

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final Closer closer;
  private final QueryContexts.Vectorize vectorize;
  private final AggregationTestHelper helper;
  private final BiFunction<TemporaryFolder, Closer, List<Segment>> segmentsGenerator;
  private final String segmentsName;

  public NestedDataGroupByQueryTest(
      GroupByQueryConfig config,
      BiFunction<TemporaryFolder, Closer, List<Segment>> segmentGenerator,
      String vectorize
  )
  {
    BuiltInTypesModule.registerHandlersAndSerde();
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        BuiltInTypesModule.getJacksonModulesList(),
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
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize.toString()
    );
  }

  @Parameterized.Parameters(name = "config = {0}, segments = {1}, vectorize = {2}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    final List<BiFunction<TemporaryFolder, Closer, List<Segment>>> segmentsGenerators =
        NestedDataTestUtils.getSegmentGenerators(NestedDataTestUtils.SIMPLE_DATA_FILE);

    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (BiFunction<TemporaryFolder, Closer, List<Segment>> generatorFn : segmentsGenerators) {
        for (String vectorize : new String[]{"false", "true", "force"}) {
          constructors.add(new Object[]{config, generatorFn, vectorize});
        }
      }
    }
    return constructors;
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


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{null, 8L},
            new Object[]{"100", 2L},
            new Object[]{"200", 2L},
            new Object[]{"300", 4L}
        )
    );
  }

  @Test
  public void testGroupByRegularColumns()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(
                                              DefaultDimensionSpec.of("v0"),
                                              DefaultDimensionSpec.of("v1"),
                                              new DefaultDimensionSpec("v2", "v2", ColumnType.LONG),
                                              new DefaultDimensionSpec("v3", "v3", ColumnType.LONG),
                                              new DefaultDimensionSpec("v4", "v4", ColumnType.STRING),
                                              new DefaultDimensionSpec("v5", "v5", ColumnType.LONG)
                                          )
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn("dim", "$", "v0", ColumnType.STRING),
                                              new NestedFieldVirtualColumn("dim", "$.x", "v1", ColumnType.STRING),
                                              new NestedFieldVirtualColumn("dim", "$", "v2", ColumnType.LONG),
                                              new NestedFieldVirtualColumn("count", "$", "v3", ColumnType.LONG),
                                              new NestedFieldVirtualColumn("count", "$", "v4", ColumnType.STRING),
                                              new NestedFieldVirtualColumn("count", "$.x", "v5", ColumnType.LONG)
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{"100", null, 100L, 1L, "1", null, 2L},
            new Object[]{"hello", null, null, 1L, "1", null, 12L},
            new Object[]{"world", null, null, 1L, "1", null, 2L}
        )
    );
  }

  @Test
  public void testGroupBySomeFieldWithFilter()
  {
    List<String> vals = new ArrayList<>();
    vals.add(null);
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


    runResults(
        groupQuery,
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
    vals.add(null);
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


    runResults(groupQuery, ImmutableList.of(new Object[]{null, 16L}));
  }

  @Test
  public void testGroupBySomeFieldWithNonExistentAgg()
  {
    List<String> vals = new ArrayList<>();
    vals.add(null);
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


    runResults(groupQuery, ImmutableList.of(new Object[]{null, null}));
  }

  @Test
  public void testGroupByNonExistentVirtualColumn()
  {
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

    runResults(
        groupQuery,
        ImmutableList.of(new Object[]{null, 16L})
    );
  }

  @Test
  public void testGroupByNonExistentFilterAsString()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn("nest", "$.fake", "v0", ColumnType.STRING)
                                          )
                                          .setDimFilter(new SelectorDimFilter("v0", "1", null))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    runResults(groupQuery, Collections.emptyList());
  }

  @Test
  public void testGroupByNonExistentFilterAsNumeric()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn("nest", "$.fake", "v0", ColumnType.LONG)
                                          )
                                          .setDimFilter(new SelectorDimFilter("v0", "1", null))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    runResults(groupQuery, Collections.emptyList());
  }

  @Test
  public void testGroupBySomeFieldOnStringColumn()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"), DefaultDimensionSpec.of("v1"))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn("dim", "$", "v0"),
                                              new NestedFieldVirtualColumn("dim", "$.x", "v1")
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{"100", null, 2L},
            new Object[]{"hello", null, 12L},
            new Object[]{"world", null, 2L}
        )
    );
  }

  @Test
  public void testGroupBySomeFieldOnStringColumnWithFilter()
  {
    List<String> vals = new ArrayList<>();
    vals.add("100");
    vals.add("200");
    vals.add("300");
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(new NestedFieldVirtualColumn("dim", "$", "v0"))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .setDimFilter(new InDimFilter("v0", vals, null))
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{"100", 2L}
        )
    );
  }

  @Test
  public void testGroupBySomeFieldOnStringColumnWithFilterExpectedTypeLong()
  {
    List<String> vals = new ArrayList<>();
    vals.add("100");
    vals.add("200");
    vals.add("300");
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.LONG))
                                          .setVirtualColumns(new NestedFieldVirtualColumn("dim", "$", "v0", ColumnType.LONG))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .setDimFilter(new InDimFilter("v0", vals, null))
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{100L, 2L}
        )
    );
  }

  @Test
  public void testGroupBySomeFieldOnNestedStringColumnWithFilterExpectedTypeLong()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.LONG))
                                          .setVirtualColumns(new NestedFieldVirtualColumn("nester", "$.y.a", "v0", ColumnType.LONG))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .setDimFilter(new SelectorDimFilter("v0", "100", null))
                                          .build();


    runResults(groupQuery, Collections.emptyList());
  }

  @Test
  public void testGroupBySomeFieldOnStringColumnWithFilterExpectedTypeDouble()
  {
    List<String> vals = new ArrayList<>();
    vals.add("100");
    vals.add("200");
    vals.add("300");
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.DOUBLE))
                                          .setVirtualColumns(new NestedFieldVirtualColumn("dim", "$", "v0", ColumnType.LONG))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .setDimFilter(new InDimFilter("v0", vals, null))
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{100.0, 2L}
        )
    );
  }

  @Test
  public void testGroupBySomeFieldOnStringColumnWithFilterExpectedTypeFloat()
  {
    List<String> vals = new ArrayList<>();
    vals.add("100");
    vals.add("200");
    vals.add("300");
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.FLOAT))
                                          .setVirtualColumns(new NestedFieldVirtualColumn("dim", "$", "v0", ColumnType.LONG))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .setDimFilter(new InDimFilter("v0", vals, null))
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{100f, 2L}
        )
    );
  }

  @Test
  public void testGroupBySomeFieldOnStringColumnWithFilterNil()
  {
    List<String> vals = new ArrayList<>();
    vals.add("100");
    vals.add("200");
    vals.add("300");
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(new NestedFieldVirtualColumn("dim", "$.x", "v0"))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .setDimFilter(new InDimFilter("v0", vals, null))
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupBySomeFieldOnLongColumn()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(
                                              DefaultDimensionSpec.of("v0", ColumnType.LONG),
                                              DefaultDimensionSpec.of("v1", ColumnType.LONG)
                                          )
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn("__time", "$", "v0"),
                                              new NestedFieldVirtualColumn("__time", "$.x", "v1")
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{1672531200000L, null, 8L},
            new Object[]{1672617600000L, null, 8L}
        )
    );
  }

  @Test
  public void testGroupBySomeFieldOnLongColumnFilter()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(
                                              DefaultDimensionSpec.of("v0", ColumnType.LONG)
                                          )
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn("__time", "$", "v0")
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setDimFilter(new SelectorDimFilter("v0", "1672531200000", null))
                                          .setContext(getContext())
                                          .build();


    runResults(groupQuery, ImmutableList.of(new Object[]{1672531200000L, 8L}));
  }

  @Test
  public void testGroupBySomeFieldOnLongColumnFilterExpectedType()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(
                                              DefaultDimensionSpec.of("v0", ColumnType.STRING)
                                          )
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn("__time", "$", "v0", ColumnType.STRING)
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setDimFilter(new SelectorDimFilter("v0", "1672531200000", null))
                                          .setContext(getContext())
                                          .build();


    runResults(groupQuery, ImmutableList.of(new Object[]{"1672531200000", 8L}));
  }

  @Test
  public void testGroupBySomeFieldOnLongColumnFilterNil()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(
                                              DefaultDimensionSpec.of("v0", ColumnType.LONG)
                                          )
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn("__time", "$.x", "v0")
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setDimFilter(new SelectorDimFilter("v0", "1609459200000", null))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupByRootAuto()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("dim"))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{"100", 2L},
            new Object[]{"hello", 12L},
            new Object[]{"world", 2L}
        )
    );
  }

  @Test
  public void testGroupByWithNestedObjectVirtualColumn()
  {
    Map<String, NestedObjectVirtualColumn.TypedExpression> keyExprMap = ImmutableMap.of(
        "nested_x",
        new NestedObjectVirtualColumn.TypedExpression("json_value(nest, '$.x', 'STRING')", ColumnType.STRING)
    );

    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v1"))
                                          .setVirtualColumns(
                                              new NestedObjectVirtualColumn(
                                                  "v0",
                                                  keyExprMap,
                                                  TestExprMacroTable.INSTANCE
                                              ),
                                              new NestedFieldVirtualColumn("v0", "$.nested_x", "v1")
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{null, 8L},
            new Object[]{"100", 2L},
            new Object[]{"200", 2L},
            new Object[]{"300", 4L}
        )
    );
  }

  @Test
  public void testGroupByWithNestedObjectVirtualColumnFilter()
  {
    final Map<String, NestedObjectVirtualColumn.TypedExpression> keyExprMap = ImmutableMap.of(
        "dimension",
        new NestedObjectVirtualColumn.TypedExpression("dim", ColumnType.STRING)
    );

    final List<String> vals = List.of("100", "hello");

    GroupByQuery groupQuery =
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .setDimensions(DefaultDimensionSpec.of("v1"))
                    .setVirtualColumns(
                        new NestedObjectVirtualColumn("v0", keyExprMap, TestExprMacroTable.INSTANCE),
                        new NestedFieldVirtualColumn("v0", "$.dimension", "v1")
                    )
                    .setAggregatorSpecs(new CountAggregatorFactory("count"))
                    .setDimFilter(new InDimFilter("v1", vals, null))
                    .setContext(getContext())
                    .build();

    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{"100", 2L},
            new Object[]{"hello", 12L}
        )
    );
  }

  @Test
  public void testGroupByWithNestedMergeVirtualColumn()
  {
    final Map<String, NestedObjectVirtualColumn.TypedExpression> obj1Map = ImmutableMap.of(
        "x",
        new NestedObjectVirtualColumn.TypedExpression("json_value(nest, '$.x', 'STRING')", ColumnType.STRING),
        "dim_value", // will be overshadowed
        new NestedObjectVirtualColumn.TypedExpression("'no'", ColumnType.STRING)
    );
    final Map<String, NestedObjectVirtualColumn.TypedExpression> obj2Map = ImmutableMap.of(
        "dim_value",
        new NestedObjectVirtualColumn.TypedExpression("'yes'", ColumnType.STRING)
    );

    GroupByQuery groupQuery =
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .setDimensions(DefaultDimensionSpec.of("v3"), DefaultDimensionSpec.of("v4"))
                    .setVirtualColumns(
                        new NestedObjectVirtualColumn("v0", obj1Map, TestExprMacroTable.INSTANCE),
                        new NestedObjectVirtualColumn("v1", obj2Map, TestExprMacroTable.INSTANCE),
                        new NestedMergeVirtualColumn("v2", ImmutableList.of("v0", "v1"), TestExprMacroTable.INSTANCE),
                        new NestedFieldVirtualColumn("v2", "$.x", "v3"),
                        new NestedFieldVirtualColumn("v2", "$.dim_value", "v4")
                    )
                    .setAggregatorSpecs(new CountAggregatorFactory("count"))
                    .setContext(getContext())
                    .build();

    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{null, "yes", 8L},
            new Object[]{"100", "yes", 2L},
            new Object[]{"200", "yes", 2L},
            new Object[]{"300", "yes", 4L}
        )
    );
  }

  @Test
  public void testGroupByWithNestedMergeVirtualColumnFilter()
  {
    final Map<String, NestedObjectVirtualColumn.TypedExpression> obj1Map = ImmutableMap.of(
        "x",
        new NestedObjectVirtualColumn.TypedExpression("json_value(nest, '$.x', 'STRING')", ColumnType.STRING),
        "dim_value", // will be overshadowed
        new NestedObjectVirtualColumn.TypedExpression("'no'", ColumnType.STRING)
    );
    final Map<String, NestedObjectVirtualColumn.TypedExpression> obj2Map = ImmutableMap.of(
        "dim_value",
        new NestedObjectVirtualColumn.TypedExpression("'yes'", ColumnType.STRING)
    );

    final List<String> vals = List.of("100", "200");

    GroupByQuery groupQuery =
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .setDimensions(DefaultDimensionSpec.of("v3"), DefaultDimensionSpec.of("v4"))
                    .setVirtualColumns(
                        new NestedObjectVirtualColumn("v0", obj1Map, TestExprMacroTable.INSTANCE),
                        new NestedObjectVirtualColumn("v1", obj2Map, TestExprMacroTable.INSTANCE),
                        new NestedMergeVirtualColumn("v2", ImmutableList.of("v0", "v1"), TestExprMacroTable.INSTANCE),
                        new NestedFieldVirtualColumn("v2", "$.x", "v3"),
                        new NestedFieldVirtualColumn("v2", "$.dim_value", "v4")
                    )
                    .setAggregatorSpecs(new CountAggregatorFactory("count"))
                    .setDimFilter(new InDimFilter("v3", vals, null))
                    .setContext(getContext())
                    .build();

    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{"100", "yes", 2L},
            new Object[]{"200", "yes", 2L}
        )
    );
  }

  private void runResults(
      GroupByQuery groupQuery,
      List<Object[]> expectedResults
  )
  {
    List<Segment> segments = segmentsGenerator.apply(tempFolder, closer);
    Supplier<List<ResultRow>> runner =
        () -> helper.runQueryOnSegmentsObjs(segments, groupQuery).toList();
    final CursorBuildSpec spec = GroupingEngine.makeCursorBuildSpec(groupQuery, null);
    boolean allCanVectorize = segments.stream()
                                      .allMatch(
                                          s -> {
                                            final CursorHolder cursorHolder = Objects.requireNonNull(s.as(CursorFactory.class))
                                                                                     .makeCursorHolder(spec);
                                            final boolean canVectorize = cursorHolder.canVectorize();
                                            cursorHolder.close();
                                            return canVectorize;
                                          });

    if (!allCanVectorize) {
      if (vectorize == QueryContexts.Vectorize.FORCE) {
        Throwable t = Assert.assertThrows(RuntimeException.class, runner::get);
        Assert.assertEquals(
            "java.util.concurrent.ExecutionException: java.lang.RuntimeException: org.apache.druid.java.util.common.ISE: Cannot vectorize!",
            t.getMessage()
        );
        return;
      }
    }

    List<ResultRow> results = runner.get();
    verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        expectedResults
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
        if (rowSignature.getColumnType(j).map(t -> t.is(ValueType.DOUBLE)).orElse(false)) {
          Assert.assertEquals((Double) expected.get(i)[j], (Double) resultRow[j], 0.01);
        } else if (rowSignature.getColumnType(j).map(t -> t.is(ValueType.FLOAT)).orElse(false)) {
          Assert.assertEquals((Float) expected.get(i)[j], (Float) resultRow[j], 0.01);
        } else {
          Assert.assertEquals(expected.get(i)[j], resultRow[j]);
        }
      }
    }
  }
}
