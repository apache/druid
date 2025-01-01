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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CompressionUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MSQComplexGroupByTest extends MSQTestBase
{
  static {
    BuiltInTypesModule.registerHandlersAndSerde();
  }

  private String dataFileNameJsonString;
  private String dataFileSignatureJsonString;
  private DataSource dataFileExternalDataSource;
  private File dataFile;

  public static Collection<Object[]> data()
  {
    Object[][] data = new Object[][]{
        {DEFAULT, DEFAULT_MSQ_CONTEXT},
        {DURABLE_STORAGE, DURABLE_STORAGE_MSQ_CONTEXT},
        {FAULT_TOLERANCE, FAULT_TOLERANCE_MSQ_CONTEXT},
        {PARALLEL_MERGE, PARALLEL_MERGE_MSQ_CONTEXT}
    };
    return Arrays.asList(data);
  }

  @BeforeEach
  public void setup() throws IOException
  {
    dataFile = newTempFile("dataFile");
    final InputStream resourceStream = this.getClass().getClassLoader()
                                           .getResourceAsStream(NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE);
    final InputStream decompressing = CompressionUtils.decompress(
        resourceStream,
        "nested-all-types-test-data.json"
    );
    Files.copy(decompressing, dataFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    decompressing.close();

    dataFileNameJsonString = queryFramework().queryJsonMapper().writeValueAsString(dataFile);

    RowSignature dataFileSignature = RowSignature.builder()
                                                 .add("timestamp", ColumnType.STRING)
                                                 .add("obj", ColumnType.NESTED_DATA)
                                                 .build();
    dataFileSignatureJsonString = queryFramework().queryJsonMapper().writeValueAsString(dataFileSignature);

    dataFileExternalDataSource = new ExternalDataSource(
        new LocalInputSource(null, null, ImmutableList.of(dataFile), SystemFields.none()),
        new JsonInputFormat(null, null, null, null, null),
        dataFileSignature
    );

    objectMapper.registerModules(BuiltInTypesModule.getJacksonModulesList());
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testInsertWithoutRollupOnNestedData(String contextName, Map<String, Object> context)
  {
    testIngestQuery().setSql("INSERT INTO foo1 SELECT\n"
                             + " obj,\n"
                             + " COUNT(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"STRING\"}, {\"name\": \"obj\", \"type\": \"COMPLEX<json>\"}]'\n"
                             + "   )\n"
                             + " )\n"
                             + " GROUP BY 1\n"
                             + " PARTITIONED BY ALL")
                     .setQueryContext(context)
                     .setExpectedSegments(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(RowSignature.builder()
                                                          .add("__time", ColumnType.LONG)
                                                          .add("obj", ColumnType.NESTED_DATA)
                                                          .add("cnt", ColumnType.LONG)
                                                          .build())
                     .setExpectedResultRows(ImmutableList.of(
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 500,
                                     "b", ImmutableMap.of(
                                         "x", "e",
                                         "z", ImmutableList.of(1, 2, 3, 4)
                                     ),
                                     "v", "a"
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 100,
                                     "b", ImmutableMap.of(
                                         "x", "a",
                                         "y", 1.1,
                                         "z", ImmutableList.of(1, 2, 3, 4)
                                     ),
                                     "v", Collections.emptyList()
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 700,
                                     "b", ImmutableMap.of(
                                         "x", "g",
                                         "y", 1.1,
                                         "z", Arrays.asList(9, null, 9, 9)
                                     ),
                                     "v", Collections.emptyList()
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 200,
                                     "b", ImmutableMap.of(
                                         "x", "b",
                                         "y", 1.1,
                                         "z", ImmutableList.of(2, 4, 6)
                                     ),
                                     "v", Collections.emptyList()
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 600,
                                     "b", ImmutableMap.of(
                                         "x", "f",
                                         "y", 1.1,
                                         "z", ImmutableList.of(6, 7, 8, 9)
                                     ),
                                     "v", "b"
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 400,
                                     "b", ImmutableMap.of(
                                         "x", "d",
                                         "y", 1.1,
                                         "z", ImmutableList.of(3, 4)
                                     ),
                                     "v", Collections.emptyList()
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(ImmutableMap.of("a", 300)),
                             1L
                         }
                     ))
                     .setQueryContext(context)
                     .verifyResults();

  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testInsertWithRollupOnNestedData(String contextName, Map<String, Object> context)
  {
    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, false);
    adjustedContext.put(MultiStageQueryContext.CTX_FINALIZE_AGGREGATIONS, false);
    testIngestQuery().setSql("INSERT INTO foo1 SELECT\n"
                             + " obj,\n"
                             + " COUNT(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"STRING\"}, {\"name\": \"obj\", \"type\": \"COMPLEX<json>\"}]'\n"
                             + "   )\n"
                             + " )\n"
                             + " GROUP BY 1\n"
                             + " PARTITIONED BY ALL")
                     .setQueryContext(adjustedContext)
                     .setExpectedSegments(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(RowSignature.builder()
                                                          .add("__time", ColumnType.LONG)
                                                          .add("obj", ColumnType.NESTED_DATA)
                                                          .add("cnt", ColumnType.LONG)
                                                          .build())
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("cnt", "cnt"))
                     .setExpectedResultRows(ImmutableList.of(
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 500,
                                     "b", ImmutableMap.of(
                                         "x", "e",
                                         "z", ImmutableList.of(1, 2, 3, 4)
                                     ),
                                     "v", "a"
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 100,
                                     "b", ImmutableMap.of(
                                         "x", "a",
                                         "y", 1.1,
                                         "z", ImmutableList.of(1, 2, 3, 4)
                                     ),
                                     "v", Collections.emptyList()
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 700,
                                     "b", ImmutableMap.of(
                                         "x", "g",
                                         "y", 1.1,
                                         "z", Arrays.asList(9, null, 9, 9)
                                     ),
                                     "v", Collections.emptyList()
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 200,
                                     "b", ImmutableMap.of(
                                         "x", "b",
                                         "y", 1.1,
                                         "z", ImmutableList.of(2, 4, 6)
                                     ),
                                     "v", Collections.emptyList()
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 600,
                                     "b", ImmutableMap.of(
                                         "x", "f",
                                         "y", 1.1,
                                         "z", ImmutableList.of(6, 7, 8, 9)
                                     ),
                                     "v", "b"
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(
                                 ImmutableMap.of(
                                     "a", 400,
                                     "b", ImmutableMap.of(
                                         "x", "d",
                                         "y", 1.1,
                                         "z", ImmutableList.of(3, 4)
                                     ),
                                     "v", Collections.emptyList()
                                 )
                             ),
                             1L
                         },
                         new Object[]{
                             0L,
                             StructuredData.wrap(ImmutableMap.of("a", 300)),
                             1L
                         }
                     ))
                     .setExpectedRollUp(true)
                     .setQueryContext(adjustedContext)
                     .verifyResults();

  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSortingOnNestedData(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("obj", ColumnType.NESTED_DATA)
                                            .build();
    testSelectQuery().setSql("SELECT\n"
                             + " obj\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"STRING\"}, {\"name\": \"obj\", \"type\": \"COMPLEX<json>\"}]'\n"

                             + "   )\n"
                             + " )\n"
                             + " ORDER BY 1")
                     .setQueryContext(ImmutableMap.of())
                     .setExpectedMSQSpec(MSQSpec
                                             .builder()
                                             .query(newScanQueryBuilder()
                                                        .dataSource(dataFileExternalDataSource)
                                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                                        .columns("obj")
                                                        .columnTypes(ColumnType.NESTED_DATA)
                                                        .context(defaultScanQueryContext(context, rowSignature))
                                                        .orderBy(Collections.singletonList(OrderBy.ascending("obj")))
                                                        .build()
                                             )
                                             .columnMappings(new ColumnMappings(ImmutableList.of(
                                                 new ColumnMapping("obj", "obj")
                                             )))
                                             .tuningConfig(MSQTuningConfig.defaultConfig())
                                             .destination(TaskReportMSQDestination.INSTANCE)
                                             .build()
                     )
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedResultRows(ImmutableList.of(
                         new Object[]{"{\"a\":500,\"b\":{\"x\":\"e\",\"z\":[1,2,3,4]},\"v\":\"a\"}"},
                         new Object[]{"{\"a\":100,\"b\":{\"x\":\"a\",\"y\":1.1,\"z\":[1,2,3,4]},\"v\":[]}"},
                         new Object[]{"{\"a\":700,\"b\":{\"x\":\"g\",\"y\":1.1,\"z\":[9,null,9,9]},\"v\":[]}"},
                         new Object[]{"{\"a\":200,\"b\":{\"x\":\"b\",\"y\":1.1,\"z\":[2,4,6]},\"v\":[]}"},
                         new Object[]{"{\"a\":600,\"b\":{\"x\":\"f\",\"y\":1.1,\"z\":[6,7,8,9]},\"v\":\"b\"}"},
                         new Object[]{"{\"a\":400,\"b\":{\"x\":\"d\",\"y\":1.1,\"z\":[3,4]},\"v\":[]}"},
                         new Object[]{"{\"a\":300}"}
                     ))
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testExactCountDistinctOnNestedData(String contextName, Map<String, Object> context)
  {
    Assumptions.assumeTrue(NullHandling.sqlCompatible());
    RowSignature rowSignature = RowSignature.builder()
                                            .add("distinct_obj", ColumnType.LONG)
                                            .build();

    Map<String, Object> modifiedContext = ImmutableMap.<String, Object>builder()
                                                      .putAll(context)
                                                      .put(PlannerConfig.CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT, false)
                                                      .build();

    DimFilter innerFilter = NullHandling.replaceWithDefault()
                            ? new SelectorDimFilter("d0", null, null)
                            : new NullFilter("d0", null);

    testSelectQuery().setSql("SELECT\n"
                             + " COUNT(DISTINCT obj) AS distinct_obj\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"STRING\"}, {\"name\": \"obj\", \"type\": \"COMPLEX<json>\"}]'\n"
                             + "   )\n"
                             + " )\n"
                             + " ORDER BY 1")
                     .setQueryContext(ImmutableMap.of(PlannerConfig.CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT, false))
                     .setExpectedMSQSpec(
                         MSQSpec
                             .builder()
                             .query(
                                 GroupByQuery
                                     .builder()
                                     .setDataSource(
                                         new QueryDataSource(
                                             GroupByQuery
                                                 .builder()
                                                 .setDataSource(dataFileExternalDataSource)
                                                 .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                 .setDimensions(
                                                     new DefaultDimensionSpec("obj", "d0", ColumnType.NESTED_DATA)
                                                 )
                                                 .setGranularity(Granularities.ALL)
                                                 .setContext(modifiedContext)
                                                 .build()
                                         )
                                     )
                                     .setAggregatorSpecs(
                                         new FilteredAggregatorFactory(
                                             new CountAggregatorFactory("a0"),
                                             new NotDimFilter(innerFilter),
                                             "a0"
                                         )
                                     )
                                     .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                     .setGranularity(Granularities.ALL)
                                     .setLimitSpec(new DefaultLimitSpec(
                                         ImmutableList.of(
                                             new OrderByColumnSpec(
                                                 "a0",
                                                 OrderByColumnSpec.Direction.ASCENDING,
                                                 StringComparators.NUMERIC
                                             )
                                         ),
                                         Integer.MAX_VALUE
                                     ))
                                     .setContext(modifiedContext)
                                     .build()
                             )
                             .columnMappings(new ColumnMappings(ImmutableList.of(
                                 new ColumnMapping("a0", "distinct_obj")
                             )))
                             .tuningConfig(MSQTuningConfig.defaultConfig())
                             .destination(TaskReportMSQDestination.INSTANCE)
                             .build()
                     )
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(modifiedContext)
                     .setExpectedResultRows(ImmutableList.of(
                         new Object[]{7L}
                     ))
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testExactCountDistinctOnNestedData2(String contextName, Map<String, Object> context)
  {
    Assumptions.assumeTrue(NullHandling.sqlCompatible());
    RowSignature dataFileSignature = RowSignature.builder()
                                                 .add("timestamp", ColumnType.STRING)
                                                 .add("cObj", ColumnType.NESTED_DATA)
                                                 .build();
    DataSource dataFileExternalDataSource2 = new ExternalDataSource(
        new LocalInputSource(null, null, ImmutableList.of(dataFile), SystemFields.none()),
        new JsonInputFormat(null, null, null, null, null),
        dataFileSignature
    );
    RowSignature rowSignature = RowSignature.builder()
                                            .add("distinct_obj", ColumnType.LONG)
                                            .build();

    Map<String, Object> modifiedContext = ImmutableMap.<String, Object>builder()
                                                      .putAll(context)
                                                      .put(PlannerConfig.CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT, false)
                                                      .build();

    DimFilter innerFilter = NullHandling.replaceWithDefault()
                            ? new SelectorDimFilter("d0", null, null)
                            : new NullFilter("d0", null);

    testSelectQuery().setSql("SELECT\n"
                             + " COUNT(DISTINCT cObj) AS distinct_obj\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"STRING\"}, {\"name\": \"cObj\", \"type\": \"COMPLEX<json>\"}]'\n"
                             + "   )\n"
                             + " )\n"
                             + " ORDER BY 1")
                     .setQueryContext(ImmutableMap.of(PlannerConfig.CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT, false))
                     .setExpectedMSQSpec(
                         MSQSpec
                             .builder()
                             .query(
                                 GroupByQuery
                                     .builder()
                                     .setDataSource(
                                         new QueryDataSource(
                                             GroupByQuery
                                                 .builder()
                                                 .setDataSource(dataFileExternalDataSource2)
                                                 .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                 .setDimensions(
                                                     new DefaultDimensionSpec("cObj", "d0", ColumnType.NESTED_DATA)
                                                 )
                                                 .setGranularity(Granularities.ALL)
                                                 .setContext(modifiedContext)
                                                 .build()
                                         )
                                     )
                                     .setAggregatorSpecs(
                                         new FilteredAggregatorFactory(
                                             new CountAggregatorFactory("a0"),
                                             new NotDimFilter(innerFilter),
                                             "a0"
                                         )
                                     )
                                     .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                     .setGranularity(Granularities.ALL)
                                     .setLimitSpec(new DefaultLimitSpec(
                                         ImmutableList.of(
                                             new OrderByColumnSpec(
                                                 "a0",
                                                 OrderByColumnSpec.Direction.ASCENDING,
                                                 StringComparators.NUMERIC
                                             )
                                         ),
                                         Integer.MAX_VALUE
                                     ))
                                     .setContext(modifiedContext)
                                     .build()
                             )
                             .columnMappings(new ColumnMappings(ImmutableList.of(
                                 new ColumnMapping("a0", "distinct_obj")
                             )))
                             .tuningConfig(MSQTuningConfig.defaultConfig())
                             .destination(TaskReportMSQDestination.INSTANCE)
                             .build()
                     )
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(modifiedContext)
                     .setExpectedResultRows(ImmutableList.of(
                         new Object[]{1L}
                     ))
                     .verifyResults();
  }
}
