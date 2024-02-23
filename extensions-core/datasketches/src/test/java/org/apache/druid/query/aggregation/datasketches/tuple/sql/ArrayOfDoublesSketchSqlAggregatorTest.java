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

package org.apache.druid.query.aggregation.datasketches.tuple.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchOperations;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchSetOpPostAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchToMetricsSumEstimatePostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class ArrayOfDoublesSketchSqlAggregatorTest extends BaseCalciteQueryTest
{

  private static final String DATA_SOURCE = "foo";

  // built from ArrayOfDoublesUpdatableSketch.update("FEDCAB", new double[] {0.0}).compact()
  private static final String COMPACT_BASE_64_ENCODED_SKETCH_FOR_INTERSECTION = "AQEJAwgBzJP/////////fwEAAAAAAAAAjFnadZuMrkgAAAAAAAAAAA==";

  private static final List<InputRow> ROWS = ImmutableList.of(
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("dim1", "CA")
                  .put("dim2", "FEDCAB")
                  .put("m1", 5)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("dim1", "US")
                  .put("dim2", "ABCDEF")
                  .put("m1", 12)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-02")
                  .put("dim1", "CA")
                  .put("dim2", "FEDCAB")
                  .put("m1", 3)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-02")
                  .put("dim1", "US")
                  .put("dim2", "ABCDEF")
                  .put("m1", 8)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-03")
                  .put("dim1", "US")
                  .put("dim2", "ABCDEF")
                  .put("m1", 2)
                  .build()
  ).stream().map(TestDataBuilder::createRow).collect(Collectors.toList());

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(new ArrayOfDoublesSketchModule());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final JoinableFactoryWrapper joinableFactory,
      final Injector injector
  ) throws IOException
  {
    ArrayOfDoublesSketchModule.registerSerde();

    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(
                                                 OffHeapMemorySegmentWriteOutMediumFactory.instance()
                                             )
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withMetrics(
                                                         new CountAggregatorFactory("cnt"),
                                                         new ArrayOfDoublesSketchAggregatorFactory(
                                                             "tuplesketch_dim2",
                                                             "dim2",
                                                             null,
                                                             ImmutableList.of("m1"),
                                                             1
                                                         ),
                                                         new LongSumAggregatorFactory("m1", "m1")
                                                     )
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(ROWS)
                                             .buildMMappedIndex();

    return SpecificSegmentsQuerySegmentWalker.createWalker(injector, conglomerate).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index
    );
  }

  @Test
  public void testMetricsSumEstimate()
  {
    cannotVectorize();

    final String sql = "SELECT\n"
                       + "  dim1,\n"
                       + "  SUM(cnt),\n"
                       + "  DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(DS_TUPLE_DOUBLES(tuplesketch_dim2)),\n"
                       + "  DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(DS_TUPLE_DOUBLES(dim2, m1)),\n"
                       + "  DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(DS_TUPLE_DOUBLES(dim2, m1, 256))\n"
                       + "FROM druid.foo\n"
                       + "GROUP BY dim1";

    final List<Object[]> expectedResults;

    expectedResults = ImmutableList.of(
        new Object[]{
            "CA",
            2L,
            "[8.0]",
            "[8.0]",
            "[8.0]"
        },
        new Object[]{
            "US",
            3L,
            "[22.0]",
            "[22.0]",
            "[22.0]"
        }
    );

    testQuery(
        sql,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            aggregators(
                                new LongSumAggregatorFactory("a0", "cnt"),
                                new ArrayOfDoublesSketchAggregatorFactory(
                                    "a1",
                                    "tuplesketch_dim2",
                                    null,
                                    null,
                                    null
                                ),
                                new ArrayOfDoublesSketchAggregatorFactory(
                                    "a2",
                                    "dim2",
                                    null,
                                    ImmutableList.of("m1"),
                                    null
                                ),
                                new ArrayOfDoublesSketchAggregatorFactory(
                                    "a3",
                                    "dim2",
                                    256,
                                    ImmutableList.of("m1"),
                                    null
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new ArrayOfDoublesSketchToMetricsSumEstimatePostAggregator(
                                    "p1",
                                    new FieldAccessPostAggregator("p0", "a1")
                                ),
                                new ArrayOfDoublesSketchToMetricsSumEstimatePostAggregator(
                                    "p3",
                                    new FieldAccessPostAggregator("p2", "a2")
                                ),
                                new ArrayOfDoublesSketchToMetricsSumEstimatePostAggregator(
                                    "p5",
                                    new FieldAccessPostAggregator("p4", "a3")
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testMetricsSumEstimateIntersect()
  {
    cannotVectorize();

    final String sql = "SELECT\n"
                       + "  SUM(cnt),\n"
                       + "  DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(DS_TUPLE_DOUBLES(tuplesketch_dim2)) AS all_sum_estimates,\n"
                       + StringUtils.replace(
        "DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(DS_TUPLE_DOUBLES_INTERSECT(DECODE_BASE64_COMPLEX('arrayOfDoublesSketch', '%s'), DS_TUPLE_DOUBLES(tuplesketch_dim2), 128)) AS intersect_sum_estimates\n",
        "%s",
        COMPACT_BASE_64_ENCODED_SKETCH_FOR_INTERSECTION
    )
                       + "FROM druid.foo";

    final List<Object[]> expectedResults;

    expectedResults = ImmutableList.of(
        new Object[]{
            5L,
            "[30.0]",
            "[8.0]"
        }
    );

    final String expectedBase64Constant = "'"
                                          + StringUtils.replace(
        COMPACT_BASE_64_ENCODED_SKETCH_FOR_INTERSECTION,
        "=",
        "\\u003D"
    )
                                          + "'";

    testQuery(
        sql,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new ArrayOfDoublesSketchAggregatorFactory(
                              "a1",
                              "tuplesketch_dim2",
                              null,
                              null,
                              null
                          )
                      )
                  )
                  .postAggregators(
                      new ArrayOfDoublesSketchToMetricsSumEstimatePostAggregator(
                          "p1",
                          new FieldAccessPostAggregator("p0", "a1")
                      ),
                      new ArrayOfDoublesSketchToMetricsSumEstimatePostAggregator(
                          "p5",
                          new ArrayOfDoublesSketchSetOpPostAggregator(
                              "p4",
                              "INTERSECT",
                              128,
                              null,
                              ImmutableList.of(
                                  expressionPostAgg(
                                      "p2",
                                      "complex_decode_base64('arrayOfDoublesSketch',"
                                      + expectedBase64Constant
                                      + ")",
                                      ColumnType.ofComplex("arrayOfDoublesSketch")
                                  ),
                                  new FieldAccessPostAggregator("p3", "a1")
                              )
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testNullInputs()
  {
    cannotVectorize();

    final String sql = "SELECT\n"
                       + "  DS_TUPLE_DOUBLES(NULL),\n"
                       + "  DS_TUPLE_DOUBLES_METRICS_SUM_ESTIMATE(NULL),\n"
                       + "  DS_TUPLE_DOUBLES_UNION(NULL, NULL),\n"
                       + "  DS_TUPLE_DOUBLES_UNION(NULL, DS_TUPLE_DOUBLES(tuplesketch_dim2)),\n"
                       + "  DS_TUPLE_DOUBLES_UNION(DS_TUPLE_DOUBLES(tuplesketch_dim2), NULL)\n"
                       + "FROM druid.foo";

    final List<Object[]> expectedResults;

    expectedResults = ImmutableList.of(
        new Object[]{
            "0.0",
            null,
            "\"AQEJAwQBzJP/////////fw==\"",
            "\"AQEJAwgBzJP/////////fwIAAAAAAAAAjFnadZuMrkg6WYAWZ8t1NgAAAAAAACBAAAAAAAAANkA=\"",
            "\"AQEJAwgBzJP/////////fwIAAAAAAAAAjFnadZuMrkg6WYAWZ8t1NgAAAAAAACBAAAAAAAAANkA=\"",
            }
    );

    testQuery(
        sql,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "null",
                          ColumnType.STRING,
                          queryFramework().macroTable()
                      )
                  )
                  .aggregators(
                      ImmutableList.of(
                          new ArrayOfDoublesSketchAggregatorFactory(
                              "a0",
                              "v0",
                              null,
                              null,
                              null
                          ),
                          new ArrayOfDoublesSketchAggregatorFactory(
                              "a1",
                              "tuplesketch_dim2",
                              null,
                              null,
                              null
                          )
                      )
                  )
                  .postAggregators(
                      ImmutableList.of(
                          new ArrayOfDoublesSketchToMetricsSumEstimatePostAggregator(
                              "p1",
                              expressionPostAgg("p0", "null", null)
                          ),
                          new ArrayOfDoublesSketchSetOpPostAggregator(
                              "p4",
                              ArrayOfDoublesSketchOperations.Operation.UNION.name(),
                              null,
                              null,
                              ImmutableList.of(
                                  expressionPostAgg("p2", "null", null),
                                  expressionPostAgg("p3", "null", null)
                              )
                          ),
                          new ArrayOfDoublesSketchSetOpPostAggregator(
                              "p7",
                              ArrayOfDoublesSketchOperations.Operation.UNION.name(),
                              null,
                              null,
                              ImmutableList.of(
                                  expressionPostAgg("p5", "null", null),
                                  new FieldAccessPostAggregator("p6", "a1")
                              )
                          ),
                          new ArrayOfDoublesSketchSetOpPostAggregator(
                              "p10",
                              ArrayOfDoublesSketchOperations.Operation.UNION.name(),
                              null,
                              null,
                              ImmutableList.of(
                                  new FieldAccessPostAggregator("p8", "a1"),
                                  expressionPostAgg("p9", "null", null)
                              )
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testArrayOfDoublesSketchIntersectOnScalarExpression()
  {
    assertQueryIsUnplannable(
        "SELECT DS_TUPLE_DOUBLES_INTERSECT(NULL, NULL) FROM foo",
        "DS_TUPLE_DOUBLES_INTERSECT can only be used on aggregates. " +
        "It cannot be used directly on a column or on a scalar expression."
    );
  }

  @Test
  public void testArrayOfDoublesSketchNotOnScalarExpression()
  {
    assertQueryIsUnplannable(
        "SELECT DS_TUPLE_DOUBLES_NOT(NULL, NULL) FROM foo",
        "DS_TUPLE_DOUBLES_NOT can only be used on aggregates. " +
        "It cannot be used directly on a column or on a scalar expression."
    );
  }

  @Test
  public void testArrayOfDoublesSketchUnionOnScalarExpression()
  {
    assertQueryIsUnplannable(
        "SELECT DS_TUPLE_DOUBLES_UNION(NULL, NULL) FROM foo",
        "DS_TUPLE_DOUBLES_UNION can only be used on aggregates. " +
        "It cannot be used directly on a column or on a scalar expression."
    );
  }
}
