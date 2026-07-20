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

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.google.common.collect.ImmutableList;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class ArrayOfDoublesSketchAggregationTest extends InitializedNullHandlingTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();
  private final AggregationTestHelper helper;
  private final AggregationTestHelper tsHelper;

  public ArrayOfDoublesSketchAggregationTest(final GroupByQueryConfig config)
  {
    ArrayOfDoublesSketchModule.registerSerde();
    DruidModule module = new ArrayOfDoublesSketchModule();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(), config, tempFolder);
    tsHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(module.getJacksonModules(), tempFolder);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @After
  public void teardown() throws IOException
  {
    helper.close();
  }

  @Test
  public void ingestingSketches() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/array_of_doubles_sketch_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(List.of("timestamp", "product", "sketch")),
        List.of(
            new ArrayOfDoublesSketchAggregatorFactory("sketch", "sketch", 1024, null, null),
            new ArrayOfDoublesSketchAggregatorFactory("non_existing_sketch", "non_existing_sketch", null, null, null)
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
                    .setAggregatorSpecs(
                        new ArrayOfDoublesSketchAggregatorFactory("sketch", "sketch", 1024, null, null),
                        new ArrayOfDoublesSketchAggregatorFactory("non_existing_sketch", "non_existing_sketch", null, null, null)
                    )
                    .setPostAggregatorSpecs(
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "estimate",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator(
                            "estimateAndBounds",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            2
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            null,
                            null
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "union",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "union", "UNION", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "intersection",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "intersection", "INTERSECT", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "anotb",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "anotb", "NOT", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToStringPostAggregator(
                            "summary",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToVariancesPostAggregator(
                            "variances",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("sketch", 40.0, (double) row.get(0), 0);
    Assert.assertEquals("non_existing_sketch", 0, (double) row.get(1), 0);
    Assert.assertEquals("estimate", 40.0, (double) row.get(2), 0);
    Assert.assertArrayEquals("estimateAndBounds", new double[]{40.0, 40.0, 40.0}, (double[]) row.get(3), 0);
    Assert.assertEquals("union", 40.0, (double) row.get(5), 0);
    Assert.assertEquals("intersection", 40.0, (double) row.get(6), 0);
    Assert.assertEquals("anotb", 0, (double) row.get(7), 0);
    Assert.assertArrayEquals("variances", new double[]{0.0}, (double[]) row.get(9), 0);

    Object obj = row.get(4); // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(40, ds.getN());
    Assert.assertEquals(1.0, ds.getMinItem(), 0);
    Assert.assertEquals(1.0, ds.getMaxItem(), 0);

    final String expectedSummary = "### HeapArrayOfDoublesCompactSketch SUMMARY: \n"
                                   + "   Estimate                : 40.0\n"
                                   + "   Upper Bound, 95% conf   : 40.0\n"
                                   + "   Lower Bound, 95% conf   : 40.0\n"
                                   + "   Theta (double)          : 1.0\n"
                                   + "   Theta (long)            : 9223372036854775807\n"
                                   + "   EstMode?                : false\n"
                                   + "   Empty?                  : false\n"
                                   + "   Retained Entries        : 40\n"
                                   + "   Seed Hash               : 93cc | 37836\n"
                                   + "### END SKETCH SUMMARY\n";
    Assert.assertEquals("summary", expectedSummary, row.get(8));
  }

  @Test
  public void ingestingSketchesTwoValues() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/array_of_doubles_sketch_data_two_values.tsv")
                     .getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "sketch")
        ),
        List.of(
            new ArrayOfDoublesSketchAggregatorFactory("sketch", "sketch", 1024, null, 2)
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
                    .setAggregatorSpecs(
                        new ArrayOfDoublesSketchAggregatorFactory("sketch", "sketch", 1024, null, 2)
                    )
                    .setPostAggregatorSpecs(
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "estimate",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            null,
                            null
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "union",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "union", "UNION", 1024, 2,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "intersection",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "intersection", "INTERSECT", 1024, 2,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "anotb",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "anotb", "NOT", 1024, 2,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToMeansPostAggregator(
                            "means",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("sketch", 40.0, (double) row.get(0), 0);
    Assert.assertEquals("estimate", 40.0, (double) row.get(1), 0);
    Assert.assertEquals("union", 40.0, (double) row.get(3), 0);
    Assert.assertEquals("intersection", 40.0, (double) row.get(4), 0);
    Assert.assertEquals("anotb", 0, (double) row.get(5), 0);

    Object meansObj = row.get(6); // means
    Assert.assertTrue(meansObj instanceof double[]);
    double[] means = (double[]) meansObj;
    Assert.assertEquals(2, means.length);
    Assert.assertEquals(1.0, means[0], 0);
    Assert.assertEquals(2.0, means[1], 0);

    Object quantilesObj = row.get(2); // quantiles-sketch
    Assert.assertTrue(quantilesObj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) quantilesObj;
    Assert.assertEquals(40, ds.getN());
    Assert.assertEquals(1.0, ds.getMinItem(), 0);
    Assert.assertEquals(1.0, ds.getMaxItem(), 0);
  }

  @Test
  public void buildingSketchesAtIngestionTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/array_of_doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "key", "key_num", "value")
        ),
        List.of(
            new ArrayOfDoublesSketchAggregatorFactory("sketch", "key", 1024, List.of("value"), null)
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
                    .setAggregatorSpecs(
                        new ArrayOfDoublesSketchAggregatorFactory("sketch", "sketch", null, null, null)
                    )
                    .setPostAggregatorSpecs(
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "estimate",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            null,
                            null
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "union",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "union", "UNION", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "intersection",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "intersection", "INTERSECT", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "anotb",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "anotb", "NOT", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("sketch", 40.0, (double) row.get(0), 0);
    Assert.assertEquals("estimate", 40.0, (double) row.get(1), 0);
    Assert.assertEquals("union", 40.0, (double) row.get(3), 0);
    Assert.assertEquals("intersection", 40.0, (double) row.get(4), 0);
    Assert.assertEquals("anotb", 0, (double) row.get(5), 0);

    Object obj = row.get(2);  // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(40, ds.getN());
    Assert.assertEquals(1.0, ds.getMinItem(), 0);
    Assert.assertEquals(1.0, ds.getMaxItem(), 0);
  }

  @Test
  public void buildingSketchesAtIngestionTimeTwoValues() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(
            this.getClass().getClassLoader().getResource("tuple/array_of_doubles_build_data_two_values.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "key", "value1", "value2")
        ),
        List.of(
            new ArrayOfDoublesSketchAggregatorFactory("sketch", "key", 1024, List.of("value1", "value2"), null)
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
                    .setAggregatorSpecs(
                        new ArrayOfDoublesSketchAggregatorFactory("sketch", "sketch", 1024, null, 2)
                    )
                    .setPostAggregatorSpecs(
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "estimate",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            2,
                            null
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "union",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "union", "UNION", 1024, 2,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "intersection",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "intersection", "INTERSECT", 1024, 2,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "anotb",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "anotb", "NOT", 1024, 2,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToMeansPostAggregator(
                            "means",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("sketch", 40.0, (double) row.get(0), 0);
    Assert.assertEquals("estimate", 40.0, (double) row.get(1), 0);
    Assert.assertEquals("union", 40.0, (double) row.get(3), 0);
    Assert.assertEquals("intersection", 40.0, (double) row.get(4), 0);
    Assert.assertEquals("anotb", 0, (double) row.get(5), 0);

    Object meansObj = row.get(6); // means
    Assert.assertTrue(meansObj instanceof double[]);
    double[] means = (double[]) meansObj;
    Assert.assertEquals(2, means.length);
    Assert.assertEquals(1.0, means[0], 0);
    Assert.assertEquals(2.0, means[1], 0);

    Object obj = row.get(2); // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(40, ds.getN());
    Assert.assertEquals(2.0, ds.getMinItem(), 0);
    Assert.assertEquals(2.0, ds.getMaxItem(), 0);
  }

  @Test
  public void buildingSketchesAtIngestionTimeTwoValuesAndNumericalKey() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(
            this.getClass().getClassLoader().getResource(
                "tuple/array_of_doubles_build_data_two_values_and_key_as_number.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(
                List.of(
                    new StringDimensionSchema("product"),
                    new LongDimensionSchema("key_num")
                )
            ),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "key", "key_num", "value1", "value2")
        ),
        List.of(
            new ArrayOfDoublesSketchAggregatorFactory("sketch", "key_num", 1024, List.of("value1", "value2"), null)
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
                    .setAggregatorSpecs(
                        new ArrayOfDoublesSketchAggregatorFactory("sketch", "sketch", 1024, null, 2)
                    )
                    .setPostAggregatorSpecs(
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "estimate",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            2,
                            null
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "union",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "union", "UNION", 1024, 2,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "intersection",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "intersection", "INTERSECT", 1024, 2,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "anotb",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "anotb", "NOT", 1024, 2,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToMeansPostAggregator(
                            "means",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("sketch", 40.0, (double) row.get(0), 0);
    Assert.assertEquals("estimate", 40.0, (double) row.get(1), 0);
    Assert.assertEquals("union", 40.0, (double) row.get(3), 0);
    Assert.assertEquals("intersection", 40.0, (double) row.get(4), 0);
    Assert.assertEquals("anotb", 0, (double) row.get(5), 0);

    Object meansObj = row.get(6); // means
    Assert.assertTrue(meansObj instanceof double[]);
    double[] means = (double[]) meansObj;
    Assert.assertEquals(2, means.length);
    Assert.assertEquals(1.0, means[0], 0);
    Assert.assertEquals(2.0, means[1], 0);

    Object obj = row.get(2); // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(40, ds.getN());
    Assert.assertEquals(2.0, ds.getMinItem(), 0);
    Assert.assertEquals(2.0, ds.getMaxItem(), 0);
  }

  @Test
  public void buildingSketchesAtIngestionTimeThreeValuesAndNulls() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(
            this.getClass()
                .getClassLoader()
                .getResource("tuple/array_of_doubles_build_data_three_values_and_nulls.tsv")
                .getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "key", "value1", "value2", "value3")
        ),
        List.of(
            new ArrayOfDoublesSketchAggregatorFactory("sketch", "key", 1024, List.of("value1", "value2", "value3"), null)
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
                    .setAggregatorSpecs(
                        new ArrayOfDoublesSketchAggregatorFactory("sketch", "sketch", 1024, null, 3)
                    )
                    .setPostAggregatorSpecs(
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "estimate",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            2,
                            null
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "union",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "union", "UNION", 1024, 3,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "intersection",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "intersection", "INTERSECT", 1024, 3,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "anotb",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "anotb", "NOT", 1024, 3,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToMeansPostAggregator(
                            "means",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch-with-nulls",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            3,
                            null
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("sketch", 30.0, (double) row.get(0), 0);
    Assert.assertEquals("estimate", 30.0, (double) row.get(1), 0);
    Assert.assertEquals("union", 30.0, (double) row.get(3), 0);
    Assert.assertEquals("intersection", 30.0, (double) row.get(4), 0);
    Assert.assertEquals("anotb", 0, (double) row.get(5), 0);

    Object meansObj = row.get(6); // means
    Assert.assertTrue(meansObj instanceof double[]);
    double[] means = (double[]) meansObj;
    Assert.assertEquals(3, means.length);
    Assert.assertEquals(1.0, means[0], 0);
    Assert.assertEquals(2.0, means[1], 0);
    Assert.assertEquals(3.0, means[2], 0.1);

    Object obj = row.get(2); // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(30, ds.getN());
    Assert.assertEquals(2.0, ds.getMinItem(), 0);
    Assert.assertEquals(2.0, ds.getMaxItem(), 0);

    Object objSketch2 = row.get(7); // quantiles-sketch-with-nulls
    Assert.assertTrue(objSketch2 instanceof DoublesSketch);
    DoublesSketch ds2 = (DoublesSketch) objSketch2;
    Assert.assertEquals(30, ds2.getN());
    Assert.assertEquals(3.0, ds2.getMinItem(), 0);
    Assert.assertEquals(3.0, ds2.getMaxItem(), 0);
  }

  @Test
  public void buildingSketchesAtQueryTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/array_of_doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(
                List.of(
                    new StringDimensionSchema("product"),
                    new StringDimensionSchema("key"),
                    new LongDimensionSchema("key_num")
                )
            ),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "key", "key_num", "value")
        ),
        List.of(
            new DoubleSumAggregatorFactory("value", "value")
        ),
        0, // minTimestamp
        Granularities.NONE,
        40, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
                    .setAggregatorSpecs(
                        new ArrayOfDoublesSketchAggregatorFactory("sketch", "key", 1024, List.of("value"), null),
                        new CountAggregatorFactory("cnt")
                    )
                    .setPostAggregatorSpecs(
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "estimate",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            null,
                            null
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "union",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "union", "UNION", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "intersection",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "intersection", "INTERSECT", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "anotb",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "anotb", "NOT", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("cnt", 40.0, Double.parseDouble(row.get(1).toString()), 0);
    Assert.assertEquals("sketch", 40.0, (double) row.get(0), 0);
    Assert.assertEquals("estimate", 40.0, Double.parseDouble(row.get(2).toString()), 0);
    Assert.assertEquals("union", 40.0, Double.parseDouble(row.get(4).toString()), 0);
    Assert.assertEquals("intersection", 40.0, Double.parseDouble(row.get(5).toString()), 0);
    Assert.assertEquals("anotb", 0, Double.parseDouble(row.get(6).toString()), 0);

    Object obj = row.get(3); // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(40, ds.getN());
    Assert.assertEquals(1.0, ds.getMinItem(), 0);
    Assert.assertEquals(1.0, ds.getMaxItem(), 0);
  }

  @Test
  public void buildingSketchesAtQueryTimeUseNumerical() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/array_of_doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(
                List.of(
                    new StringDimensionSchema("product"),
                    new StringDimensionSchema("key"),
                    new LongDimensionSchema("key_num")
                )
            ),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "key", "key_num", "value")
        ),
        List.of(
            new DoubleSumAggregatorFactory("value", "value")
        ),
        0, // minTimestamp
        Granularities.NONE,
        40, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
                    .setAggregatorSpecs(
                        new ArrayOfDoublesSketchAggregatorFactory("sketch", "key_num", 1024, List.of("value"), null),
                        new CountAggregatorFactory("cnt")
                    )
                    .setPostAggregatorSpecs(
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "estimate",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            null,
                            null
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "union",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "union", "UNION", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "intersection",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "intersection", "INTERSECT", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "anotb",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "anotb", "NOT", 1024, null,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("cnt", 40.0, Double.parseDouble(row.get(1).toString()), 0);
    Assert.assertEquals("sketch", 40.0, (double) row.get(0), 0);
    Assert.assertEquals("estimate", 40.0, Double.parseDouble(row.get(2).toString()), 0);
    Assert.assertEquals("union", 40.0, Double.parseDouble(row.get(4).toString()), 0);
    Assert.assertEquals("intersection", 40.0, Double.parseDouble(row.get(5).toString()), 0);
    Assert.assertEquals("anotb", 0, Double.parseDouble(row.get(6).toString()), 0);

    Object obj = row.get(3); // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(40, ds.getN());
    Assert.assertEquals(1.0, ds.getMinItem(), 0);
    Assert.assertEquals(1.0, ds.getMaxItem(), 0);
  }

  @Test
  public void buildingSketchesAtQueryTimeTimeseries() throws Exception
  {
    Sequence<Result<TimeseriesResultValue>> seq = tsHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/array_of_doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(
                List.of(
                    new StringDimensionSchema("product"),
                    new StringDimensionSchema("key"),
                    new LongDimensionSchema("key_num")
                )
            ),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "key", "key_num", "value")
        ),
        List.of(
            new DoubleSumAggregatorFactory("value", "value")
        ),
        0, // minTimestamp
        Granularities.NONE,
        40, // maxRowCount
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test_datasource")
              .granularity(Granularities.ALL)
              .intervals("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
              .aggregators(
                  new ArrayOfDoublesSketchAggregatorFactory("sketch", "key", 1024, List.of("value"), null),
                  new CountAggregatorFactory("cnt")
              )
              .postAggregators(
                  new ArrayOfDoublesSketchToEstimatePostAggregator(
                      "estimate",
                      new FieldAccessPostAggregator("sketch", "sketch")
                  ),
                  new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                      "quantiles-sketch",
                      new FieldAccessPostAggregator("sketch", "sketch"),
                      null,
                      null
                  ),
                  new ArrayOfDoublesSketchToEstimatePostAggregator(
                      "union",
                      new ArrayOfDoublesSketchSetOpPostAggregator(
                          "union", "UNION", 1024, null,
                          ImmutableList.of(
                              new FieldAccessPostAggregator("sketch", "sketch"),
                              new FieldAccessPostAggregator("sketch", "sketch")
                          )
                      )
                  ),
                  new ArrayOfDoublesSketchToEstimatePostAggregator(
                      "intersection",
                      new ArrayOfDoublesSketchSetOpPostAggregator(
                          "intersection", "INTERSECT", 1024, null,
                          ImmutableList.of(
                              new FieldAccessPostAggregator("sketch", "sketch"),
                              new FieldAccessPostAggregator("sketch", "sketch")
                          )
                      )
                  ),
                  new ArrayOfDoublesSketchToEstimatePostAggregator(
                      "anotb",
                      new ArrayOfDoublesSketchSetOpPostAggregator(
                          "anotb", "NOT", 1024, null,
                          ImmutableList.of(
                              new FieldAccessPostAggregator("sketch", "sketch"),
                              new FieldAccessPostAggregator("sketch", "sketch")
                          )
                      )
                  )
              )
              .build()
    );
    List<Result<TimeseriesResultValue>> results = seq.toList();
    Assert.assertEquals(1, results.size());
    TimeseriesResultValue row = results.get(0).getValue();
    Assert.assertEquals("cnt", 40.0, row.getDoubleMetric("cnt"), 0);
    Assert.assertEquals("sketch", 40.0, row.getDoubleMetric("sketch"), 0);
    Assert.assertEquals("estimate", 40.0, row.getDoubleMetric("estimate"), 0);
    Assert.assertEquals("union", 40.0, row.getDoubleMetric("union"), 0);
    Assert.assertEquals("intersection", 40.0, row.getDoubleMetric("intersection"), 0);
    Assert.assertEquals("anotb", 0, row.getDoubleMetric("anotb"), 0);

    Object obj = row.getMetric("quantiles-sketch"); // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(40, ds.getN());
    Assert.assertEquals(1.0, ds.getMinItem(), 0);
    Assert.assertEquals(1.0, ds.getMaxItem(), 0);
  }

  @Test
  public void buildingSketchesAtQueryTimeUsingNumericalTimeseries() throws Exception
  {
    Sequence<Result<TimeseriesResultValue>> seq = tsHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/array_of_doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(
                List.of(
                    new StringDimensionSchema("product"),
                    new StringDimensionSchema("key"),
                    new LongDimensionSchema("key_num")
                )
            ),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "key", "key_num", "value")
        ),
        List.of(
            new DoubleSumAggregatorFactory("value", "value")
        ),
        0, // minTimestamp
        Granularities.NONE,
        40, // maxRowCount
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test_datasource")
              .granularity(Granularities.ALL)
              .intervals("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
              .aggregators(
                  new ArrayOfDoublesSketchAggregatorFactory("sketch", "key_num", 1024, List.of("value"), null),
                  new CountAggregatorFactory("cnt")
              )
              .postAggregators(
                  new ArrayOfDoublesSketchToEstimatePostAggregator(
                      "estimate",
                      new FieldAccessPostAggregator("sketch", "sketch")
                  ),
                  new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                      "quantiles-sketch",
                      new FieldAccessPostAggregator("sketch", "sketch"),
                      null,
                      null
                  ),
                  new ArrayOfDoublesSketchToEstimatePostAggregator(
                      "union",
                      new ArrayOfDoublesSketchSetOpPostAggregator(
                          "union", "UNION", 1024, null,
                          ImmutableList.of(
                              new FieldAccessPostAggregator("sketch", "sketch"),
                              new FieldAccessPostAggregator("sketch", "sketch")
                          )
                      )
                  ),
                  new ArrayOfDoublesSketchToEstimatePostAggregator(
                      "intersection",
                      new ArrayOfDoublesSketchSetOpPostAggregator(
                          "intersection", "INTERSECT", 1024, null,
                          ImmutableList.of(
                              new FieldAccessPostAggregator("sketch", "sketch"),
                              new FieldAccessPostAggregator("sketch", "sketch")
                          )
                      )
                  ),
                  new ArrayOfDoublesSketchToEstimatePostAggregator(
                      "anotb",
                      new ArrayOfDoublesSketchSetOpPostAggregator(
                          "anotb", "NOT", 1024, null,
                          ImmutableList.of(
                              new FieldAccessPostAggregator("sketch", "sketch"),
                              new FieldAccessPostAggregator("sketch", "sketch")
                          )
                      )
                  )
              )
              .build()
    );
    List<Result<TimeseriesResultValue>> results = seq.toList();
    Assert.assertEquals(1, results.size());
    TimeseriesResultValue row = results.get(0).getValue();
    Assert.assertEquals("cnt", 40.0, row.getDoubleMetric("cnt"), 0);
    Assert.assertEquals("sketch", 40.0, row.getDoubleMetric("sketch"), 0);
    Assert.assertEquals("estimate", 40.0, row.getDoubleMetric("estimate"), 0);
    Assert.assertEquals("union", 40.0, row.getDoubleMetric("union"), 0);
    Assert.assertEquals("intersection", 40.0, row.getDoubleMetric("intersection"), 0);
    Assert.assertEquals("anotb", 0, row.getDoubleMetric("anotb"), 0);

    Object obj = row.getMetric("quantiles-sketch"); // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(40, ds.getN());
    Assert.assertEquals(1.0, ds.getMinItem(), 0);
    Assert.assertEquals(1.0, ds.getMaxItem(), 0);
  }

  // Two buckets with statistically significant difference.
  // See GenerateTestData class for details.
  @Test
  public void buildingSketchesAtQueryTimeTwoBucketsTest() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/bucket_test_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMdd", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("label", "userid"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "label", "userid", "parameter")
        ),
        List.of(
            new DoubleSumAggregatorFactory("parameter", "parameter")
        ),
        0, // minTimestamp
        Granularities.NONE,
        2000, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z")
                    .setAggregatorSpecs(
                        new FilteredAggregatorFactory(
                            new ArrayOfDoublesSketchAggregatorFactory(
                                "sketch-test", "userid", null, List.of("parameter"), null
                            ),
                            new SelectorDimFilter("label", "test", null)
                        ),
                        new FilteredAggregatorFactory(
                            new ArrayOfDoublesSketchAggregatorFactory(
                                "sketch-control", "userid", null, List.of("parameter"), null
                            ),
                            new SelectorDimFilter("label", "control", null)
                        )
                    )
                    .setPostAggregatorSpecs(
                        new ArrayOfDoublesSketchTTestPostAggregator(
                            "p-value",
                            ImmutableList.of(
                                new FieldAccessPostAggregator("sketch-test", "sketch-test"),
                                new FieldAccessPostAggregator("sketch-control", "sketch-control")
                            )
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Object obj = row.get(2); // p-value
    Assert.assertTrue(obj instanceof double[]);
    double[] array = (double[]) obj;
    Assert.assertEquals(1, array.length);
    double pValue = array[0];
    // Test and control buckets were constructed to have different means, so we
    // expect very low p value
    Assert.assertEquals(0, pValue, 0.001);
  }

  // Three buckets with null values
  @Test
  public void buildingSketchesAtQueryTimeWithNullsTest() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass()
                     .getClassLoader()
                     .getResource("tuple/array_of_doubles_build_data_three_values_and_nulls.tsv")
                     .getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product", "key"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "key", "value1", "value2", "value3")
        ),
        List.of(
            new DoubleSumAggregatorFactory("value1", "value1"),
            new DoubleSumAggregatorFactory("value2", "value2"),
            new DoubleSumAggregatorFactory("value3", "value3")
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
                    .setVirtualColumns(
                        new ExpressionVirtualColumn(
                            "nonulls3", "nvl(value3, 0.0)", ColumnType.DOUBLE, TestExprMacroTable.INSTANCE
                        )
                    )
                    .setAggregatorSpecs(
                        new ArrayOfDoublesSketchAggregatorFactory(
                            "sketch", "key", 1024, List.of("value1", "value2", "value3"), null
                        ),
                        new ArrayOfDoublesSketchAggregatorFactory(
                            "sketchNoNulls", "key", 1024, List.of("value1", "value2", "nonulls3"), null
                        )
                    )
                    .setPostAggregatorSpecs(
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "estimate",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "estimateNoNulls",
                            new FieldAccessPostAggregator("sketchNoNulls", "sketchNoNulls")
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            2,
                            null
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "union",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "union", "UNION", 1024, 3,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "intersection",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "intersection", "INTERSECT", 1024, 3,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToEstimatePostAggregator(
                            "anotb",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "anotb", "NOT", 1024, 3,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new FieldAccessPostAggregator("sketch", "sketch")
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToMeansPostAggregator(
                            "means",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch-with-nulls",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            3,
                            null
                        ),
                        new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                            "quantiles-sketch-with-no-nulls",
                            new FieldAccessPostAggregator("sketchNoNulls", "sketchNoNulls"),
                            3,
                            null
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("sketch", 30.0, (double) row.get(0), 0);
    Assert.assertEquals("sketchNoNulls", 40.0, (double) row.get(1), 0);
    Assert.assertEquals("estimate", 30.0, (double) row.get(2), 0);
    Assert.assertEquals("estimateNoNulls", 40.0, (double) row.get(3), 0);
    Assert.assertEquals("union", 30.0, (double) row.get(5), 0);
    Assert.assertEquals("intersection", 30.0, (double) row.get(6), 0);
    Assert.assertEquals("anotb", 0, (double) row.get(7), 0);

    Object meansObj = row.get(8); // means
    Assert.assertTrue(meansObj instanceof double[]);
    double[] means = (double[]) meansObj;
    Assert.assertEquals(3, means.length);
    Assert.assertEquals(1.0, means[0], 0);
    Assert.assertEquals(2.0, means[1], 0);
    Assert.assertEquals(3.0, means[2], 0.1);

    Object obj = row.get(4); // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(30, ds.getN());
    Assert.assertEquals(2.0, ds.getMinItem(), 0);
    Assert.assertEquals(2.0, ds.getMaxItem(), 0);

    Object objSketch2 = row.get(9); // quantiles-sketch-with-nulls
    Assert.assertTrue(objSketch2 instanceof DoublesSketch);
    DoublesSketch ds2 = (DoublesSketch) objSketch2;
    Assert.assertEquals(30, ds2.getN());
    Assert.assertEquals(3.0, ds2.getMinItem(), 0);
    Assert.assertEquals(3.0, ds2.getMaxItem(), 0);

    Object objSketch3 = row.get(10); // quantiles-sketch-no-nulls
    Assert.assertTrue(objSketch3 instanceof DoublesSketch);
    DoublesSketch ds3 = (DoublesSketch) objSketch3;
    Assert.assertEquals(40, ds3.getN());
    Assert.assertEquals(0.0, ds3.getMinItem(), 0);
    Assert.assertEquals(3.0, ds3.getMaxItem(), 0);
  }


  //Test ConstantTupleSketchPost-Agg and Base64 Encoding

  @Test
  public void testConstantAndBase64WithEstimateSumPostAgg() throws Exception
  {
    final String externalSketchBase64 =
        "AQEJAwgCzJP/////////fwIAAAAAAAAAbakWvEpmYR4+utyjb2+2IAAAAAAAAPA/AAAAAAAAAEAAAAAAAADwPwAAAAAAAABA";
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/array_of_doubles_sketch_data_two_values.tsv")
            .getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "sketch")
        ),
        List.of(
            new ArrayOfDoublesSketchAggregatorFactory("sketch", "sketch", 1024, null, 2)
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z")
                    .setAggregatorSpecs(
                        new ArrayOfDoublesSketchAggregatorFactory("sketch", "sketch", 1024, null, 2)
                    )
                    .setPostAggregatorSpecs(
                        new ArrayOfDoublesSketchToMetricsSumEstimatePostAggregator(
                            "estimateSum",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new ArrayOfDoublesSketchToMetricsSumEstimatePostAggregator(
                            "intersection",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "intersection", "INTERSECT", 1024, 2,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new ArrayOfDoublesSketchConstantPostAggregator(
                                        "external_sketch", externalSketchBase64
                                    )
                                )
                            )
                        ),
                        new ArrayOfDoublesSketchToBase64StringPostAggregator(
                            "intersectionString",
                            new ArrayOfDoublesSketchSetOpPostAggregator(
                                "intersection", "INTERSECT", 1024, 2,
                                ImmutableList.of(
                                    new FieldAccessPostAggregator("sketch", "sketch"),
                                    new ArrayOfDoublesSketchConstantPostAggregator(
                                        "external_sketch", externalSketchBase64
                                    )
                                )
                            )
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("sketch", 40.0, (double) row.get(0), 0);
    //Assert.assertEquals("estimateSum", 40.0, (double) row.get(1), 0);

    Object estimateSumObj = row.get(1); // estimateSum
    Assert.assertTrue(estimateSumObj instanceof double[]);
    double[] estimateSum = (double[]) estimateSumObj;
    Assert.assertEquals(2, estimateSum.length);
    Assert.assertEquals(40.0, estimateSum[0], 0);
    Assert.assertEquals(80.0, estimateSum[1], 0);

    Object intersectEstimateSumObj = row.get(2); // intersectEstimateSum
    Assert.assertTrue(intersectEstimateSumObj instanceof double[]);
    double[] intersectEstimateSum = (double[]) intersectEstimateSumObj;
    Assert.assertEquals(2, intersectEstimateSum.length);
    Assert.assertEquals(4.0, intersectEstimateSum[0], 0);
    Assert.assertEquals(8.0, intersectEstimateSum[1], 0);

    //convert intersected to base64 string
    Assert.assertEquals("intersectionString", "AQEJAwgCzJP/////////fwIAAAAAAAAAbakWvEpmYR4+utyjb2+2IAAAAAAAAABAAAAAAAAAEEAAAAAAAAAAQAAAAAAAABBA", (String) row.get(3));


  }
}
