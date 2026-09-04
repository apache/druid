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

package org.apache.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DoublesSketchAggregatorTest extends InitializedNullHandlingTest
{
  private GroupByQueryConfig config;
  private AggregationTestHelper helper;
  private AggregationTestHelper timeSeriesHelper;

  @TempDir
  private File tempFolder;

  public void initDoublesSketchAggregatorTest(final GroupByQueryConfig config, final String vectorize)
  {
    this.config = config;
    DoublesSketchModule.registerSerde();
    DoublesSketchModule module = new DoublesSketchModule();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(),
        config,
        tempFolder
    ).withQueryContext(
        QueryContext.ofMap(QueryContextParameters.VECTORIZE, QueryContextParameters.VECTORIZE.parse(vectorize))
    );
    timeSeriesHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelperWithTempDir(
        module.getJacksonModules(),
        tempFolder
    ).withQueryContext(
        QueryContext.ofMap(QueryContextParameters.VECTORIZE, QueryContextParameters.VECTORIZE.parse(vectorize))
    );
  }

  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (String vectorize : new String[]{"false", "true", "force"}) {
        constructors.add(new Object[]{config, vectorize});
      }
    }
    return constructors;
  }

  @AfterEach
  public void teardown() throws IOException
  {
    helper.close();
  }

  // this is to test Json properties and equals
  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "groupByConfig = {0}, vectorize = {1}")
  public void serializeDeserializeFactoryWithFieldName(final GroupByQueryConfig config, final String vectorize) throws Exception
  {
    initDoublesSketchAggregatorTest(config, vectorize);
    ObjectMapper objectMapper = new DefaultObjectMapper();
    new DoublesSketchModule().getJacksonModules().forEach(objectMapper::registerModule);
    DoublesSketchAggregatorFactory factory = new DoublesSketchAggregatorFactory("name", "filedName", 128);

    AggregatorFactory other = objectMapper.readValue(
        objectMapper.writeValueAsString(factory),
        AggregatorFactory.class
    );

    Assertions.assertEquals(factory, other);
  }

  // this is to test Json properties and equals for the combining factory
  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "groupByConfig = {0}, vectorize = {1}")
  public void serializeDeserializeCombiningFactoryWithFieldName(final GroupByQueryConfig config, final String vectorize) throws Exception
  {
    initDoublesSketchAggregatorTest(config, vectorize);
    ObjectMapper objectMapper = new DefaultObjectMapper();
    new DoublesSketchModule().getJacksonModules().forEach(objectMapper::registerModule);
    DoublesSketchAggregatorFactory factory = new DoublesSketchMergeAggregatorFactory("name", 128);

    AggregatorFactory other = objectMapper.readValue(
        objectMapper.writeValueAsString(factory),
        AggregatorFactory.class
    );

    Assertions.assertEquals(factory, other);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "groupByConfig = {0}, vectorize = {1}")
  public void ingestingSketches(final GroupByQueryConfig config, final String vectorize) throws Exception
  {
    initDoublesSketchAggregatorTest(config, vectorize);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/doubles_sketch_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(List.of("timestamp", "product", "sketch")),
        List.of(
            new DoublesSketchAggregatorFactory("sketch", "sketch", 128),
            new DoublesSketchAggregatorFactory("non_existent_sketch", "non_existent_sketch", 128)
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.of("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z"))
                    .setAggregatorSpecs(
                        new DoublesSketchAggregatorFactory("sketch", "sketch", 128),
                        new DoublesSketchAggregatorFactory("non_existent_sketch", "non_existent_sketch", 128)
                    )
                    .setPostAggregatorSpecs(
                        new DoublesSketchToQuantilesPostAggregator(
                            "quantiles",
                            new FieldAccessPostAggregator("field", "sketch"),
                            new double[]{0, 0.5, 1}
                        ),
                        new DoublesSketchToHistogramPostAggregator(
                            "histogram",
                            new FieldAccessPostAggregator("field", "sketch"),
                            new double[]{0.25, 0.5, 0.75},
                            null
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    Object nonExistentSketchObject = row.get(1);
    Assertions.assertTrue(nonExistentSketchObject instanceof Long);
    long nonExistentSketchValue = (long) nonExistentSketchObject;
    Assertions.assertEquals(0, nonExistentSketchValue);

    Object sketchObject = row.get(0);
    Assertions.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assertions.assertEquals(400, sketchValue);

    // post agg
    Object quantilesObject = row.get(2);
    Assertions.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assertions.assertEquals(0, quantiles[0], 0.05); // min value
    Assertions.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assertions.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.get(3);
    Assertions.assertTrue(histogramObject instanceof double[]);
    double[] histogram = (double[]) histogramObject;
    for (final double bin : histogram) {
      // 400 items uniformly distributed into 4 bins
      Assertions.assertEquals(100, bin, 100 * 0.2);
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "groupByConfig = {0}, vectorize = {1}")
  public void buildingSketchesAtIngestionTime(final GroupByQueryConfig config, final String vectorize) throws Exception
  {
    initDoublesSketchAggregatorTest(config, vectorize);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            DimensionsSpec.builder()
                          .setDefaultSchemaDimensions(List.of("product"))
                          .setDimensionExclusions(List.of("sequenceNumber"))
                          .build(),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "sequenceNumber", "product", "value", "valueWithNulls")
        ),
        List.of(
            new DoublesSketchAggregatorFactory("sketch", "value", 128),
            new DoublesSketchAggregatorFactory("sketchWithNulls", "valueWithNulls", 128)
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.of("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z"))
                    .setAggregatorSpecs(
                        new DoublesSketchAggregatorFactory("sketch", "sketch", 128),
                        new DoublesSketchAggregatorFactory("sketchWithNulls", "sketchWithNulls", 128),
                        new DoublesSketchAggregatorFactory("non_existent_sketch", "non_existent_sketch", 128)
                    )
                    .setPostAggregatorSpecs(
                        new DoublesSketchToQuantilesPostAggregator(
                            "quantiles",
                            new FieldAccessPostAggregator("field", "sketch"),
                            new double[]{0, 0.5, 1}
                        ),
                        new DoublesSketchToHistogramPostAggregator(
                            "histogram",
                            new FieldAccessPostAggregator("field", "sketch"),
                            new double[]{0.25, 0.5, 0.75},
                            null
                        ),
                        new DoublesSketchToQuantilesPostAggregator(
                            "quantilesWithNulls",
                            new FieldAccessPostAggregator("field", "sketchWithNulls"),
                            new double[]{0, 0.5, 1}
                        ),
                        new DoublesSketchToHistogramPostAggregator(
                            "histogramWithNulls",
                            new FieldAccessPostAggregator("field", "sketchWithNulls"),
                            new double[]{6.25, 7.5, 8.75},
                            null
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    Object sketchObject = row.get(0);
    Assertions.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assertions.assertEquals(400, sketchValue);

    Object sketchObjectWithNulls = row.get(1);
    Assertions.assertTrue(sketchObjectWithNulls instanceof Long);
    long sketchValueWithNulls = (long) sketchObjectWithNulls;
    Assertions.assertEquals(377, sketchValueWithNulls);

    // post agg
    Object quantilesObject = row.get(3);
    Assertions.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assertions.assertEquals(0, quantiles[0], 0.05); // min value
    Assertions.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assertions.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.get(4);
    Assertions.assertTrue(histogramObject instanceof double[]);
    double[] histogram = (double[]) histogramObject;
    Assertions.assertEquals(4, histogram.length);
    for (final double bin : histogram) {
      Assertions.assertEquals(100, bin, 100 * 0.2); // 400 items uniformly distributed into 4 bins
    }

    // post agg with nulls
    Object quantilesObjectWithNulls = row.get(5);
    Assertions.assertTrue(quantilesObjectWithNulls instanceof double[]);
    double[] quantilesWithNulls = (double[]) quantilesObjectWithNulls;
    Assertions.assertEquals(5.0, quantilesWithNulls[0], 0.05); // min value
    Assertions.assertEquals(7.55, quantilesWithNulls[1], 0.05); // median value
    Assertions.assertEquals(10.0, quantilesWithNulls[2], 0.05); // max value

    // post agg with nulls
    Object histogramObjectWithNulls = row.get(6);
    Assertions.assertTrue(histogramObjectWithNulls instanceof double[]);
    double[] histogramWithNulls = (double[]) histogramObjectWithNulls;
    Assertions.assertEquals(4, histogramWithNulls.length);
    for (final double bin : histogramWithNulls) {
      Assertions.assertEquals(100, bin, 50); // distribution is skewed due to nulls
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "groupByConfig = {0}, vectorize = {1}")
  public void buildingSketchesAtQueryTime(final GroupByQueryConfig config, final String vectorize) throws Exception
  {
    initDoublesSketchAggregatorTest(config, vectorize);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("sequenceNumber", "product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "sequenceNumber", "product", "value", "valueWithNulls")
        ),
        List.of(
            new DoubleSumAggregatorFactory("value", "value"),
            new DoubleSumAggregatorFactory("valueWithNulls", "valueWithNulls")
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.of("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z"))
                    .setAggregatorSpecs(
                        new DoublesSketchAggregatorFactory("sketch", "value", 128),
                        new DoublesSketchAggregatorFactory("sketchWithNulls", "valueWithNulls", 128)
                    )
                    .setPostAggregatorSpecs(
                        new DoublesSketchToQuantilePostAggregator(
                            "quantile",
                            new FieldAccessPostAggregator("field", "sketch"),
                            0.5
                        ),
                        new DoublesSketchToQuantilesPostAggregator(
                            "quantiles",
                            new FieldAccessPostAggregator("field", "sketch"),
                            new double[]{0, 0.5, 1}
                        ),
                        new DoublesSketchToHistogramPostAggregator(
                            "histogram",
                            new FieldAccessPostAggregator("field", "sketch"),
                            new double[]{0.25, 0.5, 0.75},
                            null
                        ),
                        new DoublesSketchToQuantilePostAggregator(
                            "quantileWithNulls",
                            new FieldAccessPostAggregator("field", "sketchWithNulls"),
                            0.5
                        ),
                        new DoublesSketchToQuantilesPostAggregator(
                            "quantilesWithNulls",
                            new FieldAccessPostAggregator("field", "sketchWithNulls"),
                            new double[]{0, 0.5, 1}
                        ),
                        new DoublesSketchToHistogramPostAggregator(
                            "histogramWithNulls",
                            new FieldAccessPostAggregator("field", "sketchWithNulls"),
                            new double[]{6.25, 7.5, 8.75},
                            null
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    Object sketchObject = row.get(0);
    Assertions.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assertions.assertEquals(400, sketchValue);

    Object sketchObjectWithNulls = row.get(1);
    Assertions.assertTrue(sketchObjectWithNulls instanceof Long);
    long sketchValueWithNulls = (long) sketchObjectWithNulls;
    Assertions.assertEquals(377, sketchValueWithNulls);

    // post agg
    Object quantileObject = row.get(2);
    Assertions.assertTrue(quantileObject instanceof Double);
    Assertions.assertEquals(0.5, (double) quantileObject, 0.05); // median value

    // post agg
    Object quantilesObject = row.get(3);
    Assertions.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assertions.assertEquals(0, quantiles[0], 0.05); // min value
    Assertions.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assertions.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.get(4);
    Assertions.assertTrue(histogramObject instanceof double[]);
    double[] histogram = (double[]) histogramObject;
    for (final double bin : histogram) {
      Assertions.assertEquals(100, bin, 100 * 0.2); // 400 items uniformly
      // distributed into 4 bins
    }

    // post agg with nulls
    Object quantileObjectWithNulls = row.get(5);
    Assertions.assertTrue(quantileObjectWithNulls instanceof Double);
    Assertions.assertEquals(
        7.5,
        (double) quantileObjectWithNulls,
        0.1
    ); // median value

    // post agg with nulls
    Object quantilesObjectWithNulls = row.get(6);
    Assertions.assertTrue(quantilesObjectWithNulls instanceof double[]);
    double[] quantilesWithNulls = (double[]) quantilesObjectWithNulls;
    Assertions.assertEquals(5.0, quantilesWithNulls[0], 0.05); // min value
    Assertions.assertEquals(7.5, quantilesWithNulls[1], 0.1); // median value
    Assertions.assertEquals(10.0, quantilesWithNulls[2], 0.05); // max value

    // post agg with nulls
    Object histogramObjectWithNulls = row.get(7);
    Assertions.assertTrue(histogramObjectWithNulls instanceof double[]);
    double[] histogramWithNulls = (double[]) histogramObjectWithNulls;
    for (final double bin : histogramWithNulls) {
      Assertions.assertEquals(100, bin, 80); // distribution is skewed due to nulls/0s
      // distributed into 4 bins
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "groupByConfig = {0}, vectorize = {1}")
  public void queryingDataWithFieldNameValueAsFloatInsteadOfSketch(final GroupByQueryConfig config, final String vectorize) throws Exception
  {
    initDoublesSketchAggregatorTest(config, vectorize);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("sequenceNumber", "product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(List.of("timestamp", "sequenceNumber", "product", "value")),
        List.of(
            new DoubleSumAggregatorFactory("value", "value")
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.of("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z"))
                    .setAggregatorSpecs(
                        new DoublesSketchAggregatorFactory("sketch", "value", 128)
                    )
                    .setPostAggregatorSpecs(
                        new DoublesSketchToQuantilePostAggregator(
                            "quantile",
                            new FieldAccessPostAggregator("field", "sketch"),
                            0.5
                        ),
                        new DoublesSketchToQuantilesPostAggregator(
                            "quantiles",
                            new FieldAccessPostAggregator("field", "sketch"),
                            new double[]{0, 0.5, 1}
                        ),
                        new DoublesSketchToHistogramPostAggregator(
                            "histogram",
                            new FieldAccessPostAggregator("field", "sketch"),
                            new double[]{0.25, 0.5, 0.75},
                            null
                        )
                    )
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    Object sketchObject = row.get(0);
    Assertions.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assertions.assertEquals(400, sketchValue);

    // post agg
    Object quantileObject = row.get(1);
    Assertions.assertTrue(quantileObject instanceof Double);
    Assertions.assertEquals(0.5, (double) quantileObject, 0.05); // median value

    // post agg
    Object quantilesObject = row.get(2);
    Assertions.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assertions.assertEquals(0, quantiles[0], 0.05); // min value
    Assertions.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assertions.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.get(3);
    Assertions.assertTrue(histogramObject instanceof double[]);
    double[] histogram = (double[]) histogramObject;
    for (final double bin : histogram) {
      Assertions.assertEquals(100, bin, 100 * 0.2); // 400 items uniformly
      // distributed into 4 bins
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "groupByConfig = {0}, vectorize = {1}")
  public void timeSeriesQueryInputAsFloat(final GroupByQueryConfig config, final String vectorize) throws Exception
  {
    initDoublesSketchAggregatorTest(config, vectorize);
    Sequence<Result<TimeseriesResultValue>> seq = timeSeriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("sequenceNumber", "product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(List.of("timestamp", "sequenceNumber", "product", "value")),
        List.of(
            new DoubleSumAggregatorFactory("value", "value")
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test_datasource")
              .granularity(Granularities.ALL)
              .intervals(Intervals.of("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z").toString())
              .aggregators(
                  new DoublesSketchAggregatorFactory("sketch", "value", 128)
              )
              .postAggregators(
                  new DoublesSketchToQuantilePostAggregator(
                      "quantile1",
                      new FieldAccessPostAggregator("field", "sketch"),
                      0.5
                  ),
                  new DoublesSketchToQuantilesPostAggregator(
                      "quantiles1",
                      new FieldAccessPostAggregator("field", "sketch"),
                      new double[]{0, 0.5, 1}
                  ),
                  new DoublesSketchToHistogramPostAggregator(
                      "histogram1",
                      new FieldAccessPostAggregator("field", "sketch"),
                      new double[]{0.25, 0.5, 0.75},
                      null
                  )
              )
              .build()
    );
    List<Result<TimeseriesResultValue>> results = seq.toList();
    Assertions.assertEquals(1, results.size());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "groupByConfig = {0}, vectorize = {1}")
  public void testSuccessWhenMaxStreamLengthHit(final GroupByQueryConfig config, final String vectorize) throws Exception
  {
    initDoublesSketchAggregatorTest(config, vectorize);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("sequenceNumber", "product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(List.of("timestamp", "sequenceNumber", "product", "value")),
        List.of(
            new DoubleSumAggregatorFactory("value", "value")
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.of("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z"))
                    .setAggregatorSpecs(
                        new DoublesSketchAggregatorFactory("sketch", "value", 128, 10L, null)
                    )
                    .setPostAggregatorSpecs(
                        new DoublesSketchToQuantilePostAggregator(
                            "quantile",
                            new FieldAccessPostAggregator("field", "sketch"),
                            0.5
                        ),
                        new DoublesSketchToQuantilesPostAggregator(
                            "quantiles",
                            new FieldAccessPostAggregator("field", "sketch"),
                            new double[]{0, 0.5, 1}
                        ),
                        new DoublesSketchToHistogramPostAggregator(
                            "histogram",
                            new FieldAccessPostAggregator("field", "sketch"),
                            new double[]{0.25, 0.5, 0.75},
                            null
                        )
                    )
                    .build()
    );
    seq.toList();
  }
}
