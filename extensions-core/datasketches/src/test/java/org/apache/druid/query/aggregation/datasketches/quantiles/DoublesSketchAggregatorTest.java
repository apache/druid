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
import com.google.common.collect.ImmutableMap;
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
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
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
public class DoublesSketchAggregatorTest extends InitializedNullHandlingTest
{
  private final GroupByQueryConfig config;
  private final AggregationTestHelper helper;
  private final AggregationTestHelper timeSeriesHelper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public DoublesSketchAggregatorTest(final GroupByQueryConfig config, final String vectorize)
  {
    this.config = config;
    DoublesSketchModule.registerSerde();
    DoublesSketchModule module = new DoublesSketchModule();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(),
        config,
        tempFolder
    ).withQueryContext(ImmutableMap.of(QueryContexts.VECTORIZE_KEY, vectorize));
    timeSeriesHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(
        module.getJacksonModules(),
        tempFolder
    ).withQueryContext(ImmutableMap.of(QueryContexts.VECTORIZE_KEY, vectorize));
  }

  @Parameterized.Parameters(name = "groupByConfig = {0}, vectorize = {1}")
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

  @After
  public void teardown() throws IOException
  {
    helper.close();
  }

  // this is to test Json properties and equals
  @Test
  public void serializeDeserializeFactoryWithFieldName() throws Exception
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    new DoublesSketchModule().getJacksonModules().forEach(objectMapper::registerModule);
    DoublesSketchAggregatorFactory factory = new DoublesSketchAggregatorFactory("name", "filedName", 128);

    AggregatorFactory other = objectMapper.readValue(
        objectMapper.writeValueAsString(factory),
        AggregatorFactory.class
    );

    Assert.assertEquals(factory, other);
  }

  // this is to test Json properties and equals for the combining factory
  @Test
  public void serializeDeserializeCombiningFactoryWithFieldName() throws Exception
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    new DoublesSketchModule().getJacksonModules().forEach(objectMapper::registerModule);
    DoublesSketchAggregatorFactory factory = new DoublesSketchMergeAggregatorFactory("name", 128);

    AggregatorFactory other = objectMapper.readValue(
        objectMapper.writeValueAsString(factory),
        AggregatorFactory.class
    );

    Assert.assertEquals(factory, other);
  }

  @Test
  public void ingestingSketches() throws Exception
  {
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
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    Object nonExistentSketchObject = row.get(1);
    Assert.assertTrue(nonExistentSketchObject instanceof Long);
    long nonExistentSketchValue = (long) nonExistentSketchObject;
    Assert.assertEquals(0, nonExistentSketchValue);

    Object sketchObject = row.get(0);
    Assert.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assert.assertEquals(400, sketchValue);

    // post agg
    Object quantilesObject = row.get(2);
    Assert.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assert.assertEquals(0, quantiles[0], 0.05); // min value
    Assert.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assert.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.get(3);
    Assert.assertTrue(histogramObject instanceof double[]);
    double[] histogram = (double[]) histogramObject;
    for (final double bin : histogram) {
      // 400 items uniformly distributed into 4 bins
      Assert.assertEquals(100, bin, 100 * 0.2);
    }
  }

  @Test
  public void buildingSketchesAtIngestionTime() throws Exception
  {
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
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    Object sketchObject = row.get(0);
    Assert.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assert.assertEquals(400, sketchValue);

    Object sketchObjectWithNulls = row.get(1);
    Assert.assertTrue(sketchObjectWithNulls instanceof Long);
    long sketchValueWithNulls = (long) sketchObjectWithNulls;
    Assert.assertEquals(377, sketchValueWithNulls);

    // post agg
    Object quantilesObject = row.get(3);
    Assert.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assert.assertEquals(0, quantiles[0], 0.05); // min value
    Assert.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assert.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.get(4);
    Assert.assertTrue(histogramObject instanceof double[]);
    double[] histogram = (double[]) histogramObject;
    Assert.assertEquals(4, histogram.length);
    for (final double bin : histogram) {
      Assert.assertEquals(100, bin, 100 * 0.2); // 400 items uniformly distributed into 4 bins
    }

    // post agg with nulls
    Object quantilesObjectWithNulls = row.get(5);
    Assert.assertTrue(quantilesObjectWithNulls instanceof double[]);
    double[] quantilesWithNulls = (double[]) quantilesObjectWithNulls;
    Assert.assertEquals(5.0, quantilesWithNulls[0], 0.05); // min value
    Assert.assertEquals(7.55, quantilesWithNulls[1], 0.05); // median value
    Assert.assertEquals(10.0, quantilesWithNulls[2], 0.05); // max value

    // post agg with nulls
    Object histogramObjectWithNulls = row.get(6);
    Assert.assertTrue(histogramObjectWithNulls instanceof double[]);
    double[] histogramWithNulls = (double[]) histogramObjectWithNulls;
    Assert.assertEquals(4, histogramWithNulls.length);
    for (final double bin : histogramWithNulls) {
      Assert.assertEquals(100, bin, 50); // distribution is skewed due to nulls
    }
  }

  @Test
  public void buildingSketchesAtQueryTime() throws Exception
  {
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
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    Object sketchObject = row.get(0);
    Assert.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assert.assertEquals(400, sketchValue);

    Object sketchObjectWithNulls = row.get(1);
    Assert.assertTrue(sketchObjectWithNulls instanceof Long);
    long sketchValueWithNulls = (long) sketchObjectWithNulls;
    Assert.assertEquals(377, sketchValueWithNulls);

    // post agg
    Object quantileObject = row.get(2);
    Assert.assertTrue(quantileObject instanceof Double);
    Assert.assertEquals(0.5, (double) quantileObject, 0.05); // median value

    // post agg
    Object quantilesObject = row.get(3);
    Assert.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assert.assertEquals(0, quantiles[0], 0.05); // min value
    Assert.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assert.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.get(4);
    Assert.assertTrue(histogramObject instanceof double[]);
    double[] histogram = (double[]) histogramObject;
    for (final double bin : histogram) {
      Assert.assertEquals(100, bin, 100 * 0.2); // 400 items uniformly
      // distributed into 4 bins
    }

    // post agg with nulls
    Object quantileObjectWithNulls = row.get(5);
    Assert.assertTrue(quantileObjectWithNulls instanceof Double);
    Assert.assertEquals(
        7.5,
        (double) quantileObjectWithNulls,
        0.1
    ); // median value

    // post agg with nulls
    Object quantilesObjectWithNulls = row.get(6);
    Assert.assertTrue(quantilesObjectWithNulls instanceof double[]);
    double[] quantilesWithNulls = (double[]) quantilesObjectWithNulls;
    Assert.assertEquals(5.0, quantilesWithNulls[0], 0.05); // min value
    Assert.assertEquals(7.5, quantilesWithNulls[1], 0.1); // median value
    Assert.assertEquals(10.0, quantilesWithNulls[2], 0.05); // max value

    // post agg with nulls
    Object histogramObjectWithNulls = row.get(7);
    Assert.assertTrue(histogramObjectWithNulls instanceof double[]);
    double[] histogramWithNulls = (double[]) histogramObjectWithNulls;
    for (final double bin : histogramWithNulls) {
      Assert.assertEquals(100, bin, 80); // distribution is skewed due to nulls/0s
      // distributed into 4 bins
    }
  }

  @Test
  public void queryingDataWithFieldNameValueAsFloatInsteadOfSketch() throws Exception
  {
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
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    Object sketchObject = row.get(0);
    Assert.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assert.assertEquals(400, sketchValue);

    // post agg
    Object quantileObject = row.get(1);
    Assert.assertTrue(quantileObject instanceof Double);
    Assert.assertEquals(0.5, (double) quantileObject, 0.05); // median value

    // post agg
    Object quantilesObject = row.get(2);
    Assert.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assert.assertEquals(0, quantiles[0], 0.05); // min value
    Assert.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assert.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.get(3);
    Assert.assertTrue(histogramObject instanceof double[]);
    double[] histogram = (double[]) histogramObject;
    for (final double bin : histogram) {
      Assert.assertEquals(100, bin, 100 * 0.2); // 400 items uniformly
      // distributed into 4 bins
    }
  }

  @Test
  public void timeSeriesQueryInputAsFloat() throws Exception
  {
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
    Assert.assertEquals(1, results.size());
  }

  @Test
  public void testSuccessWhenMaxStreamLengthHit() throws Exception
  {
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
