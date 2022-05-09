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

package org.apache.druid.query.aggregation.datasketches.kll;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
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
public class KllDoublesSketchAggregatorTest extends InitializedNullHandlingTest
{
  private final GroupByQueryConfig config;
  private final AggregationTestHelper helper;
  private final AggregationTestHelper timeSeriesHelper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public KllDoublesSketchAggregatorTest(final GroupByQueryConfig config, final String vectorize)
  {
    this.config = config;
    KllSketchModule.registerSerde();
    KllSketchModule module = new KllSketchModule();
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
    new KllSketchModule().getJacksonModules().forEach(objectMapper::registerModule);
    KllDoublesSketchAggregatorFactory factory =
        new KllDoublesSketchAggregatorFactory("name", "filedName", 200, 1000000000L);

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
    new KllSketchModule().getJacksonModules().forEach(objectMapper::registerModule);
    KllDoublesSketchAggregatorFactory factory = new KllDoublesSketchMergeAggregatorFactory("name", 200);

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
        new File(this.getClass().getClassLoader().getResource("kll/kll_doubles_sketch_data.tsv").getFile()),
        String.join(
            "\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHH\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"product\"],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"product\", \"sketch\"]",
            "  }",
            "}"
        ),
        String.join(
            "\n",
            "[",
            "  {\"type\": \"KllDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"k\": 200},",
            "  {\"type\": \"KllDoublesSketch\", \"name\": \"non_existent_sketch\", \"fieldName\": \"non_existent_sketch\", \"k\": 200}",
            "]"
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        String.join(
            "\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"KllDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"k\": 200},",
            "    {\"type\": \"KllDoublesSketch\", \"name\": \"non_existent_sketch\", \"fieldName\": \"non_existent_sketch\", \"k\": 200}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"KllDoublesSketchToQuantiles\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"KllDoublesSketchToHistogram\", \"name\": \"histogram\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
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
        new File(this.getClass().getClassLoader().getResource("kll/kll_doubles_sketch_build_data.tsv").getFile()),
        String.join(
            "\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHH\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"product\"],",
            "      \"dimensionExclusions\": [ \"sequenceNumber\"],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"sequenceNumber\", \"product\", \"value\", \"valueWithNulls\"]",
            "  }",
            "}"
        ),
        "[{\"type\": \"KllDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"k\": 200},"
        + "{\"type\": \"KllDoublesSketch\", \"name\": \"sketchWithNulls\", \"fieldName\": \"valueWithNulls\", \"k\": 200}]",
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        String.join(
            "\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"KllDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"k\": 200},",
            "    {\"type\": \"KllDoublesSketch\", \"name\": \"sketchWithNulls\", \"fieldName\": \"sketchWithNulls\", \"k\": 200},",
            "    {\"type\": \"KllDoublesSketch\", \"name\": \"non_existent_sketch\", \"fieldName\": \"non_existent_sketch\", \"k\": 200}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"KllDoublesSketchToQuantiles\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"KllDoublesSketchToHistogram\", \"name\": \"histogram\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"KllDoublesSketchToQuantiles\", \"name\": \"quantilesWithNulls\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketchWithNulls\"}},",
            "    {\"type\": \"KllDoublesSketchToHistogram\", \"name\": \"histogramWithNulls\", \"splitPoints\": [6.25, 7.5, 8.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketchWithNulls\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
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
    Assert.assertEquals(355, sketchValueWithNulls);

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
    Assert.assertEquals(7.5, quantilesWithNulls[1], 0.07); // median value
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
        new File(this.getClass().getClassLoader().getResource("kll/kll_doubles_sketch_build_data.tsv").getFile()),
        String.join(
            "\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHH\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"sequenceNumber\", \"product\"],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"sequenceNumber\", \"product\", \"value\", \"valueWithNulls\"]",
            "  }",
            "}"
        ),
        "[{\"type\": \"doubleSum\", \"name\": \"value\", \"fieldName\": \"value\"},"
        + "{\"type\": \"doubleSum\", \"name\": \"valueWithNulls\", \"fieldName\": \"valueWithNulls\"}]",
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        String.join(
            "\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"KllDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"k\": 200},",
            "    {\"type\": \"KllDoublesSketch\", \"name\": \"sketchWithNulls\", \"fieldName\": \"valueWithNulls\", \"k\": 200}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"KllDoublesSketchToQuantile\", \"name\": \"quantile\", \"fraction\": 0.5, \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"KllDoublesSketchToQuantiles\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"KllDoublesSketchToHistogram\", \"name\": \"histogram\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"KllDoublesSketchToQuantile\", \"name\": \"quantileWithNulls\", \"fraction\": 0.5, \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketchWithNulls\"}},",
            "    {\"type\": \"KllDoublesSketchToQuantiles\", \"name\": \"quantilesWithNulls\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketchWithNulls\"}},",
            "    {\"type\": \"KllDoublesSketchToHistogram\", \"name\": \"histogramWithNulls\", \"splitPoints\": [6.25, 7.5, 8.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketchWithNulls\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
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
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 400 : 355, sketchValueWithNulls);

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
        NullHandling.replaceWithDefault() ? 7.2 : 7.5,
        (double) quantileObjectWithNulls,
        0.1
    ); // median value

    // post agg with nulls
    Object quantilesObjectWithNulls = row.get(6);
    Assert.assertTrue(quantilesObjectWithNulls instanceof double[]);
    double[] quantilesWithNulls = (double[]) quantilesObjectWithNulls;
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 0.0 : 5.0, quantilesWithNulls[0], 0.05); // min value
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 7.2 : 7.5, quantilesWithNulls[1], 0.1); // median value
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
        new File(this.getClass().getClassLoader().getResource("kll/kll_doubles_sketch_build_data.tsv").getFile()),
        String.join(
            "\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHH\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"sequenceNumber\", \"product\"],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"sequenceNumber\", \"product\", \"value\"]",
            "  }",
            "}"
        ),
        "[{\"type\": \"doubleSum\", \"name\": \"value\", \"fieldName\": \"value\"}]",
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        String.join(
            "\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"KllDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"k\": 200}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"KllDoublesSketchToQuantile\", \"name\": \"quantile\", \"fraction\": 0.5, \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"KllDoublesSketchToQuantiles\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"KllDoublesSketchToHistogram\", \"name\": \"histogram\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
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
    Sequence<ResultRow> seq = timeSeriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("kll/kll_doubles_sketch_build_data.tsv").getFile()),
        String.join(
            "\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHH\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"sequenceNumber\", \"product\"],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"sequenceNumber\", \"product\", \"value\"]",
            "  }",
            "}"
        ),
        "[{\"type\": \"doubleSum\", \"name\": \"value\", \"fieldName\": \"value\"}]",
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        String.join(
            "\n",
            "{",
            "  \"queryType\": \"timeseries\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"aggregations\": [",
            "    {\"type\": \"KllDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"k\": 200}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"KllDoublesSketchToQuantile\", \"name\": \"quantile1\", \"fraction\": 0.5, \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"KllDoublesSketchToQuantiles\", \"name\": \"quantiles1\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"KllDoublesSketchToHistogram\", \"name\": \"histogram1\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
  }

  @Test
  public void testSuccessWhenMaxStreamLengthHit() throws Exception
  {
    if (GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy())) {
      helper.createIndexAndRunQueryOnSegment(
          new File(this.getClass().getClassLoader().getResource("kll/kll_doubles_sketch_build_data.tsv").getFile()),
          String.join(
              "\n",
              "{",
              "  \"type\": \"string\",",
              "  \"parseSpec\": {",
              "    \"format\": \"tsv\",",
              "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHH\"},",
              "    \"dimensionsSpec\": {",
              "      \"dimensions\": [\"sequenceNumber\", \"product\"],",
              "      \"dimensionExclusions\": [],",
              "      \"spatialDimensions\": []",
              "    },",
              "    \"columns\": [\"timestamp\", \"sequenceNumber\", \"product\", \"value\"]",
              "  }",
              "}"
          ),
          "[{\"type\": \"doubleSum\", \"name\": \"value\", \"fieldName\": \"value\"}]",
          0, // minTimestamp
          Granularities.NONE,
          10, // maxRowCount
          String.join(
              "\n",
              "{",
              "  \"queryType\": \"groupBy\",",
              "  \"dataSource\": \"test_datasource\",",
              "  \"granularity\": \"ALL\",",
              "  \"dimensions\": [],",
              "  \"aggregations\": [",
              "    {\"type\": \"KllDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"k\": 200, \"maxStreamLength\": 10}",
              "  ],",
              "  \"postAggregations\": [",
              "    {\"type\": \"KllDoublesSketchToQuantile\", \"name\": \"quantile\", \"fraction\": 0.5, \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
              "    {\"type\": \"KllDoublesSketchToQuantiles\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
              "    {\"type\": \"KllDoublesSketchToHistogram\", \"name\": \"histogram\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
              "  ],",
              "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
              "}"
          )
      );
    } else {
      Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
          new File(this.getClass().getClassLoader().getResource("kll/kll_doubles_sketch_build_data.tsv").getFile()),
          String.join(
              "\n",
              "{",
              "  \"type\": \"string\",",
              "  \"parseSpec\": {",
              "    \"format\": \"tsv\",",
              "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHH\"},",
              "    \"dimensionsSpec\": {",
              "      \"dimensions\": [\"sequenceNumber\", \"product\"],",
              "      \"dimensionExclusions\": [],",
              "      \"spatialDimensions\": []",
              "    },",
              "    \"columns\": [\"timestamp\", \"sequenceNumber\", \"product\", \"value\"]",
              "  }",
              "}"
          ),
          "[{\"type\": \"doubleSum\", \"name\": \"value\", \"fieldName\": \"value\"}]",
          0, // minTimestamp
          Granularities.NONE,
          10, // maxRowCount
          String.join(
              "\n",
              "{",
              "  \"queryType\": \"groupBy\",",
              "  \"dataSource\": \"test_datasource\",",
              "  \"granularity\": \"ALL\",",
              "  \"dimensions\": [],",
              "  \"aggregations\": [",
              "    {\"type\": \"KllDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"k\": 200, \"maxStreamLength\": 10}",
              "  ],",
              "  \"postAggregations\": [",
              "    {\"type\": \"KllDoublesSketchToQuantile\", \"name\": \"quantile\", \"fraction\": 0.5, \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
              "    {\"type\": \"KllDoublesSketchToQuantiles\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
              "    {\"type\": \"KllDoublesSketchToHistogram\", \"name\": \"histogram\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
              "  ],",
              "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
              "}"
          )
      );
      seq.toList();
    }
  }

}
