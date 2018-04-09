/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.druid.data.input.Row;
import io.druid.initialization.DruidModule;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class DoublesSketchAggregatorTest
{

  private final AggregationTestHelper helper;
  private final AggregationTestHelper timeSeriesHelper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public DoublesSketchAggregatorTest(final GroupByQueryConfig config)
  {
    DruidModule module = new DoublesSketchModule();
    module.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(), config, tempFolder);
    timeSeriesHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(
        module.getJacksonModules(),
        tempFolder
    );
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = Lists.newArrayList();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
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
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/doubles_sketch_data.tsv").getFile()),
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
            "  {\"type\": \"quantilesDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"k\": 128},",
            "  {\"type\": \"quantilesDoublesSketch\", \"name\": \"non_existent_sketch\", \"fieldName\": \"non_existent_sketch\", \"k\": 128}",
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
            "    {\"type\": \"quantilesDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"k\": 128},",
            "    {\"type\": \"quantilesDoublesSketch\", \"name\": \"non_existent_sketch\", \"fieldName\": \"non_existent_sketch\", \"k\": 128}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"quantilesDoublesSketchToQuantiles\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"quantilesDoublesSketchToHistogram\", \"name\": \"histogram\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<Row> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Row row = results.get(0);

    Object nonExistentSketchObject = row.getRaw("non_existent_sketch");
    Assert.assertTrue(nonExistentSketchObject instanceof Long);
    long nonExistentSketchValue = (long) nonExistentSketchObject;
    Assert.assertEquals(0, nonExistentSketchValue);

    Object sketchObject = row.getRaw("sketch");
    Assert.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assert.assertEquals(400, sketchValue);

    // post agg
    Object quantilesObject = row.getRaw("quantiles");
    Assert.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assert.assertEquals(0, quantiles[0], 0.05); // min value
    Assert.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assert.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.getRaw("histogram");
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
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/doubles_build_data.tsv").getFile()),
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
            "    \"columns\": [\"timestamp\", \"sequenceNumber\", \"product\", \"value\"]",
            "  }",
            "}"
        ),
        "[{\"type\": \"quantilesDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"k\": 128}]",
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
            "    {\"type\": \"quantilesDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"k\": 128},",
            "    {\"type\": \"quantilesDoublesSketch\", \"name\": \"non_existent_sketch\", \"fieldName\": \"non_existent_sketch\", \"k\": 128}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"quantilesDoublesSketchToQuantiles\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"quantilesDoublesSketchToHistogram\", \"name\": \"histogram\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<Row> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Row row = results.get(0);

    Object sketchObject = row.getRaw("sketch");
    Assert.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assert.assertEquals(400, sketchValue);

    // post agg
    Object quantilesObject = row.getRaw("quantiles");
    Assert.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assert.assertEquals(0, quantiles[0], 0.05); // min value
    Assert.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assert.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.getRaw("histogram");
    Assert.assertTrue(histogramObject instanceof double[]);
    double[] histogram = (double[]) histogramObject;
    Assert.assertEquals(4, histogram.length);
    for (final double bin : histogram) {
      Assert.assertEquals(100, bin, 100 * 0.2); // 400 items uniformly
      // distributed into 4 bins
    }
  }

  @Test
  public void buildingSketchesAtQueryTime() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/doubles_build_data.tsv").getFile()),
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
            "    {\"type\": \"quantilesDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"k\": 128}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"quantilesDoublesSketchToQuantile\", \"name\": \"quantile\", \"fraction\": 0.5, \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"quantilesDoublesSketchToQuantiles\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"quantilesDoublesSketchToHistogram\", \"name\": \"histogram\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<Row> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Row row = results.get(0);

    Object sketchObject = row.getRaw("sketch");
    Assert.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assert.assertEquals(400, sketchValue);

    // post agg
    Object quantileObject = row.getRaw("quantile");
    Assert.assertTrue(quantileObject instanceof Double);
    Assert.assertEquals(0.5, (double) quantileObject, 0.05); // median value

    // post agg
    Object quantilesObject = row.getRaw("quantiles");
    Assert.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assert.assertEquals(0, quantiles[0], 0.05); // min value
    Assert.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assert.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.getRaw("histogram");
    Assert.assertTrue(histogramObject instanceof double[]);
    double[] histogram = (double[]) histogramObject;
    for (final double bin : histogram) {
      Assert.assertEquals(100, bin, 100 * 0.2); // 400 items uniformly
      // distributed into 4 bins
    }
  }

  @Test
  public void QueryingDataWithFieldNameValueAsFloatInsteadOfSketch() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/doubles_build_data.tsv").getFile()),
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
            "    {\"type\": \"quantilesDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"k\": 128}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"quantilesDoublesSketchToQuantile\", \"name\": \"quantile\", \"fraction\": 0.5, \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"quantilesDoublesSketchToQuantiles\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"quantilesDoublesSketchToHistogram\", \"name\": \"histogram\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<Row> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Row row = results.get(0);

    Object sketchObject = row.getRaw("sketch");
    Assert.assertTrue(sketchObject instanceof Long);
    long sketchValue = (long) sketchObject;
    Assert.assertEquals(400, sketchValue);

    // post agg
    Object quantileObject = row.getRaw("quantile");
    Assert.assertTrue(quantileObject instanceof Double);
    Assert.assertEquals(0.5, (double) quantileObject, 0.05); // median value

    // post agg
    Object quantilesObject = row.getRaw("quantiles");
    Assert.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assert.assertEquals(0, quantiles[0], 0.05); // min value
    Assert.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assert.assertEquals(1, quantiles[2], 0.05); // max value

    // post agg
    Object histogramObject = row.getRaw("histogram");
    Assert.assertTrue(histogramObject instanceof double[]);
    double[] histogram = (double[]) histogramObject;
    for (final double bin : histogram) {
      Assert.assertEquals(100, bin, 100 * 0.2); // 400 items uniformly
      // distributed into 4 bins
    }
  }

  @Test
  public void TimeSeriesQueryInputAsFloat() throws Exception
  {
    Sequence<Row> seq = timeSeriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("quantiles/doubles_build_data.tsv").getFile()),
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
            "    {\"type\": \"quantilesDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"k\": 128}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"quantilesDoublesSketchToQuantile\", \"name\": \"quantile1\", \"fraction\": 0.5, \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"quantilesDoublesSketchToQuantiles\", \"name\": \"quantiles1\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"quantilesDoublesSketchToHistogram\", \"name\": \"histogram1\", \"splitPoints\": [0.25, 0.5, 0.75], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<Row> results = seq.toList();
    Assert.assertEquals(1, results.size());
  }
}
