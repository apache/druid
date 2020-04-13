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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class TDigestSketchAggregatorTest extends InitializedNullHandlingTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public TDigestSketchAggregatorTest(final GroupByQueryConfig config)
  {
    TDigestSketchModule.registerSerde();
    TDigestSketchModule module = new TDigestSketchModule();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(), config, tempFolder);
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

  // this is to test Json properties and equals
  @Test
  public void serializeDeserializeFactoryWithFieldName() throws Exception
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    new TDigestSketchModule().getJacksonModules().forEach(objectMapper::registerModule);
    TDigestSketchAggregatorFactory factory = new TDigestSketchAggregatorFactory("name", "filedName", 128);

    AggregatorFactory other = objectMapper.readValue(
        objectMapper.writeValueAsString(factory),
        AggregatorFactory.class
    );

    Assert.assertEquals(factory, other);
  }

  @Test
  public void buildingSketchesAtIngestionTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("doubles_build_data.tsv").getFile()),
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
        "[{\"type\": \"tDigestSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"compression\": 200}]",
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
            "    {\"type\": \"tDigestSketch\", \"name\": \"merged_sketch\", \"fieldName\": \"sketch\", "
            + "\"compression\": "
            + "200}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"quantilesFromTDigestSketch\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], "
            + "\"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"merged_sketch\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    // post agg
    Object quantilesObject = row.get(1); // "quantiles"
    Assert.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assert.assertEquals(0.001, quantiles[0], 0.0006); // min value
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 0.47 : 0.5, quantiles[1], 0.05); // median value
    Assert.assertEquals(1, quantiles[2], 0.05); // max value
  }

  @Test
  public void buildingSketchesAtQueryTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("doubles_build_data.tsv").getFile()),
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
            "    {\"type\": \"tDigestSketch\", \"name\": \"sketch\", \"fieldName\": \"value\", \"compression\": 200}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"quantilesFromTDigestSketch\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);


    // post agg
    Object quantilesObject = row.get(1); // "quantiles"
    Assert.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 0.0 : 0.001, quantiles[0], 0.0006); // min value
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 0.35 : 0.5, quantiles[1], 0.05); // median value
    Assert.assertEquals(1, quantiles[2], 0.05); // max value
  }

  @Test
  public void testIngestingSketches() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("doubles_sketch_data.tsv").getFile()),
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
            "  {\"type\": \"tDigestSketch\", \"name\": \"first_level_merge_sketch\", \"fieldName\": \"sketch\", "
            + "\"compression\": "
            + "200}",
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
            "    {\"type\": \"tDigestSketch\", \"name\": \"second_level_merge_sketch\", \"fieldName\": "
            + "\"first_level_merge_sketch\", \"compression\": "
            + "200}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"quantilesFromTDigestSketch\", \"name\": \"quantiles\", \"fractions\": [0, 0.5, 1], \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"second_level_merge_sketch\"}}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    // post agg
    Object quantilesObject = row.get(1); // "quantiles"
    Assert.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assert.assertEquals(0.001, quantiles[0], 0.0006); // min value
    Assert.assertEquals(NullHandling.replaceWithDefault() ? 0.47 : 0.5, quantiles[1], 0.05); // median value
    Assert.assertEquals(1, quantiles[2], 0.05); // max value
  }
}
