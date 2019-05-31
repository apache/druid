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

package org.apache.druid.query.aggregation.datasketches.hll;

import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
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
public class HllSketchAggregatorTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public HllSketchAggregatorTest(GroupByQueryConfig config)
  {
    HllSketchModule.registerSerde();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        new HllSketchModule().getJacksonModules(), config, tempFolder);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[] {config});
    }
    return constructors;
  }

  @Test
  public void ingestSketches() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile()),
        String.join("\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMdd\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"dim\", \"multiDim\"],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"dim\", \"multiDim\", \"sketch\"],",
            "    \"listDelimiter\": \",\"",
            "  }",
            "}"),
        String.join("\n",
            "[",
            "  {\"type\": \"HLLSketchMerge\", \"name\": \"sketch\", \"fieldName\": \"sketch\"}",
            "]"),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"HLLSketchMerge\", \"name\": \"sketch\", \"fieldName\": \"sketch\"}",
            "  ],",
            "  \"intervals\": [\"2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z\"]",
            "}"));
    List<Row> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Row row = results.get(0);
    Assert.assertEquals(200, (double) row.getMetric("sketch"), 0.1);
  }

  @Test
  public void buildSketchesAtIngestionTime() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        String.join("\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMdd\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"dim\"],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"dim\", \"multiDim\", \"id\"],",
                    "    \"listDelimiter\": \",\"",
            "  }",
            "}"),
        String.join("\n",
            "[",
            "  {\"type\": \"HLLSketchBuild\", \"name\": \"sketch\", \"fieldName\": \"id\"}",
            "]"),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"HLLSketchMerge\", \"name\": \"sketch\", \"fieldName\": \"sketch\"}",
            "  ],",
            "  \"intervals\": [\"2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z\"]",
            "}"));
    List<Row> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Row row = results.get(0);
    Assert.assertEquals(200, (double) row.getMetric("sketch"), 0.1);
  }

  @Test
  public void buildSketchesAtQueryTime() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        String.join("\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMdd\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"dim\", \"multiDim\", \"id\"],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"dim\", \"multiDim\", \"id\"],",
            "    \"listDelimiter\": \",\"",
            "  }",
            "}"),
        "[]",
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"HLLSketchBuild\", \"name\": \"sketch\", \"fieldName\": \"id\"}",
            "  ],",
            "  \"intervals\": [\"2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z\"]",
            "}"));
    List<Row> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Row row = results.get(0);
    Assert.assertEquals(200, (double) row.getMetric("sketch"), 0.1);
  }

  @Test
  public void buildSketchesAtQueryTimeMultiValue() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        String.join("\n",
                    "{",
                    "  \"type\": \"string\",",
                    "  \"parseSpec\": {",
                    "    \"format\": \"tsv\",",
                    "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMdd\"},",
                    "    \"dimensionsSpec\": {",
                    "      \"dimensions\": [\"dim\", \"multiDim\", \"id\"],",
                    "      \"dimensionExclusions\": [],",
                    "      \"spatialDimensions\": []",
                    "    },",
                    "    \"columns\": [\"timestamp\", \"dim\", \"multiDim\", \"id\"],",
                    "    \"listDelimiter\": \",\"",
                    "  }",
                    "}"),
        "[]",
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        String.join("\n",
                    "{",
                    "  \"queryType\": \"groupBy\",",
                    "  \"dataSource\": \"test_datasource\",",
                    "  \"granularity\": \"ALL\",",
                    "  \"dimensions\": [],",
                    "  \"aggregations\": [",
                    "    {\"type\": \"HLLSketchBuild\", \"name\": \"sketch\", \"fieldName\": \"multiDim\"}",
                    "  ],",
                    "  \"intervals\": [\"2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z\"]",
                    "}"));
    List<Row> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Row row = results.get(0);
    Assert.assertEquals(14, (double) row.getMetric("sketch"), 0.1);
  }
}
