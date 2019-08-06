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

import com.yahoo.sketches.quantiles.DoublesSketch;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
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
public class ArrayOfDoublesSketchAggregationTest
{

  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public ArrayOfDoublesSketchAggregationTest(final GroupByQueryConfig config)
  {
    DruidModule module = new ArrayOfDoublesSketchModule();
    module.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(), config, tempFolder);
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
        String.join("\n",
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
            "}"),
        String.join("\n",
            "[",
            "  {\"type\": \"arrayOfDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"nominalEntries\": 1024},",
            "  {\"type\": \"arrayOfDoublesSketch\", \"name\": \"non_existing_sketch\", \"fieldName\": \"non_existing_sketch\"}",
            "]"),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"arrayOfDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"nominalEntries\": 1024},",
            "    {\"type\": \"arrayOfDoublesSketch\", \"name\": \"non_existing_sketch\", \"fieldName\": \"non_existing_sketch\"}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"estimate\", \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"arrayOfDoublesSketchToQuantilesSketch\", \"name\": \"quantiles-sketch\", \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"union\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"union\",",
            "      \"operation\": \"UNION\",",
            "      \"nominalEntries\": 1024,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"intersection\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"intersection\",",
            "      \"operation\": \"INTERSECT\",",
            "      \"nominalEntries\": 1024,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"anotb\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"anotb\",",
            "      \"operation\": \"NOT\",",
            "      \"nominalEntries\": 1024,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }}",
            "  ],",
            "  \"intervals\": [\"2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z\"]",
            "}"));
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("non_existing_sketch", 0, (double) row.get(1), 0);
    Assert.assertEquals("sketch", 40.0, (double) row.get(0), 0);
    Assert.assertEquals("estimate", 40.0, (double) row.get(2), 0);
    Assert.assertEquals("union", 40.0, (double) row.get(4), 0);
    Assert.assertEquals("intersection", 40.0, (double) row.get(5), 0);
    Assert.assertEquals("anotb", 0, (double) row.get(6), 0);

    Object obj = row.get(3); // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(40, ds.getN());
    Assert.assertEquals(1.0, ds.getMinValue(), 0);
    Assert.assertEquals(1.0, ds.getMaxValue(), 0);
  }

  @Test
  public void ingestingSketchesTwoValues() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/array_of_doubles_sketch_data_two_values.tsv")
            .getFile()),
        String.join("\n",
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
            "}"),
        String.join("\n",
            "[",
            "  {\"type\": \"arrayOfDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"nominalEntries\": 1024, \"numberOfValues\": 2}",
            "]"),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"arrayOfDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"nominalEntries\": 1024, \"numberOfValues\": 2}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"estimate\", \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"arrayOfDoublesSketchToQuantilesSketch\", \"name\": \"quantiles-sketch\", \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"union\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"union\",",
            "      \"operation\": \"UNION\",",
            "      \"nominalEntries\": 1024,",
            "      \"numberOfValues\": 2,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"intersection\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"intersection\",",
            "      \"operation\": \"INTERSECT\",",
            "      \"nominalEntries\": 1024,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"anotb\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"anotb\",",
            "      \"operation\": \"NOT\",",
            "      \"nominalEntries\": 1024,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {",
            "      \"type\": \"arrayOfDoublesSketchToMeans\",",
            "      \"name\": \"means\",",
            "      \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}",
            "    }",
            "  ],",
            "  \"intervals\": [\"2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z\"]",
            "}"));
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
    Assert.assertEquals(1.0, ds.getMinValue(), 0);
    Assert.assertEquals(1.0, ds.getMaxValue(), 0);
  }

  @Test
  public void buildingSketchesAtIngestionTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/array_of_doubles_build_data.tsv").getFile()),
        String.join("\n",
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
            "    \"columns\": [\"timestamp\", \"product\", \"key\", \"value\"]",
            "  }",
            "}"),
        String.join("\n",
            "[",
            "  {\"type\": \"arrayOfDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"key\", \"metricColumns\": [\"value\"], \"nominalEntries\": 1024}",
            "]"),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"arrayOfDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"size\": 1024}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"estimate\", \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"arrayOfDoublesSketchToQuantilesSketch\", \"name\": \"quantiles-sketch\", \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"union\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"union\",",
            "      \"operation\": \"UNION\",",
            "      \"nominalEntries\": 1024,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"intersection\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"intersection\",",
            "      \"operation\": \"INTERSECT\",",
            "      \"nominalEntries\": 1024,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"anotb\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"anotb\",",
            "      \"operation\": \"NOT\",",
            "      \"nominalEntries\": 1024,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }}",
            "  ],",
            "  \"intervals\": [\"2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z\"]",
            "}"));
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
    Assert.assertEquals(1.0, ds.getMinValue(), 0);
    Assert.assertEquals(1.0, ds.getMaxValue(), 0);
  }

  @Test
  public void buildingSketchesAtIngestionTimeTwoValues() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(
            this.getClass().getClassLoader().getResource("tuple/array_of_doubles_build_data_two_values.tsv").getFile()),
        String.join("\n",
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
            "    \"columns\": [\"timestamp\", \"product\", \"key\", \"value1\", \"value2\"]",
            "  }",
            "}"),
        String.join("\n",
            "[",
            "  {\"type\": \"arrayOfDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"key\", \"metricColumns\": [ \"value1\", \"value2\" ], \"nominalEntries\": 1024}",
            "]"),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"arrayOfDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"nominalEntries\": 1024, \"numberOfValues\": 2}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"estimate\", \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"arrayOfDoublesSketchToQuantilesSketch\", \"name\": \"quantiles-sketch\", \"column\": 2, \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"union\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"union\",",
            "      \"operation\": \"UNION\",",
            "      \"nominalEntries\": 1024,",
            "      \"numberOfValues\": 2,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"intersection\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"intersection\",",
            "      \"operation\": \"INTERSECT\",",
            "      \"nominalEntries\": 1024,",
            "      \"numberOfValues\": 2,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"anotb\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"anotb\",",
            "      \"operation\": \"NOT\",",
            "      \"nominalEntries\": 1024,",
            "      \"numberOfValues\": 2,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {",
            "      \"type\": \"arrayOfDoublesSketchToMeans\",",
            "      \"name\": \"means\",",
            "      \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}",
            "    }",
            "  ],",
            "  \"intervals\": [\"2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z\"]",
            "}"));
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
    Assert.assertEquals(2.0, ds.getMinValue(), 0);
    Assert.assertEquals(2.0, ds.getMaxValue(), 0);
  }

  @Test
  public void buildingSketchesAtQueryTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/array_of_doubles_build_data.tsv").getFile()),
        String.join("\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHH\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"product\", \"key\"],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"product\", \"key\", \"value\"]",
            "  }",
            "}"),
        String.join("\n",
            "[",
            "  {\"type\": \"doubleSum\", \"name\": \"value\", \"fieldName\": \"value\"}",
            "]"),
        0, // minTimestamp
        Granularities.NONE,
        40, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"arrayOfDoublesSketch\", \"name\": \"sketch\", \"fieldName\": \"key\", \"metricColumns\": [\"value\"], \"nominalEntries\": 1024},",
            "    {\"type\": \"count\", \"name\":\"cnt\"}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"estimate\", \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"arrayOfDoublesSketchToQuantilesSketch\", \"name\": \"quantiles-sketch\", \"field\": {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"union\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"union\",",
            "      \"operation\": \"UNION\",",
            "      \"nominalEntries\": 1024,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"intersection\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"intersection\",",
            "      \"operation\": \"INTERSECT\",",
            "      \"nominalEntries\": 1024,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }},",
            "    {\"type\": \"arrayOfDoublesSketchToEstimate\", \"name\": \"anotb\", \"field\": {",
            "      \"type\": \"arrayOfDoublesSketchSetOp\",",
            "      \"name\": \"anotb\",",
            "      \"operation\": \"NOT\",",
            "      \"nominalEntries\": 1024,",
            "      \"fields\": [{\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}, {\"type\": \"fieldAccess\", \"fieldName\": \"sketch\"}]",
            "    }}",
            "  ],",
            "  \"intervals\": [\"2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z\"]",
            "}"));
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("cnt", 40.0, new Double(row.get(1).toString()), 0);
    Assert.assertEquals("sketch", 40.0, (double) row.get(0), 0);
    Assert.assertEquals("estimate", 40.0, new Double(row.get(2).toString()), 0);
    Assert.assertEquals("union", 40.0, new Double(row.get(4).toString()), 0);
    Assert.assertEquals("intersection", 40.0, new Double(row.get(5).toString()), 0);
    Assert.assertEquals("anotb", 0, new Double(row.get(6).toString()), 0);

    Object obj = row.get(3); // quantiles-sketch
    Assert.assertTrue(obj instanceof DoublesSketch);
    DoublesSketch ds = (DoublesSketch) obj;
    Assert.assertEquals(40, ds.getN());
    Assert.assertEquals(1.0, ds.getMinValue(), 0);
    Assert.assertEquals(1.0, ds.getMaxValue(), 0);
  }

  // Two buckets with statistically significant difference.
  // See GenerateTestData class for details.
  @Test
  public void buildingSketchesAtQueryTimeAndTTest() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("tuple/bucket_test_data.tsv").getFile()),
        String.join("\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMdd\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"label\", \"userid\"]",
            "    },",
            "    \"columns\": [\"timestamp\", \"label\", \"userid\", \"parameter\"]",
            "  }",
            "}"),
        String.join("\n",
            "[",
            "  {\"type\": \"doubleSum\", \"name\": \"parameter\", \"fieldName\": \"parameter\"}",
            "]"),
        0, // minTimestamp
        Granularities.NONE,
        2000, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {",
            "      \"type\": \"filtered\",",
            "      \"filter\": {\"type\": \"selector\", \"dimension\": \"label\", \"value\": \"test\"},",
            "      \"aggregator\": {\"type\": \"arrayOfDoublesSketch\", \"name\": \"sketch-test\", \"fieldName\": \"userid\", \"metricColumns\": [\"parameter\"]}",
            "    },",
            "    {",
            "      \"type\": \"filtered\",",
            "      \"filter\": {\"type\": \"selector\", \"dimension\": \"label\", \"value\": \"control\"},",
            "      \"aggregator\": {\"type\": \"arrayOfDoublesSketch\", \"name\": \"sketch-control\", \"fieldName\": \"userid\", \"metricColumns\": [\"parameter\"]}",
            "    }",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"arrayOfDoublesSketchTTest\",",
            "      \"name\": \"p-value\", \"fields\": [",
            "        {\"type\": \"fieldAccess\", \"fieldName\": \"sketch-test\"},",
            "        {\"type\": \"fieldAccess\", \"fieldName\": \"sketch-control\"}",
            "      ]",
            "    }",
            "  ],",
            "  \"intervals\": [\"2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z\"]",
            "}"));
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

}
