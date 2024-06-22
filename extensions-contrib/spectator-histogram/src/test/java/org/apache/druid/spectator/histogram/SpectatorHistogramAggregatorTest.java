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

package org.apache.druid.spectator.histogram;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spectator.api.histogram.PercentileBuckets;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class SpectatorHistogramAggregatorTest extends InitializedNullHandlingTest
{
  public static final String INPUT_DATA_PARSE_SPEC = String.join(
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
      "    \"columns\": [\"timestamp\", \"product\", \"cost\"]",
      "  }",
      "}"
  );
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private static final SegmentMetadataQueryRunnerFactory METADATA_QR_FACTORY = new SegmentMetadataQueryRunnerFactory(
      new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );
  private static final Map<String, SpectatorHistogram> EXPECTED_HISTOGRAMS = new HashMap<>();

  static {
    SpectatorHistogram histogram = new SpectatorHistogram();
    histogram.add(PercentileBuckets.indexOf(10), 1L);
    EXPECTED_HISTOGRAMS.put("A", histogram);

    histogram = new SpectatorHistogram();
    histogram.add(PercentileBuckets.indexOf(30 + 40 + 40 + 40 + 50 + 50), 1L);
    EXPECTED_HISTOGRAMS.put("B", histogram);

    histogram = new SpectatorHistogram();
    histogram.add(PercentileBuckets.indexOf(50 + 20000), 1L);
    EXPECTED_HISTOGRAMS.put("C", histogram);
  }

  private final AggregationTestHelper helper;
  private final AggregationTestHelper timeSeriesHelper;

  public SpectatorHistogramAggregatorTest(final GroupByQueryConfig config)
  {
    SpectatorHistogramModule.registerSerde();
    SpectatorHistogramModule module = new SpectatorHistogramModule();
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
    new SpectatorHistogramModule().getJacksonModules().forEach(objectMapper::registerModule);
    SpectatorHistogramAggregatorFactory factory = new SpectatorHistogramAggregatorFactory(
        "name",
        "filedName",
        AggregatorUtil.SPECTATOR_HISTOGRAM_CACHE_TYPE_ID
    );
    AggregatorFactory other = objectMapper.readValue(
        objectMapper.writeValueAsString(factory),
        AggregatorFactory.class
    );

    Assert.assertEquals(factory, other);
  }

  @Test
  public void testBuildingHistogramQueryTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"longSum\", \"name\": \"cost_sum\", \"fieldName\": \"cost\"}",
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
            "  \"dimensions\": [\"product\"],",
            "  \"aggregations\": [",
            "    {\"type\": \"spectatorHistogram\", \"name\": \"cost_histogram\", \"fieldName\": "
            + "\"cost_sum\"}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<ResultRow> results = seq.toList();
    assertResultsMatch(results, 0, "A");
    assertResultsMatch(results, 1, "B");
    assertResultsMatch(results, 2, "C");
  }

  @Test
  public void testBuildingAndMergingHistograms() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"spectatorHistogram\", \"name\": \"histogram\", \"fieldName\": \"cost\"}",
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
            "  \"dimenions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"spectatorHistogram\", \"name\": \"merged_cost_histogram\", \"fieldName\": "
            + "\"histogram\"}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(expected, results.get(0).get(0));
  }

  @Test
  public void testBuildingAndMergingHistogramsTimeseriesQuery() throws Exception
  {
    Object rawseq = timeSeriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"spectatorHistogram\", \"name\": \"histogram\", \"fieldName\": \"cost\"}",
            "]"
        ),
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
            "    {\"type\": \"spectatorHistogram\", \"name\": \"merged_cost_histogram\", \"fieldName\": "
            + "\"histogram\"}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    Sequence<Result<TimeseriesResultValue>> seq = (Sequence<Result<TimeseriesResultValue>>) rawseq;
    List<Result<TimeseriesResultValue>> results = seq.toList();
    Assert.assertEquals(1, results.size());
    SpectatorHistogram value = (SpectatorHistogram) results.get(0).getValue().getMetric("merged_cost_histogram");
    Assert.assertEquals(expected, value);
  }

  @Test
  public void testBuildingAndMergingGroupbyHistograms() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"spectatorHistogram\", \"name\": \"histogram\", \"fieldName\": \"cost\"}",
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
            "  \"dimensions\": [\"product\"],",
            "  \"aggregations\": [",
            "    {\"type\": \"spectatorHistogram\", \"name\": \"merged_histogram\", \"fieldName\": "
            + "\"histogram\"}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(6, results.size());

    SpectatorHistogram expectedA = new SpectatorHistogram();
    expectedA.add(PercentileBuckets.indexOf(10), 1L);
    Assert.assertEquals(expectedA, results.get(0).get(1));

    SpectatorHistogram expectedB = new SpectatorHistogram();
    expectedB.add(PercentileBuckets.indexOf(30), 1L);
    expectedB.add(PercentileBuckets.indexOf(40), 3L);
    expectedB.add(PercentileBuckets.indexOf(50), 2L);
    Assert.assertEquals(expectedB, results.get(1).get(1));

    SpectatorHistogram expectedC = new SpectatorHistogram();
    expectedC.add(PercentileBuckets.indexOf(50), 1L);
    expectedC.add(PercentileBuckets.indexOf(20000), 1L);
    Assert.assertEquals(expectedC, results.get(2).get(1));

    Assert.assertNull(results.get(3).get(1));
    Assert.assertNull(results.get(4).get(1));
    Assert.assertNull(results.get(5).get(1));
  }

  @Test
  public void testBuildingAndCountingHistograms() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"spectatorHistogram\", \"name\": \"histogram\", \"fieldName\": \"cost\"}",
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
            "  \"dimenions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"longSum\", \"name\": \"count_histogram\", \"fieldName\": "
            + "\"histogram\"},",
            "    {\"type\": \"doubleSum\", \"name\": \"double_count_histogram\", \"fieldName\": "
            + "\"histogram\"}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    // Check longSum
    Assert.assertEquals(9L, results.get(0).get(0));
    // Check doubleSum
    Assert.assertEquals(9.0, (Double) results.get(0).get(1), 0.001);
  }

  @Test
  public void testBuildingAndCountingHistogramsWithNullFilter() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"spectatorHistogram\", \"name\": \"histogram\", \"fieldName\": \"cost\"}",
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
            "  \"dimenions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"longSum\", \"name\": \"count_histogram\", \"fieldName\": "
            + "\"histogram\"},",
            "    {\"type\": \"doubleSum\", \"name\": \"double_count_histogram\", \"fieldName\": "
            + "\"histogram\"}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"],",
            "  \"filter\": {\n",
            "    \"fields\": [\n",
            "      {\n",
            "        \"field\": {\n",
            "          \"dimension\": \"histogram\",\n",
            "          \"value\": \"0\",\n",
            "          \"type\": \"selector\"\n",
            "        },\n",
            "        \"type\": \"not\"\n",
            "      },\n",
            "      {\n",
            "        \"field\": {\n",
            "          \"dimension\": \"histogram\",\n",
            "          \"value\": \"\",\n",
            "          \"type\": \"selector\"\n",
            "        },\n",
            "        \"type\": \"not\"\n",
            "      }\n",
            "    ],\n",
            "    \"type\": \"and\"\n",
            "  }",
            "}"
        )
    );

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    // Check longSum
    Assert.assertEquals(9L, results.get(0).get(0));
    // Check doubleSum
    Assert.assertEquals(9.0, (Double) results.get(0).get(1), 0.001);
  }

  @Test
  public void testIngestAsHistogramDistribution() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"spectatorHistogramDistribution\", \"name\": \"histogram\", \"fieldName\": \"cost\"}",
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
            "  \"dimenions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"spectatorHistogram\", \"name\": \"merged_cost_histogram\", \"fieldName\": "
            + "\"histogram\"}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(expected, results.get(0).get(0));
  }

  @Test
  public void testIngestHistogramsTimer() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"spectatorHistogramTimer\", \"name\": \"histogram\", \"fieldName\": \"cost\"}",
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
            "  \"dimenions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"spectatorHistogram\", \"name\": \"merged_cost_histogram\", \"fieldName\": "
            + "\"histogram\"}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(expected, results.get(0).get(0));
  }

  @Test
  public void testIngestingPreaggregatedHistograms() throws Exception
  {
    Object rawseq = timeSeriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("pre_agg_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"spectatorHistogram\", \"name\": \"histogram\", \"fieldName\": \"cost\"}",
            "]"
        ),
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
            "    {\"type\": \"spectatorHistogram\", \"name\": \"merged_cost_histogram\", \"fieldName\": "
            + "\"histogram\"}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    Sequence<Result<TimeseriesResultValue>> seq = (Sequence<Result<TimeseriesResultValue>>) rawseq;
    List<Result<TimeseriesResultValue>> results = seq.toList();
    Assert.assertEquals(1, results.size());
    SpectatorHistogram value = (SpectatorHistogram) results.get(0).getValue().getMetric("merged_cost_histogram");
    Assert.assertEquals(expected, value);
  }

  @Test
  public void testMetadataQueryTimer() throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    helper.createIndex(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"spectatorHistogramTimer\", \"name\": \"histogram\", \"fieldName\": \"cost\"}",
            "]"
        ),
        segmentDir,
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        true
    );

    ObjectMapper mapper = (ObjectMapper) TestHelper.makeJsonMapper();
    SpectatorHistogramModule module = new SpectatorHistogramModule();
    module.getJacksonModules().forEach(mod -> mapper.registerModule(mod));
    IndexIO indexIO = new IndexIO(
        mapper,
        new ColumnConfig() {}
    );

    QueryableIndex index = indexIO.loadIndex(segmentDir);

    SegmentId segmentId = SegmentId.dummy("segmentId");
    QueryRunner runner = QueryRunnerTestHelper.makeQueryRunner(
        METADATA_QR_FACTORY,
        segmentId,
        new QueryableIndexSegment(index, segmentId),
        null
    );

    SegmentMetadataQuery segmentMetadataQuery = Druids.newSegmentMetadataQueryBuilder()
                                                      .dataSource("test_datasource")
                                                      .intervals("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                                                      .merge(true)
                                                      .build();
    List<SegmentAnalysis> results = runner.run(QueryPlus.wrap(segmentMetadataQuery)).toList();
    System.out.println(results);
    Assert.assertEquals(1, results.size());
    Map<String, ColumnAnalysis> columns = results.get(0).getColumns();
    Assert.assertNotNull(columns.get("histogram"));
    Assert.assertEquals("spectatorHistogramTimer", columns.get("histogram").getType());
  }

  @Test
  public void testMetadataQueryDistribution() throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    helper.createIndex(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"spectatorHistogramDistribution\", \"name\": \"histogram\", \"fieldName\": \"cost\"}",
            "]"
        ),
        segmentDir,
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        true
    );

    ObjectMapper mapper = (ObjectMapper) TestHelper.makeJsonMapper();
    SpectatorHistogramModule module = new SpectatorHistogramModule();
    module.getJacksonModules().forEach(mod -> mapper.registerModule(mod));
    IndexIO indexIO = new IndexIO(
        mapper,
        new ColumnConfig() { }
    );

    QueryableIndex index = indexIO.loadIndex(segmentDir);

    SegmentId segmentId = SegmentId.dummy("segmentId");
    QueryRunner runner = QueryRunnerTestHelper.makeQueryRunner(
        METADATA_QR_FACTORY,
        segmentId,
        new QueryableIndexSegment(index, segmentId),
        null
    );

    SegmentMetadataQuery segmentMetadataQuery = Druids.newSegmentMetadataQueryBuilder()
                                                      .dataSource("test_datasource")
                                                      .intervals("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                                                      .merge(true)
                                                      .build();
    List<SegmentAnalysis> results = runner.run(QueryPlus.wrap(segmentMetadataQuery)).toList();
    System.out.println(results);
    Assert.assertEquals(1, results.size());
    Map<String, ColumnAnalysis> columns = results.get(0).getColumns();
    Assert.assertNotNull(columns.get("histogram"));
    Assert.assertEquals("spectatorHistogramDistribution", columns.get("histogram").getType());
  }

  @Test
  public void testPercentilePostAggregator() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_DATA_PARSE_SPEC,
        String.join(
            "\n",
            "[",
            "  {\"type\": \"spectatorHistogram\", \"name\": \"histogram\", \"fieldName\": \"cost\"}",
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
            "  \"dimenions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"spectatorHistogram\", \"name\": \"merged_cost_histogram\", \"fieldName\": "
            + "\"histogram\"}",
            "  ],",
            "  \"postAggregations\": [",
            "    {\"type\": \"percentileSpectatorHistogram\", \"name\": \"percentileValue\", \"field\": {\"type\": \"fieldAccess\",\"fieldName\": \"merged_cost_histogram\"}"
            + ", \"percentile\": \"50.0\"},",
            "    {\"type\": \"percentilesSpectatorHistogram\", \"name\": \"percentileValues\", \"field\": {\"type\": \"fieldAccess\",\"fieldName\": \"merged_cost_histogram\"}"
            + ", \"percentiles\": [25.0, 50.0, 75.0, 99.0]}",
            "  ],",
            "  \"intervals\": [\"2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    // Check on Median (true median is 40)
    Assert.assertEquals(40.0, (double) results.get(0).get(1), 0.2);
    // True percentiles for 25, 50, 75, 99
    double[] expectedPercentiles = new double[]{40.0, 40.0, 50.0, 18404.0};
    double[] resultPercentiles = (double[]) results.get(0).get(2);

    for (int i = 0; i < expectedPercentiles.length; i++) {
      double expectedPercentile = expectedPercentiles[i];
      double resultPercentile = resultPercentiles[i];
      double error18pcnt = expectedPercentile * 0.18;
      // Should be within 18%
      Assert.assertEquals(expectedPercentile, resultPercentile, error18pcnt);
    }
  }

  private static void assertResultsMatch(List<ResultRow> results, int rowNum, String expectedProduct)
  {
    ResultRow row = results.get(rowNum);
    Object product = row.get(0);
    Assert.assertTrue("Expected dimension of type String", product instanceof String);
    Assert.assertEquals("Product values didn't match", expectedProduct, product);
    Object histogram = row.get(1);
    Assert.assertTrue(
        "Expected histogram metric of type SpectatorHistogramUtils.HistogramMap",
        histogram instanceof SpectatorHistogram
    );
    Assert.assertEquals("Count values didn't match", EXPECTED_HISTOGRAMS.get(product), histogram);
  }

}
