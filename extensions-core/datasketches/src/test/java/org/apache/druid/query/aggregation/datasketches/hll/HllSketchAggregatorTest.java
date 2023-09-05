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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class HllSketchAggregatorTest extends InitializedNullHandlingTest
{
  private static final boolean ROUND = true;

  private final AggregationTestHelper groupByHelper;
  private final AggregationTestHelper timeseriesHelper;
  private final QueryContexts.Vectorize vectorize;
  private final StringEncoding stringEncoding;

  @Rule
  public final TemporaryFolder groupByFolder = new TemporaryFolder();

  @Rule
  public final TemporaryFolder timeseriesFolder = new TemporaryFolder();

  public HllSketchAggregatorTest(GroupByQueryConfig config, String vectorize, StringEncoding stringEncoding)
  {
    HllSketchModule.registerSerde();
    groupByHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        new HllSketchModule().getJacksonModules(), config, groupByFolder
    );
    timeseriesHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(
        new HllSketchModule().getJacksonModules(), timeseriesFolder
    );
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.stringEncoding = stringEncoding;
  }

  @Parameterized.Parameters(name = "groupByConfig = {0}, vectorize = {1}, stringEncoding = {2}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (String vectorize : new String[]{"false", "force"}) {
        for (StringEncoding stringEncoding : StringEncoding.values()) {
          if (!("force".equals(vectorize))) {
            constructors.add(new Object[]{config, vectorize, stringEncoding});
          }
        }
      }
    }
    return constructors;
  }

  @Test
  public void ingestSketches() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim"),
            Arrays.asList("timestamp", "dim", "multiDim", "sketch")
        ),
        buildAggregatorJson("HLLSketchMerge", "sketch", !ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchMerge", "sketch", !ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200, (double) row.get(0), 0.1);
  }

  @Test
  public void ingestSketchesTimeseries() throws Exception
  {
    final File inputFile = new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile());
    final String parserJson = buildParserJson(
        Arrays.asList("dim", "multiDim"),
        Arrays.asList("timestamp", "dim", "multiDim", "sketch")
    );
    final String aggregators =
        buildAggregatorJson("HLLSketchMerge", "sketch", !ROUND, HllSketchAggregatorFactory.DEFAULT_STRING_ENCODING);
    final int minTimestamp = 0;
    final Granularity gran = Granularities.NONE;
    final int maxRowCount = 10;
    final String queryJson = buildTimeseriesQueryJson("HLLSketchMerge", "sketch", !ROUND);

    File segmentDir1 = timeseriesFolder.newFolder();
    timeseriesHelper.createIndex(
        inputFile,
        parserJson,
        aggregators,
        segmentDir1,
        minTimestamp,
        gran,
        maxRowCount,
        true
    );

    File segmentDir2 = timeseriesFolder.newFolder();
    timeseriesHelper.createIndex(
        inputFile,
        parserJson,
        aggregators,
        segmentDir2,
        minTimestamp,
        gran,
        maxRowCount,
        true
    );

    Sequence<Result> seq = timeseriesHelper.runQueryOnSegments(Arrays.asList(segmentDir1, segmentDir2), queryJson);
    List<Result> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Result row = results.get(0);
    Assert.assertEquals(200, (double) ((TimeseriesResultValue) row.getValue()).getMetric("sketch"), 0.1);
  }

  @Test
  public void buildSketchesAtIngestionTime() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Collections.singletonList("dim"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        buildAggregatorJson("HLLSketchBuild", "id", !ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchMerge", "sketch", !ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200, (double) row.get(0), 0.1);
  }

  @Test
  public void buildSketchesAtIngestionTimeTimeseries() throws Exception
  {
    Sequence<Result> seq = timeseriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Collections.singletonList("dim"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        buildAggregatorJson("HLLSketchBuild", "id", !ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildTimeseriesQueryJson("HLLSketchMerge", "sketch", !ROUND)
    );
    List<Result> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Result row = results.get(0);
    Assert.assertEquals(200, (double) ((TimeseriesResultValue) row.getValue()).getMetric("sketch"), 0.1);
  }

  @Test
  public void buildSketchesAtQueryTime() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim", "id"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        "[]",
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchBuild", "id", !ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200, (double) row.get(0), 0.1);
  }

  @Test
  public void buildSketchesAtQueryTimeTimeseries() throws Exception
  {
    Sequence<Result> seq = timeseriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim", "id"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        "[]",
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildTimeseriesQueryJson("HLLSketchBuild", "id", !ROUND)
    );
    List<Result> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Result row = results.get(0);
    Assert.assertEquals(200, (double) ((TimeseriesResultValue) row.getValue()).getMetric("sketch"), 0.1);
  }

  @Test
  public void unsuccessfulComplexTypesInHLL() throws Exception
  {
    String metricSpec = "[{"
                        + "\"type\": \"hyperUnique\","
                        + "\"name\": \"index_hll\","
                        + "\"fieldName\": \"id\""
                        + "}]";
    try {
      Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
          new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile()),
          buildParserJson(
              Arrays.asList("dim", "multiDim", "id"),
              Arrays.asList("timestamp", "dim", "multiDim", "id")
          ),
          metricSpec,
          0, // minTimestamp
          Granularities.NONE,
          200, // maxRowCount
          buildGroupByQueryJson("HLLSketchMerge", "sketch", ROUND, stringEncoding)
      );
    }
    catch (RuntimeException e) {
      Assert.assertTrue(
          e.getMessage().contains("Invalid input [index_hll] of type [COMPLEX<hyperUnique>] for [HLLSketchBuild]"));
    }
  }

  @Test
  public void buildSketchesAtQueryTimeMultiValue() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim", "id"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        "[]",
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchBuild", "multiDim", !ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(14, (double) row.get(0), 0.1);
  }

  @Test
  public void roundBuildSketch() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim", "id"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        "[]",
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchBuild", "id", ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200L, (long) row.get(0));
  }

  @Test
  public void roundMergeSketch() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim"),
            Arrays.asList("timestamp", "dim", "multiDim", "sketch")
        ),
        buildAggregatorJson("HLLSketchMerge", "sketch", ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchMerge", "sketch", ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200L, (long) row.get(0));
  }

  @Test
  public void testPostAggs() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim"),
            Arrays.asList("timestamp", "dim", "multiDim", "sketch")
        ),
        buildAggregatorJson("HLLSketchMerge", "sketch", ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        groupByHelper.getObjectMapper().writeValueAsString(
            GroupByQuery.builder()
                        .setDataSource("test_datasource")
                        .setGranularity(Granularities.ALL)
                        .setInterval(Intervals.ETERNITY)
                        .setAggregatorSpecs(
                            new HllSketchMergeAggregatorFactory("sketch", "sketch", null, null, null, null, false)
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new HllSketchToEstimatePostAggregator(
                                    "estimate",
                                    new FieldAccessPostAggregator("f1", "sketch"),
                                    false
                                ),
                                new HllSketchToEstimateWithBoundsPostAggregator(
                                    "estimateWithBounds",
                                    new FieldAccessPostAggregator(
                                        "f1",
                                        "sketch"
                                    ),
                                    2
                                ),
                                new HllSketchToStringPostAggregator(
                                    "summary",
                                    new FieldAccessPostAggregator("f1", "sketch")
                                ),
                                new HllSketchUnionPostAggregator(
                                    "union",
                                    ImmutableList.of(new FieldAccessPostAggregator(
                                        "f1",
                                        "sketch"
                                    ), new FieldAccessPostAggregator("f2", "sketch")),
                                    null,
                                    null
                                ),
                                new FieldAccessPostAggregator("fieldAccess", "sketch")
                            )
                        )
                        .build()
        )
    );
    final String expectedSummary = "### HLL SKETCH SUMMARY: \n"
                                   + "  Log Config K   : 12\n"
                                   + "  Hll Target     : HLL_4\n"
                                   + "  Current Mode   : SET\n"
                                   + "  Memory         : false\n"
                                   + "  LB             : 200.0\n"
                                   + "  Estimate       : 200.0000988444255\n"
                                   + "  UB             : 200.01008469948434\n"
                                   + "  OutOfOrder Flag: false\n"
                                   + "  Coupon Count   : 200\n";

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200, (double) row.get(0), 0.1);
    Assert.assertEquals(200, (double) row.get(1), 0.1);
    Assert.assertArrayEquals(new double[]{200, 200, 200}, (double[]) row.get(2), 0.1);
    Assert.assertEquals(expectedSummary, row.get(3));
    // union with self = self
    Assert.assertEquals(expectedSummary, ((HllSketchHolder) row.get(4)).getSketch().toString());
  }

  private static String buildParserJson(List<String> dimensions, List<String> columns)
  {
    Map<String, Object> timestampSpec = ImmutableMap.of(
        "column", "timestamp",
        "format", "yyyyMMdd"
    );
    Map<String, Object> dimensionsSpec = ImmutableMap.of(
        "dimensions", dimensions,
        "dimensionExclusions", Collections.emptyList(),
        "spatialDimensions", Collections.emptyList()
    );
    Map<String, Object> parseSpec = ImmutableMap.of(
        "format", "tsv",
        "timestampSpec", timestampSpec,
        "dimensionsSpec", dimensionsSpec,
        "columns", columns,
        "listDelimiter", ","
    );
    Map<String, Object> object = ImmutableMap.of(
        "type", "string",
        "parseSpec", parseSpec
    );
    return toJson(object);
  }

  private static String toJson(Object object)
  {
    final String json;
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return json;
  }

  private static String buildAggregatorJson(
      String aggregationType,
      String aggregationFieldName,
      boolean aggregationRound,
      StringEncoding stringEncoding
  )
  {
    Map<String, Object> aggregator = buildAggregatorObject(
        aggregationType,
        aggregationFieldName,
        aggregationRound,
        stringEncoding
    );
    return toJson(Collections.singletonList(aggregator));
  }

  private static Map<String, Object> buildAggregatorObject(
      String aggregationType,
      String aggregationFieldName,
      boolean aggregationRound,
      StringEncoding stringEncoding
  )
  {
    return ImmutableMap.<String, Object>builder()
                       .put("type", aggregationType)
                       .put("name", "sketch")
                       .put("fieldName", aggregationFieldName)
                       .put("round", aggregationRound)
                       .put("tgtHllType", "HLL_8")
                       .put("stringEncoding", stringEncoding.toString())
                       .build();
  }

  private String buildGroupByQueryJson(
      String aggregationType,
      String aggregationFieldName,
      boolean aggregationRound,
      StringEncoding stringEncoding
  )
  {
    Map<String, Object> aggregation = buildAggregatorObject(
        aggregationType,
        aggregationFieldName,
        aggregationRound,
        stringEncoding
    );
    Map<String, Object> object = new ImmutableMap.Builder<String, Object>()
        .put("queryType", "groupBy")
        .put("dataSource", "test_dataSource")
        .put("granularity", "ALL")
        .put("dimensions", Collections.emptyList())
        .put("aggregations", Collections.singletonList(aggregation))
        .put(
            "postAggregations",
            Collections.singletonList(
                ImmutableMap.of("type", "fieldAccess", "name", "sketch_raw", "fieldName", "sketch")
            )
        )
        .put("intervals", Collections.singletonList("2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z"))
        .put("context", ImmutableMap.of(QueryContexts.VECTORIZE_KEY, vectorize.toString()))
        .build();
    return toJson(object);
  }

  private String buildTimeseriesQueryJson(
      String aggregationType,
      String aggregationFieldName,
      boolean aggregationRound
  )
  {
    Map<String, Object> aggregation = buildAggregatorObject(
        aggregationType,
        aggregationFieldName,
        aggregationRound,
        HllSketchAggregatorFactory.DEFAULT_STRING_ENCODING
    );
    Map<String, Object> object = new ImmutableMap.Builder<String, Object>()
        .put("queryType", "timeseries")
        .put("dataSource", "test_dataSource")
        .put("granularity", "ALL")
        .put("aggregations", Collections.singletonList(aggregation))
        .put("intervals", Collections.singletonList("2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z"))
        .put("context", ImmutableMap.of(QueryContexts.VECTORIZE_KEY, vectorize.toString()))
        .build();
    return toJson(object);
  }

}
