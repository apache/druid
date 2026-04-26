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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.datasketches.hll.HllSketch;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByTestColumnSelectorFactory;
import org.apache.druid.query.groupby.epinephelinae.GrouperTestUtil;
import org.apache.druid.query.timeseries.TimeseriesQuery;
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
import java.util.List;

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
        buildInputRowSchema(List.of("dim", "multiDim")),
        buildInputFormat(List.of("timestamp", "dim", "multiDim", "sketch")),
        buildMergeAggregatorList("sketch", !ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQuery("HLLSketchMerge", "sketch", !ROUND, stringEncoding)
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
    final InputRowSchema inputRowSchema = buildInputRowSchema(List.of("dim", "multiDim"));
    final DelimitedInputFormat inputFormat = buildInputFormat(List.of("timestamp", "dim", "multiDim", "sketch"));
    final List<AggregatorFactory> aggregators =
        buildMergeAggregatorList("sketch", !ROUND, HllSketchAggregatorFactory.DEFAULT_STRING_ENCODING);
    final int minTimestamp = 0;
    final int maxRowCount = 10;

    File segmentDir1 = timeseriesFolder.newFolder();
    timeseriesHelper.createIndex(
        inputFile,
        inputRowSchema,
        inputFormat,
        aggregators,
        segmentDir1,
        minTimestamp,
        Granularities.NONE,
        maxRowCount,
        true
    );

    File segmentDir2 = timeseriesFolder.newFolder();
    timeseriesHelper.createIndex(
        inputFile,
        inputRowSchema,
        inputFormat,
        aggregators,
        segmentDir2,
        minTimestamp,
        Granularities.NONE,
        maxRowCount,
        true
    );

    Sequence<Result<TimeseriesResultValue>> seq = timeseriesHelper.runQueryOnSegments(
        Arrays.asList(segmentDir1, segmentDir2),
        buildTimeseriesQuery("HLLSketchMerge", "sketch", !ROUND)
    );
    List<Result<TimeseriesResultValue>> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Result<TimeseriesResultValue> row = results.get(0);
    Assert.assertEquals(200, (double) row.getValue().getMetric("sketch"), 0.1);
  }

  @Test
  public void buildSketchesAtIngestionTime() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildInputRowSchema(List.of("dim")),
        buildInputFormat(List.of("timestamp", "dim", "multiDim", "id")),
        buildBuildAggregatorList("id", !ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQuery("HLLSketchMerge", "sketch", !ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200, (double) row.get(0), 0.1);
  }

  @Test
  public void buildSketchesAtIngestionTimeTimeseries() throws Exception
  {
    Sequence<Result<TimeseriesResultValue>> seq = timeseriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildInputRowSchema(List.of("dim")),
        buildInputFormat(List.of("timestamp", "dim", "multiDim", "id")),
        buildBuildAggregatorList("id", !ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildTimeseriesQuery("HLLSketchMerge", "sketch", !ROUND)
    );
    List<Result<TimeseriesResultValue>> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Result<TimeseriesResultValue> row = results.get(0);
    Assert.assertEquals(200, (double) row.getValue().getMetric("sketch"), 0.1);
  }

  @Test
  public void buildSketchesAtQueryTime() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildInputRowSchema(List.of("dim", "multiDim", "id")),
        buildInputFormat(List.of("timestamp", "dim", "multiDim", "id")),
        List.of(),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQuery("HLLSketchBuild", "id", !ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200, (double) row.get(0), 0.1);
  }

  @Test
  public void buildSketchesAtQueryTimeTimeseries() throws Exception
  {
    Sequence<Result<TimeseriesResultValue>> seq = timeseriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildInputRowSchema(List.of("dim", "multiDim", "id")),
        buildInputFormat(List.of("timestamp", "dim", "multiDim", "id")),
        List.of(),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildTimeseriesQuery("HLLSketchBuild", "id", !ROUND)
    );
    List<Result<TimeseriesResultValue>> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Result<TimeseriesResultValue> row = results.get(0);
    Assert.assertEquals(200, (double) row.getValue().getMetric("sketch"), 0.1);
  }

  @Test
  public void unsuccessfulComplexTypesInHLL() throws Exception
  {
    try {
      Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
          new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile()),
          buildInputRowSchema(List.of("dim", "multiDim", "id")),
          buildInputFormat(List.of("timestamp", "dim", "multiDim", "id")),
          List.of(new HyperUniquesAggregatorFactory("index_hll", "id")),
          0, // minTimestamp
          Granularities.NONE,
          200, // maxRowCount
          buildGroupByQuery("HLLSketchMerge", "sketch", ROUND, stringEncoding)
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
        buildInputRowSchema(List.of("dim", "multiDim", "id")),
        buildInputFormat(List.of("timestamp", "dim", "multiDim", "id")),
        List.of(),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQuery("HLLSketchBuild", "multiDim", !ROUND, stringEncoding)
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
        buildInputRowSchema(List.of("dim", "multiDim", "id")),
        buildInputFormat(List.of("timestamp", "dim", "multiDim", "id")),
        List.of(),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQuery("HLLSketchBuild", "id", ROUND, stringEncoding)
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
        buildInputRowSchema(List.of("dim", "multiDim")),
        buildInputFormat(List.of("timestamp", "dim", "multiDim", "sketch")),
        buildMergeAggregatorList("sketch", ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQuery("HLLSketchMerge", "sketch", ROUND, stringEncoding)
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
        buildInputRowSchema(List.of("dim", "multiDim")),
        buildInputFormat(List.of("timestamp", "dim", "multiDim", "sketch")),
        buildMergeAggregatorList("sketch", ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
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

  @Test
  public void testRelocation()
  {
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    HllSketchHolder sketchHolder = new HllSketchHolder(null, new HllSketch());
    sketchHolder.getSketch().update(1);

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("sketch", sketchHolder)));
    HllSketchHolder[] holders = groupByHelper.runRelocateVerificationTest(
        new HllSketchMergeAggregatorFactory("sketch", "sketch", null, null, null, true, true),
        columnSelectorFactory,
        HllSketchHolder.class
    );
    Assert.assertEquals(holders[0].getEstimate(), holders[1].getEstimate(), 0);
  }

  private static InputRowSchema buildInputRowSchema(List<String> dimensions)
  {
    return new InputRowSchema(
        new TimestampSpec("timestamp", "yyyyMMdd", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dimensions)),
        ColumnsFilter.all()
    );
  }

  private static DelimitedInputFormat buildInputFormat(List<String> columns)
  {
    return new DelimitedInputFormat(columns, ",", null, null, null, 0, null);
  }

  private static List<AggregatorFactory> buildMergeAggregatorList(
      String fieldName,
      boolean round,
      StringEncoding stringEncoding
  )
  {
    return List.of(new HllSketchMergeAggregatorFactory("sketch", fieldName, null, "HLL_8", stringEncoding, null, round));
  }

  private static List<AggregatorFactory> buildBuildAggregatorList(
      String fieldName,
      boolean round,
      StringEncoding stringEncoding
  )
  {
    return List.of(new HllSketchBuildAggregatorFactory("sketch", fieldName, null, "HLL_8", stringEncoding, null, round));
  }

  private GroupByQuery buildGroupByQuery(
      String aggregationType,
      String fieldName,
      boolean round,
      StringEncoding stringEncoding
  )
  {
    final HllSketchAggregatorFactory agg;
    if ("HLLSketchMerge".equals(aggregationType)) {
      agg = new HllSketchMergeAggregatorFactory("sketch", fieldName, null, "HLL_8", stringEncoding, null, round);
    } else {
      agg = new HllSketchBuildAggregatorFactory("sketch", fieldName, null, "HLL_8", stringEncoding, null, round);
    }
    return GroupByQuery.builder()
                       .setDataSource("test_dataSource")
                       .setGranularity(Granularities.ALL)
                       .setInterval(Intervals.of("2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z"))
                       .setAggregatorSpecs(agg)
                       .setPostAggregatorSpecs(
                           new FieldAccessPostAggregator("sketch_raw", "sketch")
                       )
                       .setContext(ImmutableMap.of(QueryContexts.VECTORIZE_KEY, vectorize.toString()))
                       .build();
  }

  private TimeseriesQuery buildTimeseriesQuery(
      String aggregationType,
      String fieldName,
      boolean round
  )
  {
    final HllSketchAggregatorFactory agg;
    if ("HLLSketchMerge".equals(aggregationType)) {
      agg = new HllSketchMergeAggregatorFactory(
          "sketch",
          fieldName,
          null,
          "HLL_8",
          HllSketchAggregatorFactory.DEFAULT_STRING_ENCODING,
          null,
          round
      );
    } else {
      agg = new HllSketchBuildAggregatorFactory(
          "sketch",
          fieldName,
          null,
          "HLL_8",
          HllSketchAggregatorFactory.DEFAULT_STRING_ENCODING,
          null,
          round
      );
    }
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource("test_dataSource")
                 .granularity(Granularities.ALL)
                 .intervals(Intervals.of("2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z").toString())
                 .aggregators(agg)
                 .context(ImmutableMap.of(QueryContexts.VECTORIZE_KEY, vectorize.toString()))
                 .build();
  }
}
