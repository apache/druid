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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.netflix.spectator.api.histogram.PercentileBuckets;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
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
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpectatorHistogramAggregatorTest extends InitializedNullHandlingTest
{
  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      new TimestampSpec("timestamp", "yyyyMMddHH", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
      ColumnsFilter.all()
  );

  private static final DelimitedInputFormat INPUT_FORMAT = DelimitedInputFormat.forColumns(
      List.of("timestamp", "product", "cost")
  );
  @TempDir
  public File tempFolder;

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

  private AggregationTestHelper helper;
  private AggregationTestHelper timeSeriesHelper;

  public void initSpectatorHistogramAggregatorTest(final GroupByQueryConfig config)
  {
    SpectatorHistogramModule.registerSerde();
    SpectatorHistogramModule module = new SpectatorHistogramModule();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelperWithTempDir(
        module.getJacksonModules(), config, tempFolder);
    timeSeriesHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelperWithTempDir(
        module.getJacksonModules(),
        tempFolder
    );
  }

  private static List<GroupByQueryConfig> testConfigs()
  {
    return List.of(
        new GroupByQueryConfig()
        {
          @Override
          public int getBufferGrouperInitialBuckets()
          {
            return 4;
          }
        },
        new GroupByQueryConfig()
        {
          @Override
          public int getBufferGrouperMaxSize()
          {
            return 2;
          }

          @Override
          public HumanReadableBytes getMaxOnDiskStorage()
          {
            return HumanReadableBytes.valueOf(10L * 1024 * 1024);
          }
        },
        new org.apache.druid.jackson.DefaultObjectMapper().convertValue(
            java.util.Map.of(
                "maxSelectorDictionarySize", 20,
                "maxMergingDictionarySize", 400,
                "maxOnDiskStorage", 10L * 1024 * 1024
            ),
            GroupByQueryConfig.class
        ),
        new GroupByQueryConfig()
        {
          @Override
          public int getNumParallelCombineThreads()
          {
            return 2;
          }
        }
    );
  }

  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  // this is to test Json properties and equals
  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void serializeDeserializeFactoryWithFieldName(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
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

    Assertions.assertEquals(factory, other);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testBuildingHistogramQueryTime(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new LongSumAggregatorFactory("cost_sum", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setDimensions(new DefaultDimensionSpec("product", "product"))
                    .setAggregatorSpecs(new SpectatorHistogramAggregatorFactory("cost_histogram", "cost_sum"))
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .build()
    );
    List<ResultRow> results = seq.toList();
    assertResultsMatch(results, 0, "A");
    assertResultsMatch(results, 1, "B");
    assertResultsMatch(results, 2, "C");
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testBuildingAndMergingHistograms(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory("histogram", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(new SpectatorHistogramAggregatorFactory("merged_cost_histogram", "histogram"))
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .build()
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals(expected, results.get(0).get(0));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testBuildingAndMergingHistogramsTimeseriesQuery(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    TimeseriesQuery tsQuery = Druids.newTimeseriesQueryBuilder()
        .dataSource("test_datasource")
        .granularity(Granularities.ALL)
        .aggregators(new SpectatorHistogramAggregatorFactory("merged_cost_histogram", "histogram"))
        .intervals("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
        .build();
    Sequence<Result<TimeseriesResultValue>> seq = timeSeriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory("histogram", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        tsQuery
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    List<Result<TimeseriesResultValue>> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    SpectatorHistogram value = (SpectatorHistogram) results.get(0).getValue().getMetric("merged_cost_histogram");
    Assertions.assertEquals(expected, value);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testBuildingAndMergingGroupbyHistograms(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory("histogram", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setDimensions(new DefaultDimensionSpec("product", "product"))
                    .setAggregatorSpecs(new SpectatorHistogramAggregatorFactory("merged_histogram", "histogram"))
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .build()
    );

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(6, results.size());

    SpectatorHistogram expectedA = new SpectatorHistogram();
    expectedA.add(PercentileBuckets.indexOf(10), 1L);
    Assertions.assertEquals(expectedA, results.get(0).get(1));

    SpectatorHistogram expectedB = new SpectatorHistogram();
    expectedB.add(PercentileBuckets.indexOf(30), 1L);
    expectedB.add(PercentileBuckets.indexOf(40), 3L);
    expectedB.add(PercentileBuckets.indexOf(50), 2L);
    Assertions.assertEquals(expectedB, results.get(1).get(1));

    SpectatorHistogram expectedC = new SpectatorHistogram();
    expectedC.add(PercentileBuckets.indexOf(50), 1L);
    expectedC.add(PercentileBuckets.indexOf(20000), 1L);
    Assertions.assertEquals(expectedC, results.get(2).get(1));

    Assertions.assertNull(results.get(3).get(1));
    Assertions.assertNull(results.get(4).get(1));
    Assertions.assertNull(results.get(5).get(1));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testBuildingAndCountingHistograms(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory("histogram", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(
                        new LongSumAggregatorFactory("count_histogram", "histogram"),
                        new DoubleSumAggregatorFactory("double_count_histogram", "histogram")
                    )
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .build()
    );

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    // Check longSum
    Assertions.assertEquals(9L, results.get(0).get(0));
    // Check doubleSum
    Assertions.assertEquals(9.0, (Double) results.get(0).get(1), 0.001);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testBuildingAndCountingHistogramsWithNullFilter(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory("histogram", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(
                        new LongSumAggregatorFactory("count_histogram", "histogram"),
                        new DoubleSumAggregatorFactory("double_count_histogram", "histogram")
                    )
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .setDimFilter(new AndDimFilter(
                        new NotDimFilter(new SelectorDimFilter("histogram", "0", null)),
                        new NotDimFilter(new SelectorDimFilter("histogram", "", null))
                    ))
                    .build()
    );

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    // Check longSum
    Assertions.assertEquals(9L, results.get(0).get(0));
    // Check doubleSum
    Assertions.assertEquals(9.0, (Double) results.get(0).get(1), 0.001);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIngestAsHistogramDistribution(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory.Distribution("histogram", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(new SpectatorHistogramAggregatorFactory("merged_cost_histogram", "histogram"))
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .build()
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals(expected, results.get(0).get(0));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIngestHistogramsTimer(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory.Timer("histogram", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(new SpectatorHistogramAggregatorFactory("merged_cost_histogram", "histogram"))
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .build()
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals(expected, results.get(0).get(0));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIngestingPreaggregatedHistograms(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    TimeseriesQuery preAggTsQuery = Druids.newTimeseriesQueryBuilder()
        .dataSource("test_datasource")
        .granularity(Granularities.ALL)
        .aggregators(new SpectatorHistogramAggregatorFactory("merged_cost_histogram", "histogram"))
        .intervals("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
        .build();
    Sequence<Result<TimeseriesResultValue>> seq = timeSeriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("pre_agg_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory("histogram", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        preAggTsQuery
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    List<Result<TimeseriesResultValue>> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    SpectatorHistogram value = (SpectatorHistogram) results.get(0).getValue().getMetric("merged_cost_histogram");
    Assertions.assertEquals(expected, value);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testMetadataQueryTimer(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    File segmentDir = newFolder(tempFolder, "junit");
    helper.createIndex(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory.Timer("histogram", "cost")),
        segmentDir,
        0, // minTimestamp
        Granularities.NONE,
        10 // maxRowCount
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
    Assertions.assertEquals(1, results.size());
    Map<String, ColumnAnalysis> columns = results.get(0).getColumns();
    Assertions.assertNotNull(columns.get("histogram"));
    Assertions.assertEquals("spectatorHistogramTimer", columns.get("histogram").getType());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testMetadataQueryDistribution(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    File segmentDir = newFolder(tempFolder, "junit");
    helper.createIndex(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory.Distribution("histogram", "cost")),
        segmentDir,
        0, // minTimestamp
        Granularities.NONE,
        10 // maxRowCount
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
    Assertions.assertEquals(1, results.size());
    Map<String, ColumnAnalysis> columns = results.get(0).getColumns();
    Assertions.assertNotNull(columns.get("histogram"));
    Assertions.assertEquals("spectatorHistogramDistribution", columns.get("histogram").getType());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testPercentilePostAggregator(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory("histogram", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(new SpectatorHistogramAggregatorFactory("merged_cost_histogram", "histogram"))
                    .setPostAggregatorSpecs(
                        new SpectatorHistogramPercentilePostAggregator(
                            "percentileValue",
                            new FieldAccessPostAggregator(null, "merged_cost_histogram"),
                            50.0
                        ),
                        new SpectatorHistogramPercentilesPostAggregator(
                            "percentileValues",
                            new FieldAccessPostAggregator(null, "merged_cost_histogram"),
                            new double[]{25.0, 50.0, 75.0, 99.0}
                        )
                    )
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .build()
    );
    SpectatorHistogram expected = new SpectatorHistogram();
    expected.add(PercentileBuckets.indexOf(10), 1L);
    expected.add(PercentileBuckets.indexOf(30), 1L);
    expected.add(PercentileBuckets.indexOf(40), 3L);
    expected.add(PercentileBuckets.indexOf(50), 3L);
    expected.add(PercentileBuckets.indexOf(20000), 1L);

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    // Check on Median (true median is 40)
    Assertions.assertEquals(40.0, (double) results.get(0).get(1), 0.2);
    // True percentiles for 25, 50, 75, 99
    double[] expectedPercentiles = new double[]{40.0, 40.0, 50.0, 18404.0};
    double[] resultPercentiles = (double[]) results.get(0).get(2);

    for (int i = 0; i < expectedPercentiles.length; i++) {
      double expectedPercentile = expectedPercentiles[i];
      double resultPercentile = resultPercentiles[i];
      double error18pcnt = expectedPercentile * 0.18;
      // Should be within 18%
      Assertions.assertEquals(expectedPercentile, resultPercentile, error18pcnt);
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testBuildingAndCountingHistogramsIncrementalIndex(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    List<String> dimensions = Collections.singletonList("d");
    int n = 10;
    DateTime startOfDay = DateTimes.of("2000-01-01");
    List<InputRow> inputRows = new ArrayList<>(n);
    for (int i = 1; i <= n; i++) {
      String val = String.valueOf(i * 1.0d);

      inputRows.add(new MapBasedInputRow(
          startOfDay.plusMinutes(i),
          dimensions,
          ImmutableMap.of("x", i, "d", val)
      ));
    }

    IncrementalIndex index = IndexBuilder.create()
                                         .rows(inputRows)
                                         .schema(
                                             IncrementalIndexSchema.builder()
                                                                   .withDimensionsSpec(
                                                                       DimensionsSpec.builder()
                                                                                     .setDefaultSchemaDimensions(dimensions)
                                                                                     .build()
                                                                   )
                                                                   .withMetrics(
                                                                       new CountAggregatorFactory("count"),
                                                                       new SpectatorHistogramAggregatorFactory("histogram", "x")
                                                                   )
                                                                   .withQueryGranularity(Granularities.NONE)
                                                                   .build()
                                         )
                                         .buildIncrementalIndex();

    ImmutableList<Segment> segments = ImmutableList.of(
        new IncrementalIndexSegment(index, SegmentId.dummy("test")),
        helper.persistIncrementalIndex(index, null)
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource("test")
        .setGranularity(Granularities.HOUR)
        .setInterval("1970/2050")
        .setAggregatorSpecs(
            new DoubleSumAggregatorFactory("doubleSum", "histogram")
        ).build();

    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segments, query);

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    // Check timestamp
    Assertions.assertEquals(startOfDay.getMillis(), results.get(0).get(0));
    // Check doubleSum
    Assertions.assertEquals(n * segments.size(), (Double) results.get(0).get(1), 0.001);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testPercentilePostAggregatorWithNullSketch(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory("histogram", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setDimensions(new DefaultDimensionSpec("product", "product"))
                    .setAggregatorSpecs(new SpectatorHistogramAggregatorFactory("merged_histogram", "histogram"))
                    .setPostAggregatorSpecs(
                        new SpectatorHistogramPercentilePostAggregator(
                            "p50",
                            new FieldAccessPostAggregator(null, "merged_histogram"),
                            50.0
                        )
                    )
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .build()
    );

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(6, results.size());

    // First three rows should have valid histograms and percentile values
    Assertions.assertNotNull(results.get(0).get(2), "Row [0] should have non-null percentile");
    Assertions.assertNotNull(results.get(1).get(2), "Row [1] should have non-null percentile");
    Assertions.assertNotNull(results.get(2).get(2), "Row [2] should have non-null percentile");

    // Last three rows have null histograms, so percentile should also be null
    Assertions.assertNull(results.get(3).get(2), "Row [3] should have null percentile when histogram is null");
    Assertions.assertNull(results.get(4).get(2), "Row [4] should have null percentile when histogram is null");
    Assertions.assertNull(results.get(5).get(2), "Row [5] should have null percentile when histogram is null");
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testPercentilesPostAggregatorWithNullSketch(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory("histogram", "cost")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setDimensions(new DefaultDimensionSpec("product", "product"))
                    .setAggregatorSpecs(new SpectatorHistogramAggregatorFactory("merged_histogram", "histogram"))
                    .setPostAggregatorSpecs(
                        new SpectatorHistogramPercentilesPostAggregator(
                            "percentiles",
                            new FieldAccessPostAggregator(null, "merged_histogram"),
                            new double[]{25.0, 50.0, 75.0}
                        )
                    )
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .build()
    );

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(6, results.size());

    // First three rows should have valid histograms and percentiles arrays
    Assertions.assertNotNull(results.get(0).get(2), "Row [0] should have non-null percentiles array");
    Assertions.assertTrue(results.get(0).get(2) instanceof double[], "Row [0] percentiles should be double array");
    Assertions.assertNotNull(results.get(1).get(2), "Row [1] should have non-null percentiles array");
    Assertions.assertTrue(results.get(1).get(2) instanceof double[], "Row [1] percentiles should be double array");
    Assertions.assertNotNull(results.get(2).get(2), "Row [2] should have non-null percentiles array");
    Assertions.assertTrue(results.get(2).get(2) instanceof double[], "Row [2] percentiles should be double array");

    // Last three rows have null histograms, so percentiles should also be null
    Assertions.assertNull(results.get(3).get(2), "Row [3] should have null percentiles when histogram is null");
    Assertions.assertNull(results.get(4).get(2), "Row [4] should have null percentiles when histogram is null");
    Assertions.assertNull(results.get(5).get(2), "Row [5] should have null percentiles when histogram is null");
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testCountPostAggregator(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory("histogram", "cost")),
        0,
        Granularities.NONE,
        10,
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(new SpectatorHistogramAggregatorFactory("merged_cost_histogram", "histogram"))
                    .setPostAggregatorSpecs(
                        new SpectatorHistogramCountPostAggregator(
                            "count",
                            new FieldAccessPostAggregator(null, "merged_cost_histogram")
                        )
                    )
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .build()
    );

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    // The merged histogram has 9 total observations (1+1+3+3+1 from the buckets)
    Assertions.assertEquals(9L, results.get(0).get(1));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testCountPostAggregatorWithNullSketch(final GroupByQueryConfig config) throws Exception
  {
    initSpectatorHistogramAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("input_data.tsv").getFile()),
        INPUT_ROW_SCHEMA,
        INPUT_FORMAT,
        List.of(new SpectatorHistogramAggregatorFactory("histogram", "cost")),
        0,
        Granularities.NONE,
        10,
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setDimensions(new DefaultDimensionSpec("product", "product"))
                    .setAggregatorSpecs(new SpectatorHistogramAggregatorFactory("merged_histogram", "histogram"))
                    .setPostAggregatorSpecs(
                        new SpectatorHistogramCountPostAggregator(
                            "count",
                            new FieldAccessPostAggregator(null, "merged_histogram")
                        )
                    )
                    .setInterval("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z")
                    .build()
    );

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(6, results.size());

    // First three rows should have valid histograms and count values
    // Product A: 1 observation
    Assertions.assertEquals(1L, results.get(0).get(2));
    // Product B: 6 observations (1+3+2 from buckets at indices 30, 40, 50)
    Assertions.assertEquals(6L, results.get(1).get(2));
    // Product C: 2 observations (1+1 from buckets at indices 50, 20000)
    Assertions.assertEquals(2L, results.get(2).get(2));

    // Last three rows have null histograms, so count should also be null
    Assertions.assertNull(results.get(3).get(2), "Row [3] should have null count when histogram is null");
    Assertions.assertNull(results.get(4).get(2), "Row [4] should have null count when histogram is null");
    Assertions.assertNull(results.get(5).get(2), "Row [5] should have null count when histogram is null");
  }

  private static void assertResultsMatch(List<ResultRow> results, int rowNum, String expectedProduct)
  {
    ResultRow row = results.get(rowNum);
    Object product = row.get(0);
    Assertions.assertTrue(product instanceof String, "Expected dimension of type String");
    Assertions.assertEquals(expectedProduct, product, "Product values didn't match");
    Object histogram = row.get(1);
    Assertions.assertTrue(
        histogram instanceof SpectatorHistogram,
        "Expected histogram metric of type SpectatorHistogramUtils.HistogramMap"
    );
    Assertions.assertEquals(EXPECTED_HISTOGRAMS.get(product), histogram, "Count values didn't match");
  }

  private static File newFolder(File root, String... subDirs) throws IOException
  {
    final String subFolder = String.join("/", subDirs);
    final File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }

}
