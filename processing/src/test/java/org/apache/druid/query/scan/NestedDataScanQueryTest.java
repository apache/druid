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

package org.apache.druid.query.scan;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.Druids;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class NestedDataScanQueryTest extends InitializedNullHandlingTest
{
  private static final Logger LOG = new Logger(NestedDataScanQueryTest.class);

  private final AggregationTestHelper helper;
  private final Closer closer;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @After
  public void teardown() throws IOException
  {
    closer.close();
  }

  public NestedDataScanQueryTest()
  {
    NestedDataModule.registerHandlersAndSerde();
    List<? extends Module> mods = NestedDataModule.getJacksonModulesList();
    this.helper = AggregationTestHelper.createScanQueryAggregationTestHelper(mods, tempFolder);
    this.closer = Closer.create();
  }

  @Test
  public void testIngestAndScanSegments() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn("nest", "$.x", "x"),
                                                 new NestedFieldVirtualColumn("nester", "$.x[0]", "x_0"),
                                                 new NestedFieldVirtualColumn("nester", "$.y.c[1]", "y_c_1")
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createSimpleNestedTestDataSegments(tempFolder, closer);

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  @Test
  public void testIngestAndScanSegmentsRollup() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn("nest", "$.long", "long")
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = ImmutableList.<Segment>builder().addAll(
        NestedDataTestUtils.createSegments(
            tempFolder,
            closer,
            NestedDataTestUtils.NUMERIC_DATA_FILE,
            NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
            NestedDataTestUtils.TIMESTAMP_SPEC,
            NestedDataTestUtils.AUTO_DISCOVERY,
            TransformSpec.NONE,
            NestedDataTestUtils.COUNT,
            Granularities.YEAR,
            true,
            IndexSpec.DEFAULT
        )
    ).build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    logResults(results);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(6, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestAndScanSegmentsRealtime() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn("nest", "$.x", "x"),
                                                 new NestedFieldVirtualColumn("nester", "$.x[0]", "x_0"),
                                                 new NestedFieldVirtualColumn("nester", "$.y.c[1]", "y_c_1"),
                                                 new NestedFieldVirtualColumn("nester", "$.", "nester_root"),
                                                 new NestedFieldVirtualColumn("dim", "$", "dim_root"),
                                                 new NestedFieldVirtualColumn("dim", "$.x", "dim_path"),
                                                 new NestedFieldVirtualColumn("count", "$", "count_root"),
                                                 new NestedFieldVirtualColumn("count", "$.x", "count_path")
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createSimpleNestedTestDataIncrementalIndex(tempFolder)
    );
    List<Segment> segs = NestedDataTestUtils.createSimpleNestedTestDataSegments(tempFolder, closer);


    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    if (NullHandling.sqlCompatible()) {
      Assert.assertEquals(resultsSegments.get(0).getEvents().toString(), resultsRealtime.get(0).getEvents().toString());
    }
  }

  @Test
  public void testIngestAndScanSegmentsRealtimeWithFallback() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .columns("x", "x_0", "y_c_1")
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn(
                                                     "nest",
                                                     "x",
                                                     ColumnType.LONG,
                                                     null,
                                                     true,
                                                     "$.x",
                                                     false
                                                 ),
                                                 new NestedFieldVirtualColumn(
                                                     "nester",
                                                     "x_0",
                                                     ColumnType.NESTED_DATA,
                                                     null,
                                                     true,
                                                     "$.x[0]",
                                                     false
                                                 ),
                                                 new NestedFieldVirtualColumn(
                                                     "nester",
                                                     "y_c_1",
                                                     ColumnType.NESTED_DATA,
                                                     null,
                                                     true,
                                                     "$.y.c[1]",
                                                     false
                                                 ),
                                                 new NestedFieldVirtualColumn(
                                                     "nester",
                                                     "nester_root",
                                                     ColumnType.NESTED_DATA,
                                                     null,
                                                     true,
                                                     "$.",
                                                     false
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createSimpleNestedTestDataIncrementalIndex(tempFolder)
    );
    List<Segment> segs = NestedDataTestUtils.createSimpleNestedTestDataSegments(tempFolder, closer);


    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    Assert.assertEquals(resultsSegments.get(0).getEvents().toString(), resultsRealtime.get(0).getEvents().toString());
  }

  @Test
  public void testIngestAndScanSegmentsTsvV4() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn("nest", "$.x", "x"),
                                                 new NestedFieldVirtualColumn("nester", "$.x[0]", "x_0"),
                                                 new NestedFieldVirtualColumn("nester", "$.y.c[1]", "y_c_1")
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createSimpleSegmentsTsvV4(tempFolder, closer);

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }


  @Test
  public void testIngestAndScanSegmentsTsv() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn("nest", "$.x", "x"),
                                                 new NestedFieldVirtualColumn("nester", "$.x[0]", "x_0"),
                                                 new NestedFieldVirtualColumn("nester", "$.y.c[1]", "y_c_1")
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createSimpleSegmentsTsv(tempFolder, closer);

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  @Test
  public void testIngestWithMergesAndScanSegments() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createSegmentsForJsonInput(
        tempFolder,
        closer,
        NestedDataTestUtils.SIMPLE_DATA_FILE,
        Granularities.HOUR,
        true,
        IndexSpec.DEFAULT
    );
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  @Test
  public void testIngestWithMoreMergesAndScanSegments() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();


    List<Segment> segs = NestedDataTestUtils.createSegmentsWithConcatenatedJsonInput(
        tempFolder,
        closer,
        NestedDataTestUtils.SIMPLE_DATA_FILE,
        Granularities.HOUR,
        false,
        10,
        1
    );
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    logResults(results);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(80, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestWithMoreMergesAndScanSegmentsRollup() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();


    // same rows over and over so expect same 8 rows after rollup
    List<Segment> segs = NestedDataTestUtils.createSegmentsWithConcatenatedJsonInput(
        tempFolder,
        closer,
        NestedDataTestUtils.SIMPLE_DATA_FILE,
        Granularities.YEAR,
        true,
        100,
        1
    );
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  @Test
  public void testIngestAndScanSegmentsAndFilter() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn("nest", "$.x", "x")
                                             )
                                             .filters(
                                                 new SelectorDimFilter("x", "200", null)
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createSimpleNestedTestDataSegments(tempFolder, closer);

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    logResults(results);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(1, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestAndScanSegmentsAndRangeFilter() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn("nest", "$.x", "x")
                                             )
                                             .filters(
                                                 new BoundDimFilter(
                                                     "x",
                                                     "100",
                                                     "300",
                                                     false,
                                                     false,
                                                     null,
                                                     null,
                                                     StringComparators.LEXICOGRAPHIC
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createSimpleNestedTestDataSegments(tempFolder, closer);

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    logResults(results);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(4, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestAndScanSegmentsRealtimeSchemaDiscovery() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createIncrementalIndex(
            tempFolder,
            NestedDataTestUtils.TYPES_DATA_FILE,
            NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
            NestedDataTestUtils.TIMESTAMP_SPEC,
            NestedDataTestUtils.AUTO_DISCOVERY,
            TransformSpec.NONE,
            NestedDataTestUtils.COUNT,
            Granularities.DAY,
            true
        )
    );
    List<Segment> segs = NestedDataTestUtils.createSegments(
        tempFolder,
        closer,
        NestedDataTestUtils.TYPES_DATA_FILE,
        NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
        NestedDataTestUtils.TIMESTAMP_SPEC,
        NestedDataTestUtils.AUTO_DISCOVERY,
        TransformSpec.NONE,
        NestedDataTestUtils.COUNT,
        Granularities.DAY,
        true,
        IndexSpec.DEFAULT
    );


    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    Assert.assertEquals(resultsRealtime.get(0).getEvents().toString(), resultsSegments.get(0).getEvents().toString());
  }

  @Test
  public void testIngestAndScanSegmentsRealtimeAutoExplicit() throws Exception
  {
    DimensionsSpec spec = DimensionsSpec.builder()
                                        .setDimensions(
                                            ImmutableList.of(
                                                new AutoTypeColumnSchema("str", ColumnType.STRING),
                                                new AutoTypeColumnSchema("long", ColumnType.LONG),
                                                new AutoTypeColumnSchema("double", ColumnType.FLOAT)
                                            )
                                        )
                                        .build();
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createIncrementalIndex(
            tempFolder,
            NestedDataTestUtils.TYPES_DATA_FILE,
            NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
            NestedDataTestUtils.TIMESTAMP_SPEC,
            spec,
            TransformSpec.NONE,
            NestedDataTestUtils.COUNT,
            Granularities.DAY,
            true
        )
    );
    List<Segment> segs = NestedDataTestUtils.createSegments(
        tempFolder,
        closer,
        NestedDataTestUtils.TYPES_DATA_FILE,
        NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
        NestedDataTestUtils.TIMESTAMP_SPEC,
        spec,
        TransformSpec.NONE,
        NestedDataTestUtils.COUNT,
        Granularities.DAY,
        true,
        IndexSpec.DEFAULT
    );


    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    Assert.assertEquals(resultsRealtime.get(0).getEvents().toString(), resultsSegments.get(0).getEvents().toString());
  }

  @Test
  public void testIngestAndScanSegmentsRealtimeSchemaDiscoveryArrayTypes() throws Exception
  {
    Druids.ScanQueryBuilder builder = Druids.newScanQueryBuilder()
                                            .dataSource("test_datasource")
                                            .intervals(
                                                new MultipleIntervalSegmentSpec(
                                                    Collections.singletonList(Intervals.ETERNITY)
                                                )
                                            )
                                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                            .limit(100)
                                            .context(ImmutableMap.of());

    Query<ScanResultValue> scanQuery = builder.build();
    final AggregatorFactory[] aggs = new AggregatorFactory[]{new CountAggregatorFactory("count")};
    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createIncrementalIndex(
            tempFolder,
            NestedDataTestUtils.ARRAY_TYPES_DATA_FILE,
            NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
            NestedDataTestUtils.TIMESTAMP_SPEC,
            NestedDataTestUtils.AUTO_DISCOVERY,
            TransformSpec.NONE,
            aggs,
            Granularities.NONE,
            true
        )
    );
    List<Segment> segs = NestedDataTestUtils.createSegments(
        tempFolder,
        closer,
        NestedDataTestUtils.ARRAY_TYPES_DATA_FILE,
        NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
        NestedDataTestUtils.TIMESTAMP_SPEC,
        NestedDataTestUtils.AUTO_DISCOVERY,
        TransformSpec.NONE,
        aggs,
        Granularities.NONE,
        true,
        IndexSpec.DEFAULT
    );


    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    Assert.assertEquals(resultsSegments.get(0).getEvents().toString(), resultsRealtime.get(0).getEvents().toString());
  }

  @Test
  public void testIngestAndScanSegmentsRealtimeSchemaDiscoveryMoreArrayTypesNonStrictBooleans() throws Exception
  {

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(false);
      Druids.ScanQueryBuilder builder = Druids.newScanQueryBuilder()
                                              .dataSource("test_datasource")
                                              .intervals(
                                                  new MultipleIntervalSegmentSpec(
                                                      Collections.singletonList(Intervals.ETERNITY)
                                                  )
                                              )
                                              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                              .limit(100)
                                              .context(ImmutableMap.of());
      Query<ScanResultValue> scanQuery = builder.build();
      final AggregatorFactory[] aggs = new AggregatorFactory[]{new CountAggregatorFactory("count")};
      List<Segment> realtimeSegs = ImmutableList.of(
          NestedDataTestUtils.createIncrementalIndex(
              tempFolder,
              NestedDataTestUtils.ARRAY_TYPES_DATA_FILE_2,
              NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
              NestedDataTestUtils.TIMESTAMP_SPEC,
              NestedDataTestUtils.AUTO_DISCOVERY,
              TransformSpec.NONE,
              aggs,
              Granularities.NONE,
              true
          )
      );
      List<Segment> segs = NestedDataTestUtils.createSegments(
          tempFolder,
          closer,
          NestedDataTestUtils.ARRAY_TYPES_DATA_FILE_2,
          NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
          NestedDataTestUtils.TIMESTAMP_SPEC,
          NestedDataTestUtils.AUTO_DISCOVERY,
          TransformSpec.NONE,
          aggs,
          Granularities.NONE,
          true,
          IndexSpec.DEFAULT
      );


      final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
      final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

      List<ScanResultValue> resultsRealtime = seq.toList();
      List<ScanResultValue> resultsSegments = seq2.toList();
      logResults(resultsSegments);
      logResults(resultsRealtime);
      Assert.assertEquals(1, resultsRealtime.size());
      Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
      Assert.assertEquals(
          "["
          + "[978652800000, [A, A], [null, null], [1, 1], [0.1, 0.1], [true, true], [null, null], {s_str1=[A, A], s_str2=[null, null], s_num_int=[1, 1], s_num_float=[0.1, 0.1], s_bool=[true, true], s_null=[null, null]}, 1], "
          + "[978739200000, [A, A], [null, null], [1, 1], [0.1, 0.1], [true, true], [null, null], {s_str1=[A, A], s_str2=[null, null], s_num_int=[1, 1], s_num_float=[0.1, 0.1], s_bool=[true, true], s_null=[null, null]}, 1], "
          + "[978825600000, [A, A], [null, null], [1, 1], [0.1, 0.1], [true, true], [null, null], {s_str1=[A, A], s_str2=[null, null], s_num_int=[1, 1], s_num_float=[0.1, 0.1], s_bool=[true, true], s_null=[null, null]}, 1], "
          + "[978912000000, [A, A], [null, null], [1, 1], [0.1, 0.1], [true, true], [null, null], {s_str1=[A, A], s_str2=[null, null], s_num_int=[1, 1], s_num_float=[0.1, 0.1], s_bool=[true, true], s_null=[null, null]}, 1]]",
          resultsSegments.get(0).getEvents().toString()
      );
      Assert.assertEquals(resultsSegments.get(0).getEvents().toString(), resultsRealtime.get(0).getEvents().toString());
    }
    finally {
      ExpressionProcessing.initializeForTests();
    }
  }

  @Test
  public void testIngestAndScanSegmentsRealtimeSchemaDiscoveryMoreArrayTypesStrictBooleans() throws Exception
  {
    Druids.ScanQueryBuilder builder = Druids.newScanQueryBuilder()
                                            .dataSource("test_datasource")
                                            .intervals(
                                                new MultipleIntervalSegmentSpec(
                                                    Collections.singletonList(Intervals.ETERNITY)
                                                )
                                            )
                                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                            .limit(100)
                                            .context(ImmutableMap.of());
    Query<ScanResultValue> scanQuery = builder.build();
    final AggregatorFactory[] aggs = new AggregatorFactory[]{new CountAggregatorFactory("count")};
    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createIncrementalIndex(
            tempFolder,
            NestedDataTestUtils.ARRAY_TYPES_DATA_FILE_2,
            NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
            NestedDataTestUtils.TIMESTAMP_SPEC,
            NestedDataTestUtils.AUTO_DISCOVERY,
            TransformSpec.NONE,
            aggs,
            Granularities.NONE,
            true
        )
    );
    List<Segment> segs = NestedDataTestUtils.createSegments(
        tempFolder,
        closer,
        NestedDataTestUtils.ARRAY_TYPES_DATA_FILE_2,
        NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
        NestedDataTestUtils.TIMESTAMP_SPEC,
        NestedDataTestUtils.AUTO_DISCOVERY,
        TransformSpec.NONE,
        aggs,
        Granularities.NONE,
        true,
        IndexSpec.DEFAULT
    );


    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    Assert.assertEquals(
        "["
        + "[978652800000, [A, A], [null, null], [1, 1], [0.1, 0.1], [1, 1], [null, null], {s_str1=[A, A], s_str2=[null, null], s_num_int=[1, 1], s_num_float=[0.1, 0.1], s_bool=[true, true], s_null=[null, null]}, 1], "
        + "[978739200000, [A, A], [null, null], [1, 1], [0.1, 0.1], [1, 1], [null, null], {s_str1=[A, A], s_str2=[null, null], s_num_int=[1, 1], s_num_float=[0.1, 0.1], s_bool=[true, true], s_null=[null, null]}, 1], "
        + "[978825600000, [A, A], [null, null], [1, 1], [0.1, 0.1], [1, 1], [null, null], {s_str1=[A, A], s_str2=[null, null], s_num_int=[1, 1], s_num_float=[0.1, 0.1], s_bool=[true, true], s_null=[null, null]}, 1], "
        + "[978912000000, [A, A], [null, null], [1, 1], [0.1, 0.1], [1, 1], [null, null], {s_str1=[A, A], s_str2=[null, null], s_num_int=[1, 1], s_num_float=[0.1, 0.1], s_bool=[true, true], s_null=[null, null]}, 1]]",
        resultsSegments.get(0).getEvents().toString()
    );
    Assert.assertEquals(resultsSegments.get(0).getEvents().toString(), resultsRealtime.get(0).getEvents().toString());
  }

  @Test
  public void testIngestAndScanSegmentsRealtimeSchemaDiscoveryTypeGauntlet() throws Exception
  {
    Druids.ScanQueryBuilder builder = Druids.newScanQueryBuilder()
                                            .dataSource("test_datasource")
                                            .intervals(
                                                new MultipleIntervalSegmentSpec(
                                                    Collections.singletonList(Intervals.ETERNITY)
                                                )
                                            )
                                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                            .limit(100)
                                            .context(ImmutableMap.of());
    Query<ScanResultValue> scanQuery = builder.build();
    final AggregatorFactory[] aggs = new AggregatorFactory[]{new CountAggregatorFactory("count")};
    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createIncrementalIndex(
            tempFolder,
            NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE,
            NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
            NestedDataTestUtils.TIMESTAMP_SPEC,
            NestedDataTestUtils.AUTO_DISCOVERY,
            TransformSpec.NONE,
            aggs,
            Granularities.NONE,
            true
        )
    );
    List<Segment> segs = NestedDataTestUtils.createSegments(
        tempFolder,
        closer,
        NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE,
        NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
        NestedDataTestUtils.TIMESTAMP_SPEC,
        NestedDataTestUtils.AUTO_DISCOVERY,
        TransformSpec.NONE,
        aggs,
        Granularities.NONE,
        true,
        IndexSpec.DEFAULT
    );


    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(
          "[[1672531200000, null, 0, 0.0, 1, 51, -0.13, 1, [], [51, -35], {a=700, b={x=g, y=1.1, z=[9, null, 9, 9]}, v=[]}, {x=400, y=[{l=[null], m=100, n=5}, {l=[a, b, c], m=a, n=1}], z={}}, null, [a, b], null, [2, 3], null, [null], null, [1, 0, 1], null, [{x=1}, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, null, 2, 0.0, 0, b, 1.1, b, 2, b, {a=200, b={x=b, y=1.1, z=[2, 4, 6]}, v=[]}, {x=10, y=[{l=[b, b, c], m=b, n=2}, [1, 2, 3]], z={a=[5.5], b=false}}, [a, b, c], [null, b], [2, 3], null, [3.3, 4.4, 5.5], [999.0, null, 5.5], [null, null, 2.2], [1, 1], [null, [null], []], [{x=3}, {x=4}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, a, 1, 1.0, 1, 1, 1, 1, 1, 1, {a=100, b={x=a, y=1.1, z=[1, 2, 3, 4]}, v=[]}, {x=1234, y=[{l=[a, b, c], m=a, n=1}, {l=[a, b, c], m=a, n=1}], z={a=[1.1, 2.2, 3.3], b=true}}, [a, b], [a, b], [1, 2, 3], [1, null, 3], [1.1, 2.2, 3.3], [1.1, 2.2, null], [a, 1, 2.2], [1, 0, 1], [[1, 2, null], [3, 4]], [{x=1}, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, b, 4, 3.3, 1, 1, 0.0, {}, 4, 1, {a=400, b={x=d, y=1.1, z=[3, 4]}, v=[]}, {x=1234, z={a=[1.1, 2.2, 3.3], b=true}}, [d, e], [b, b], [1, 4], [1], [2.2, 3.3, 4.0], null, [a, b, c], [null, 0, 1], [[1, 2], [3, 4], [5, 6, 7]], [{x=null}, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, c, 0, 4.4, 1, hello, -1000, {}, [], hello, {a=500, b={x=e, z=[1, 2, 3, 4]}, v=a}, {x=11, y=[], z={a=[null], b=false}}, null, null, [1, 2, 3], [], [1.1, 2.2, 3.3], null, null, [0], null, [{x=1000}, {y=2000}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, d, 5, 5.9, 0, null, 3.33, a, 6, null, {a=600, b={x=f, y=1.1, z=[6, 7, 8, 9]}, v=b}, null, [a, b], null, null, [null, 2, 9], null, [999.0, 5.5, null], [a, 1, 2.2], [], [[1], [1, 2, null]], [{a=1}, {b=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, null, 3, 2.0, 0, 3.0, 1.0, 3.3, 3, 3.0, {a=300}, {x=4.4, y=[{l=[], m=100, n=3}, {l=[a]}, {l=[b], n=[]}], z={a=[], b=true}}, [b, c], [d, null, b], [1, 2, 3, 4], [1, 2, 3], [1.1, 3.3], [null, 2.2, null], [1, null, 1], [1, null, 1], [[1], null, [1, 2, 3]], [null, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1]]",
          resultsSegments.get(0).getEvents().toString()
      );
    } else {
      Assert.assertEquals(
          "[[1672531200000, null, null, null, 1, 51, -0.13, 1, [], [51, -35], {a=700, b={x=g, y=1.1, z=[9, null, 9, 9]}, v=[]}, {x=400, y=[{l=[null], m=100, n=5}, {l=[a, b, c], m=a, n=1}], z={}}, null, [a, b], null, [2, 3], null, [null], null, [1, 0, 1], null, [{x=1}, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, , 2, null, 0, b, 1.1, b, 2, b, {a=200, b={x=b, y=1.1, z=[2, 4, 6]}, v=[]}, {x=10, y=[{l=[b, b, c], m=b, n=2}, [1, 2, 3]], z={a=[5.5], b=false}}, [a, b, c], [null, b], [2, 3], null, [3.3, 4.4, 5.5], [999.0, null, 5.5], [null, null, 2.2], [1, 1], [null, [null], []], [{x=3}, {x=4}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, a, 1, 1.0, 1, 1, 1, 1, 1, 1, {a=100, b={x=a, y=1.1, z=[1, 2, 3, 4]}, v=[]}, {x=1234, y=[{l=[a, b, c], m=a, n=1}, {l=[a, b, c], m=a, n=1}], z={a=[1.1, 2.2, 3.3], b=true}}, [a, b], [a, b], [1, 2, 3], [1, null, 3], [1.1, 2.2, 3.3], [1.1, 2.2, null], [a, 1, 2.2], [1, 0, 1], [[1, 2, null], [3, 4]], [{x=1}, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, b, 4, 3.3, 1, 1, null, {}, 4, 1, {a=400, b={x=d, y=1.1, z=[3, 4]}, v=[]}, {x=1234, z={a=[1.1, 2.2, 3.3], b=true}}, [d, e], [b, b], [1, 4], [1], [2.2, 3.3, 4.0], null, [a, b, c], [null, 0, 1], [[1, 2], [3, 4], [5, 6, 7]], [{x=null}, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, c, null, 4.4, 1, hello, -1000, {}, [], hello, {a=500, b={x=e, z=[1, 2, 3, 4]}, v=a}, {x=11, y=[], z={a=[null], b=false}}, null, null, [1, 2, 3], [], [1.1, 2.2, 3.3], null, null, [0], null, [{x=1000}, {y=2000}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, d, 5, 5.9, 0, null, 3.33, a, 6, null, {a=600, b={x=f, y=1.1, z=[6, 7, 8, 9]}, v=b}, null, [a, b], null, null, [null, 2, 9], null, [999.0, 5.5, null], [a, 1, 2.2], [], [[1], [1, 2, null]], [{a=1}, {b=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, null, 3, 2.0, null, 3.0, 1.0, 3.3, 3, 3.0, {a=300}, {x=4.4, y=[{l=[], m=100, n=3}, {l=[a]}, {l=[b], n=[]}], z={a=[], b=true}}, [b, c], [d, null, b], [1, 2, 3, 4], [1, 2, 3], [1.1, 3.3], [null, 2.2, null], [1, null, 1], [1, null, 1], [[1], null, [1, 2, 3]], [null, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1]]",
          resultsSegments.get(0).getEvents().toString()
      );
    }
    Assert.assertEquals(resultsSegments.get(0).getEvents().toString(), resultsRealtime.get(0).getEvents().toString());
  }

  @Test
  public void testIngestAndScanSegmentsAndFilterPartialPathArrayIndex() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .filters(
                                                 NotDimFilter.of(NullFilter.forColumn("v0"))
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn(
                                                     "complexObj",
                                                     "v0",
                                                     ColumnType.NESTED_DATA,
                                                     null,
                                                     true,
                                                     "$.y[0]",
                                                     false
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createSegmentsForJsonInput(
        tempFolder,
        closer,
        NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE,
        Granularities.HOUR,
        true,
        IndexSpec.DEFAULT
    );

    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createIncrementalIndexForJsonInput(
            tempFolder,
            NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE,
            Granularities.NONE,
            true
        )
    );

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);
    final Sequence<ScanResultValue> seqRealtime = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    List<ScanResultValue> results = seq.toList();
    List<ScanResultValue> resultsRealtime = seqRealtime.toList();
    logResults(results);
    logResults(resultsRealtime);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(4, ((List) results.get(0).getEvents()).size());
    Assert.assertEquals(results.size(), resultsRealtime.size());
    Assert.assertEquals(results.get(0).getEvents().toString(), resultsRealtime.get(0).getEvents().toString());
  }

  @Test
  public void testIngestAndScanSegmentsAndFilterPartialPath() throws Exception
  {
    Query<ScanResultValue> scanQuery = Druids.newScanQueryBuilder()
                                             .dataSource("test_datasource")
                                             .intervals(
                                                 new MultipleIntervalSegmentSpec(
                                                     Collections.singletonList(Intervals.ETERNITY)
                                                 )
                                             )
                                             .filters(
                                                 NotDimFilter.of(NullFilter.forColumn("v0"))
                                             )
                                             .virtualColumns(
                                                 new NestedFieldVirtualColumn(
                                                     "obj",
                                                     "v0",
                                                     ColumnType.NESTED_DATA,
                                                     null,
                                                     true,
                                                     "$.b",
                                                     false
                                                 )
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> segs = NestedDataTestUtils.createSegmentsForJsonInput(
        tempFolder,
        closer,
        NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE,
        Granularities.HOUR,
        true,
        IndexSpec.DEFAULT
    );

    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createIncrementalIndexForJsonInput(
            tempFolder,
            NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE,
            Granularities.NONE,
            true
        )
    );

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);
    final Sequence<ScanResultValue> seqRealtime = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    List<ScanResultValue> results = seq.toList();
    List<ScanResultValue> resultsRealtime = seqRealtime.toList();
    logResults(results);
    logResults(resultsRealtime);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(6, ((List) results.get(0).getEvents()).size());
    Assert.assertEquals(results.size(), resultsRealtime.size());
    Assert.assertEquals(results.get(0).getEvents().toString(), resultsRealtime.get(0).getEvents().toString());
  }

  @Test
  public void testIngestAndScanSegmentsNestedColumnNotNullFilter() throws Exception
  {
    Druids.ScanQueryBuilder builder = Druids.newScanQueryBuilder()
                                            .dataSource("test_datasource")
                                            .intervals(
                                                new MultipleIntervalSegmentSpec(
                                                    Collections.singletonList(Intervals.ETERNITY)
                                                )
                                            )
                                            .filters(NotDimFilter.of(NullFilter.forColumn("complexObj")))
                                            .columns("complexObj")
                                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                            .limit(100)
                                            .context(ImmutableMap.of());
    Query<ScanResultValue> scanQuery = builder.build();
    final AggregatorFactory[] aggs = new AggregatorFactory[]{new CountAggregatorFactory("count")};
    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createIncrementalIndex(
            tempFolder,
            NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE,
            NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
            NestedDataTestUtils.TIMESTAMP_SPEC,
            NestedDataTestUtils.AUTO_DISCOVERY,
            TransformSpec.NONE,
            aggs,
            Granularities.NONE,
            true
        )
    );
    List<Segment> segs = NestedDataTestUtils.createSegments(
        tempFolder,
        closer,
        NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE,
        NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
        NestedDataTestUtils.TIMESTAMP_SPEC,
        NestedDataTestUtils.AUTO_DISCOVERY,
        TransformSpec.NONE,
        aggs,
        Granularities.NONE,
        true,
        IndexSpec.DEFAULT
    );


    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(
          "[[{x=400, y=[{l=[null], m=100, n=5}, {l=[a, b, c], m=a, n=1}], z={}}], [{x=10, y=[{l=[b, b, c], m=b, n=2}, [1, 2, 3]], z={a=[5.5], b=false}}], [{x=1234, y=[{l=[a, b, c], m=a, n=1}, {l=[a, b, c], m=a, n=1}], z={a=[1.1, 2.2, 3.3], b=true}}], [{x=1234, z={a=[1.1, 2.2, 3.3], b=true}}], [{x=11, y=[], z={a=[null], b=false}}], [{x=4.4, y=[{l=[], m=100, n=3}, {l=[a]}, {l=[b], n=[]}], z={a=[], b=true}}]]",
          resultsSegments.get(0).getEvents().toString()
      );
    } else {
      Assert.assertEquals(
          "[[{x=400, y=[{l=[null], m=100, n=5}, {l=[a, b, c], m=a, n=1}], z={}}], [{x=10, y=[{l=[b, b, c], m=b, n=2}, [1, 2, 3]], z={a=[5.5], b=false}}], [{x=1234, y=[{l=[a, b, c], m=a, n=1}, {l=[a, b, c], m=a, n=1}], z={a=[1.1, 2.2, 3.3], b=true}}], [{x=1234, z={a=[1.1, 2.2, 3.3], b=true}}], [{x=11, y=[], z={a=[null], b=false}}], [{x=4.4, y=[{l=[], m=100, n=3}, {l=[a]}, {l=[b], n=[]}], z={a=[], b=true}}]]",
          resultsSegments.get(0).getEvents().toString()
      );
    }
    Assert.assertEquals(resultsSegments.get(0).getEvents().toString(), resultsRealtime.get(0).getEvents().toString());
  }

  private static void logResults(List<ScanResultValue> results)
  {
    StringBuilder bob = new StringBuilder();
    int ctr = 0;
    for (Object event : (List) results.get(0).getEvents()) {
      bob.append("row:").append(++ctr).append(" - ").append(event).append("\n");
    }
    LOG.info("results:\n%s", bob);
  }
}
