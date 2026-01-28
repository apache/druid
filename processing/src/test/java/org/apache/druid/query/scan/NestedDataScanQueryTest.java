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
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Druids;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.DefaultColumnFormatConfig;
import org.apache.druid.segment.NestedDataColumnSchema;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BitmapIndexType;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.NestedCommonFormatColumnFormatSpec;
import org.apache.druid.segment.nested.ObjectStorageEncoding;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(JUnitParamsRunner.class)
public class NestedDataScanQueryTest extends InitializedNullHandlingTest
{
  private static final Logger LOG = new Logger(NestedDataScanQueryTest.class);

  DefaultColumnFormatConfig DEFAULT_FORMAT = new DefaultColumnFormatConfig(null, null, null);

  private final AggregationTestHelper helper;
  private final Closer closer;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public static Object[] getNestedColumnFormatSpec()
  {
    List<Object> specs = new ArrayList<>();
    NestedCommonFormatColumnFormatSpec.Builder builder = NestedCommonFormatColumnFormatSpec.builder();
    for (boolean auto : new boolean[]{true, false}) {
      for (ObjectStorageEncoding objectStorage : ObjectStorageEncoding.values()) {
        for (BitmapIndexType bitmapIndex : new BitmapIndexType[]{
            BitmapIndexType.DictionaryEncodedValueIndex.INSTANCE,
            BitmapIndexType.NullValueIndex.INSTANCE
        }) {
          specs.add(
              new Object[]{
                  StringUtils.format(
                      "auto[%b], ObjectStorageEncoding[%s], BitmapIndexType[%s]",
                      auto,
                      objectStorage,
                      bitmapIndex
                  ),
                  auto,
                  builder.setObjectStorageEncoding(objectStorage)
                         .setLongFieldBitmapIndexType(bitmapIndex)
                         .setDoubleFieldBitmapIndexType(bitmapIndex).build()
              });
        }
      }
    }
    return specs.toArray();
  }

  @After
  public void teardown() throws IOException
  {
    closer.close();
  }

  public NestedDataScanQueryTest()
  {
    BuiltInTypesModule.registerHandlersAndSerde();
    List<? extends Module> mods = BuiltInTypesModule.getJacksonModulesList();
    this.helper = AggregationTestHelper.createScanQueryAggregationTestHelper(mods, tempFolder);
    this.closer = Closer.create();
  }

  @Test
  public void testIngestAndScanSegmentsSimple() throws Exception
  {
    // Dimension schema auto discovery
    Query<ScanResultValue> scanQuery = queryBuilder()
        .virtualColumns(
            new NestedFieldVirtualColumn("nest", "$.x", "x"),
            new NestedFieldVirtualColumn("nester", "$.x[0]", "x_0"),
            new NestedFieldVirtualColumn("nester", "$.y.c[1]", "y_c_1")
        ).build();
    List<Segment> segs = new NestedDataTestUtils.ResourceFileSegmentBuilder(tempFolder, closer).build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  @Test
  @Parameters(method = "getNestedColumnFormatSpec")
  @TestCaseName("{0}")
  public void testIngestAndScanSegmentsWithSpec(String name, boolean auto, NestedCommonFormatColumnFormatSpec spec)
      throws Exception
  {
    // Test with different column format spec
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().setDimensions(
        Stream.of("dim", "nest", "nester", "variant", "list")
              .map(col -> auto
                          ? new AutoTypeColumnSchema(col, null, spec)
                          : new NestedDataColumnSchema(col, 5, spec, DEFAULT_FORMAT))
              .collect(Collectors.toList())).build();
    Query<ScanResultValue> scanQuery = queryBuilder()
        .virtualColumns(
            new NestedFieldVirtualColumn("nest", "$.x", "x"),
            new NestedFieldVirtualColumn("nester", "$.x[0]", "x_0"),
            new NestedFieldVirtualColumn("nester", "$.y.c[1]", "y_c_1")
        )
        .context(ImmutableMap.of())
        .build();
    List<Segment> segs = new NestedDataTestUtils.ResourceFileSegmentBuilder(tempFolder, closer).dimensionsSpec(
                                                                                                   dimensionsSpec)
                                                                                               .build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  @Test
  @Parameters(method = "getNestedColumnFormatSpec")
  @TestCaseName("{0}")
  public void testIngestAndScanSegmentsNumericWithSpec(
      String name,
      boolean auto,
      NestedCommonFormatColumnFormatSpec spec
  ) throws Exception
  {
    // Test with different column format spec
    DimensionsSpec dimensionsSpec =
        DimensionsSpec.builder()
                      .setDimensions(List.of(
                          new LongDimensionSchema("__time"),
                          auto
                          ? new AutoTypeColumnSchema("nest", null, spec)
                          : new NestedDataColumnSchema("nest", 5, spec, DEFAULT_FORMAT)
                      ))
                      .build();
    Query<ScanResultValue> scanQuery = queryBuilder()
        .virtualColumns(new NestedFieldVirtualColumn("nest", "$.long", "long"))
        .build();

    NestedDataTestUtils.ResourceFileSegmentBuilder builder =
        new NestedDataTestUtils.ResourceFileSegmentBuilder(
            tempFolder,
            closer
        ).input(NestedDataTestUtils.NUMERIC_DATA_FILE)
         .dimensionsSpec(dimensionsSpec)
         .granularity(Granularities.YEAR);
    List<Segment> realtimeSegs = List.of(builder.buildIncremental());
    List<Segment> segs = builder.build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    if (ObjectStorageEncoding.NONE.equals(spec.getObjectStorageEncoding())) {
      // use StructuredData here because it sorts the fields in nest column
      Assert.assertEquals(
          StructuredData.wrap(resultsRealtime.get(0).getEvents()),
          StructuredData.wrap(resultsSegments.get(0).getEvents())
      );
    } else {
      Assert.assertEquals(resultsRealtime.get(0).getEvents().toString(), resultsSegments.get(0).getEvents().toString());
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void testIngestAndScanSegmentsNumericRollup(boolean rollup) throws Exception
  {
    Query<ScanResultValue> scanQuery = queryBuilder()
        .virtualColumns(new NestedFieldVirtualColumn("nest", "$.long", "long"))
        .build();
    List<Segment> segs =
        new NestedDataTestUtils.ResourceFileSegmentBuilder(
            tempFolder,
            closer
        ).input(NestedDataTestUtils.NUMERIC_DATA_FILE)
         .rollup(rollup)
         .granularity(Granularities.YEAR)
         .build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    logResults(results);
    Assert.assertEquals(1, results.size());
    if (rollup) {
      Assert.assertEquals(6, ((List) results.get(0).getEvents()).size());
    } else {
      Assert.assertEquals(10, ((List) results.get(0).getEvents()).size());
    }
  }

  @Test
  public void testIngestAndScanSegmentsRealtime() throws Exception
  {
    Query<ScanResultValue> scanQuery = queryBuilder()
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
        .build();
    NestedDataTestUtils.ResourceFileSegmentBuilder builder = new NestedDataTestUtils.ResourceFileSegmentBuilder(
        tempFolder,
        closer
    );
    List<Segment> realtimeSegs = List.of(builder.buildIncremental());
    List<Segment> segs = builder.build();

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
  public void testIngestAndScanSegmentsRealtimeWithFallback() throws Exception
  {
    VirtualColumns virtualColumns = VirtualColumns.create(
        new NestedFieldVirtualColumn("nest", "x", ColumnType.LONG, null, true, "$.x", false),
        new NestedFieldVirtualColumn("nester", "x_0", ColumnType.NESTED_DATA, null, true, "$.x[0]", false),
        new NestedFieldVirtualColumn("nester", "y_c_1", ColumnType.NESTED_DATA, null, true, "$.y.c[1]", false),
        new NestedFieldVirtualColumn("nester", "nester_root", ColumnType.NESTED_DATA, null, true, "$.", false)
    );
    Query<ScanResultValue> scanQuery = queryBuilder().columns("x", "x_0", "y_c_1")
                                                     .virtualColumns(virtualColumns)
                                                     .build();
    NestedDataTestUtils.ResourceFileSegmentBuilder builder = new NestedDataTestUtils.ResourceFileSegmentBuilder(
        tempFolder,
        closer
    );
    List<Segment> realtimeSegs = ImmutableList.of(builder.buildIncremental());
    List<Segment> segs = builder.build();

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
  @Parameters(method = "getNestedColumnFormatSpec")
  @TestCaseName("{0}")
  public void testIngestAndScanSegmentsTsv(String name, boolean auto, NestedCommonFormatColumnFormatSpec spec)
      throws Exception
  {
    // Test with different column format spec
    List<DimensionSchema> dimensionsSpec = NestedDataTestUtils.SIMPLE_DATA_TSV_COLUMN_NAMES
        .stream()
        .map(col -> auto
                    ? new AutoTypeColumnSchema(col, null, spec)
                    : new NestedDataColumnSchema(col, 5, spec, DEFAULT_FORMAT))
        .collect(Collectors.toList());
    Query<ScanResultValue> scanQuery = queryBuilder()
        .virtualColumns(
            new NestedFieldVirtualColumn("nest", "$.x", "x"),
            new NestedFieldVirtualColumn("nester", "$.x[0]", "x_0"),
            new NestedFieldVirtualColumn("nester", "$.y.c[1]", "y_c_1")
        )
        .build();

    List<Segment> segments = new NestedDataTestUtils.ResourceFileSegmentBuilder(tempFolder, closer)
        .input(NestedDataTestUtils.SIMPLE_DATA_TSV_FILE)
        .inputFormat(NestedDataTestUtils.SIMPLE_DATA_TSV_INPUT_FORMAT)
        .dimensionsSpec(DimensionsSpec.builder().setDimensions(dimensionsSpec).build())
        .transformSpec(NestedDataTestUtils.SIMPLE_DATA_TSV_TRANSFORM)
        .build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segments, scanQuery);

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  @Test
  public void testIngestWithMoreMergesAndScanSegments() throws Exception
  {
    List<Segment> segs = NestedDataTestUtils.createSegmentsWithConcatenatedJsonInput(
        tempFolder,
        closer,
        NestedDataTestUtils.SIMPLE_DATA_FILE,
        Granularities.HOUR,
        false,
        10,
        1
    );
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, queryBuilder().build());

    List<ScanResultValue> results = seq.toList();
    logResults(results);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(80, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestWithMoreMergesAndScanSegmentsRollup() throws Exception
  {
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
    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, queryBuilder().build());

    List<ScanResultValue> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(8, ((List) results.get(0).getEvents()).size());
    logResults(results);
  }

  @Test
  @Parameters(method = "getNestedColumnFormatSpec")
  @TestCaseName("{0}")
  public void testIngestAndScanSegmentsAndFilter(String name, boolean auto, NestedCommonFormatColumnFormatSpec spec)
      throws Exception
  {
    // Test with different column format spec
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().setDimensions(
        Stream.of("dim", "nest", "nester", "variant", "list")
              .map(col -> auto
                          ? new AutoTypeColumnSchema(col, null, spec)
                          : new NestedDataColumnSchema(col, 5, spec, DEFAULT_FORMAT))
              .collect(Collectors.toList())).build();
    Query<ScanResultValue> scanQuery = queryBuilder()
        .virtualColumns(new NestedFieldVirtualColumn("nest", "$.x", "x"))
        .filters(new SelectorDimFilter("x", "200", null))
        .build();
    List<Segment> segs = new NestedDataTestUtils.ResourceFileSegmentBuilder(tempFolder, closer).dimensionsSpec(
                                                                                                   dimensionsSpec)
                                                                                               .build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    logResults(results);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(1, ((List) results.get(0).getEvents()).size());
  }

  @Test
  @Parameters(method = "getNestedColumnFormatSpec")
  @TestCaseName("{0}")
  public void testIngestAndScanSegmentsAndRangeFilter(
      String name,
      boolean auto,
      NestedCommonFormatColumnFormatSpec spec
  ) throws Exception
  {
    // Test with different column format spec
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder().setDimensions(
        Stream.of("dim", "nest", "nester", "variant", "list")
              .map(col -> auto
                          ? new AutoTypeColumnSchema(col, null, spec)
                          : new NestedDataColumnSchema(col, 5, spec, DEFAULT_FORMAT))
              .collect(Collectors.toList())).build();
    Query<ScanResultValue> scanQuery = queryBuilder()
        .virtualColumns(new NestedFieldVirtualColumn("nest", "$.x", "x"))
        .filters(new BoundDimFilter("x", "100", "300", false, false, null, null, StringComparators.LEXICOGRAPHIC))
        .build();
    List<Segment> segs = new NestedDataTestUtils.ResourceFileSegmentBuilder(tempFolder, closer).dimensionsSpec(
                                                                                                   dimensionsSpec)
                                                                                               .build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    logResults(results);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(4, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testIngestAndScanSegmentsRealtimeSchemaDiscovery() throws Exception
  {
    NestedDataTestUtils.ResourceFileSegmentBuilder segmentBuilder =
        new NestedDataTestUtils.ResourceFileSegmentBuilder(
            tempFolder,
            closer
        ).input(NestedDataTestUtils.TYPES_DATA_FILE)
         .granularity(Granularities.DAY);
    List<Segment> realtimeSegs = List.of(segmentBuilder.buildIncremental());
    List<Segment> segs = segmentBuilder.build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, queryBuilder().build());
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, queryBuilder().build());

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    Assert.assertEquals(resultsRealtime.get(0).getEvents().toString(), resultsSegments.get(0).getEvents().toString());
  }

  @Test
  @Parameters(method = "getNestedColumnFormatSpec")
  @TestCaseName("{0}")
  public void testIngestAndScanSegmentsRealtimeAutoExplicit(
      String name,
      boolean auto,
      NestedCommonFormatColumnFormatSpec spec
  ) throws Exception
  {
    // Test with different column format spec
    Assume.assumeTrue(auto);
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder()
                                                  .setDimensions(
                                                      List.of(
                                                          new AutoTypeColumnSchema("str", ColumnType.STRING, spec),
                                                          new AutoTypeColumnSchema("long", ColumnType.LONG, spec),
                                                          new AutoTypeColumnSchema("double", ColumnType.FLOAT, spec)
                                                      )
                                                  )
                                                  .build();
    NestedDataTestUtils.ResourceFileSegmentBuilder builder =
        new NestedDataTestUtils.ResourceFileSegmentBuilder(
            tempFolder,
            closer
        ).input(NestedDataTestUtils.TYPES_DATA_FILE)
         .dimensionsSpec(dimensionsSpec)
         .granularity(Granularities.DAY);
    List<Segment> realtimeSegs = List.of(builder.buildIncremental());
    List<Segment> segs = builder.build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, queryBuilder().build());
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, queryBuilder().build());

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
    NestedDataTestUtils.ResourceFileSegmentBuilder builder =
        new NestedDataTestUtils.ResourceFileSegmentBuilder(
            tempFolder,
            closer
        ).input(NestedDataTestUtils.ARRAY_TYPES_DATA_FILE);
    List<Segment> realtimeSegs = List.of(builder.buildIncremental());
    List<Segment> segs = builder.build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, queryBuilder().build());
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, queryBuilder().build());

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    Assert.assertEquals(resultsRealtime.get(0).getEvents().toString(), resultsSegments.get(0).getEvents().toString());
  }

  @Test
  public void testIngestAndScanSegmentsRealtimeSchemaDiscoveryMoreArrayTypes() throws Exception
  {
    NestedDataTestUtils.ResourceFileSegmentBuilder builder =
        new NestedDataTestUtils.ResourceFileSegmentBuilder(
            tempFolder,
            closer
        ).input(NestedDataTestUtils.ARRAY_TYPES_DATA_FILE_2);
    List<Segment> realtimeSegs = List.of(builder.buildIncremental());
    List<Segment> segs = builder.build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, queryBuilder().build());
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, queryBuilder().build());

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
    Assert.assertEquals(resultsRealtime.get(0).getEvents().toString(), resultsSegments.get(0).getEvents().toString());
  }

  @Test
  public void testIngestAndScanSegmentsRealtimeSchemaDiscoveryTypeGauntlet() throws Exception
  {
    NestedDataTestUtils.ResourceFileSegmentBuilder builder =
        new NestedDataTestUtils.ResourceFileSegmentBuilder(
            tempFolder,
            closer
        ).input(NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE);

    List<Segment> realtimeSegs = List.of(builder.buildIncremental());
    List<Segment> segs = builder.build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, queryBuilder().build());
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, queryBuilder().build());

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    Assert.assertEquals(
        "[[1672531200000, null, null, null, 1, 51, -0.13, 1, [], [51, -35], {a=700, b={x=g, y=1.1, z=[9, null, 9, 9]}, c=null, v=[]}, {x=400, y=[{l=[null], m=100, n=5}, {l=[a, b, c], m=a, n=1}], z={}}, null, [a, b], null, [2, 3], null, [null], null, [1, 0, 1], null, [{x=1}, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, , 2, null, 0, b, 1.1, b, 2, b, {a=200, b={x=b, y=1.1, z=[2, 4, 6]}, c=[a, 123], v=[]}, {x=10, y=[{l=[b, b, c], m=b, n=2}, [1, 2, 3]], z={a=[5.5], b=false}}, [a, b, c], [null, b], [2, 3], null, [3.3, 4.4, 5.5], [999.0, null, 5.5], [null, null, 2.2], [1, 1], [null, [null], []], [{x=3}, {x=4}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, a, 1, 1.0, 1, 1, 1, 1, 1, 1, {a=100, b={x=a, y=1.1, z=[1, 2, 3, 4]}, c=[100], v=[]}, {x=1234, y=[{l=[a, b, c], m=a, n=1}, {l=[a, b, c], m=a, n=1}], z={a=[1.1, 2.2, 3.3], b=true}}, [a, b], [a, b], [1, 2, 3], [1, null, 3], [1.1, 2.2, 3.3], [1.1, 2.2, null], [a, 1, 2.2], [1, 0, 1], [[1, 2, null], [3, 4]], [{x=1}, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, b, 4, 3.3, 1, 1, null, {}, 4, 1, {a=400, b={x=d, y=1.1, z=[3, 4]}, c={a=1}, v=[]}, {x=1234, z={a=[1.1, 2.2, 3.3], b=true}}, [d, e], [b, b], [1, 4], [1], [2.2, 3.3, 4.0], null, [a, b, c], [null, 0, 1], [[1, 2], [3, 4], [5, 6, 7]], [{x=null}, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, c, null, 4.4, 1, hello, -1000, {}, [], hello, {a=500, b={x=e, z=[1, 2, 3, 4]}, c=hello, v=a}, {x=11, y=[], z={a=[null], b=false}}, null, null, [1, 2, 3], [], [1.1, 2.2, 3.3], null, null, [0], null, [{x=1000}, {y=2000}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, d, 5, 5.9, 0, null, 3.33, a, 6, null, {a=600, b={x=f, y=1.1, z=[6, 7, 8, 9]}, c=12.3, v=b}, null, [a, b], null, null, [null, 2, 9], null, [999.0, 5.5, null], [a, 1, 2.2], [], [[1], [1, 2, null]], [{a=1}, {b=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1], [1672531200000, null, 3, 2.0, null, 3.0, 1.0, 3.3, 3, 3.0, {a=300}, {x=4.4, y=[{l=[], m=100, n=3}, {l=[a]}, {l=[b], n=[]}], z={a=[], b=true}}, [b, c], [d, null, b], [1, 2, 3, 4], [1, 2, 3], [1.1, 3.3], [null, 2.2, null], [1, null, 1], [1, null, 1], [[1], null, [1, 2, 3]], [null, {x=2}], null, hello, 1234, 1.234, {x=1, y=hello, z={a=1.1, b=1234, c=[a, b, c], d=[]}}, [a, b, c], [1, 2, 3], [1.1, 2.2, 3.3], [], {}, [null, null], [{}, {}, {}], [{a=b, x=1, y=1.3}], 1]]",
        resultsSegments.get(0).getEvents().toString()
    );
    Assert.assertEquals(resultsRealtime.get(0).getEvents().toString(), resultsSegments.get(0).getEvents().toString());
  }

  @Test
  @Parameters(method = "getNestedColumnFormatSpec")
  @TestCaseName("{0}")
  public void testIngestAndScanSegmentsAndFilterPartialPathArrayIndex(
      String name,
      boolean auto,
      NestedCommonFormatColumnFormatSpec spec
  ) throws Exception
  {
    DimensionsSpec dimensionsSpec =
        DimensionsSpec.builder()
                      .setDimensions(List.of(auto
                                             ? new AutoTypeColumnSchema("complexObj", ColumnType.NESTED_DATA, spec)
                                             : new NestedDataColumnSchema("complexObj", 5, spec, DEFAULT_FORMAT)))
                      .build();
    Query<ScanResultValue> scanQuery = queryBuilder()
        .columns("timestamp", "str", "double", "bool", "variant",
                 "variantNumeric", "variantEmptyObj", "variantEmtpyArray", "variantWithArrays"
        ) // not all columns are included here, because when raw object is not stored we lost the boolean type and some null value fields
        .filters(NotDimFilter.of(NullFilter.forColumn("v0")))
        .virtualColumns(
            new NestedFieldVirtualColumn("complexObj", "v0", ColumnType.NESTED_DATA, null, true, "$.y[0]", false)
        )
        .build();

    NestedDataTestUtils.ResourceFileSegmentBuilder builder =
        new NestedDataTestUtils.ResourceFileSegmentBuilder(
            tempFolder,
            closer
        ).input(NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE)
         .dimensionsSpec(dimensionsSpec);
    List<Segment> realtimeSegs = List.of(builder.buildIncremental());
    List<Segment> segs = builder.granularity(Granularities.HOUR).build();

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
  @Parameters(method = "getNestedColumnFormatSpec")
  @TestCaseName("{0}")
  public void testIngestAndScanSegmentsAndFilterPartialPath(
      String name,
      boolean auto,
      NestedCommonFormatColumnFormatSpec spec
  ) throws Exception
  {
    DimensionsSpec dimensionsSpec =
        DimensionsSpec.builder()
                      .setDimensions(List.of(auto
                                             ? new AutoTypeColumnSchema("obj", ColumnType.NESTED_DATA, spec)
                                             : new NestedDataColumnSchema("obj", 5, spec, DEFAULT_FORMAT)))
                      .build();
    Query<ScanResultValue> scanQuery = queryBuilder()
        .columns("timestamp", "str", "double", "bool", "variant",
                 "variantNumeric", "variantEmptyObj", "variantEmtpyArray", "variantWithArrays"
        ) // not all columns are included here, because when raw object is not stored we lost the boolean type and some null value fields
        .filters(NotDimFilter.of(NullFilter.forColumn("v0")))
        .virtualColumns(new NestedFieldVirtualColumn("obj", "v0", ColumnType.NESTED_DATA, null, true, "$.b", false))
        .build();
    NestedDataTestUtils.ResourceFileSegmentBuilder builder =
        new NestedDataTestUtils.ResourceFileSegmentBuilder(
            tempFolder,
            closer
        ).input(NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE)
         .dimensionsSpec(dimensionsSpec);
    List<Segment> realtimeSegs = List.of(builder.buildIncremental());
    List<Segment> segs = builder.granularity(Granularities.HOUR).build();

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
  @Parameters(method = "getNestedColumnFormatSpec")
  @TestCaseName("{0}")
  public void testIngestAndScanSegmentsNestedColumnNotNullFilter(
      String name,
      boolean auto,
      NestedCommonFormatColumnFormatSpec spec
  ) throws Exception
  {
    DimensionsSpec dimensionsSpec =
        DimensionsSpec.builder()
                      .setDimensions(List.of(
                          auto ? new AutoTypeColumnSchema("str", null, spec) : new StringDimensionSchema("str"),
                          auto
                          ? new AutoTypeColumnSchema("complexObj", ColumnType.NESTED_DATA, spec)
                          : new NestedDataColumnSchema("complexObj", 5, spec, DEFAULT_FORMAT)
                      ))
                      .build();
    Query<ScanResultValue> scanQuery = queryBuilder()
        .filters(new AndDimFilter(NotDimFilter.of(NullFilter.forColumn("complexObj")), new NullFilter("str", null)))
        .columns("complexObj")
        .build();
    NestedDataTestUtils.ResourceFileSegmentBuilder builder =
        new NestedDataTestUtils.ResourceFileSegmentBuilder(
            tempFolder,
            closer
        ).input(NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE)
         .dimensionsSpec(dimensionsSpec);
    List<Segment> realtimeSegs = List.of(builder.buildIncremental());
    List<Segment> segs = builder.build();

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(realtimeSegs, scanQuery);
    final Sequence<ScanResultValue> seq2 = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> resultsRealtime = seq.toList();
    List<ScanResultValue> resultsSegments = seq2.toList();
    logResults(resultsSegments);
    logResults(resultsRealtime);
    Assert.assertEquals(1, resultsRealtime.size());
    Assert.assertEquals(resultsRealtime.size(), resultsSegments.size());
    if (ObjectStorageEncoding.NONE.equals(spec.getObjectStorageEncoding())) {
      Assert.assertEquals(
          "[[{x=400, y=[{l=[null], m=100, n=5}, {l=[a, b, c], m=a, n=1}]}]]",
          resultsSegments.get(0).getEvents().toString()
      );
    } else {
      Assert.assertEquals(
          "[[{x=400, y=[{l=[null], m=100, n=5}, {l=[a, b, c], m=a, n=1}], z={}}]]",
          resultsSegments.get(0).getEvents().toString()
      );
      Assert.assertEquals(resultsRealtime.get(0).getEvents().toString(), resultsSegments.get(0).getEvents().toString());
    }
  }

  private static Druids.ScanQueryBuilder queryBuilder()
  {
    return Druids.newScanQueryBuilder()
                 .dataSource("test_datasource")
                 .eternityInterval()
                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                 .limit(100)
                 .context(ImmutableMap.of());
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
