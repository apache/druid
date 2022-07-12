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
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Druids;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.apache.druid.segment.nested.NestedPathPart;
import org.apache.druid.segment.vector.BaseDoubleVectorValueSelector;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
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
  private static final String NESTED_LONG_FIELD = "long";
  private static final String NESTED_DOUBLE_FIELD = "double";
  private static final String NESTED_MIXED_NUMERIC_FIELD = "mixed_numeric";
  private static final String NESTED_MIXED_FIELD = "mixed";
  private static final String NESTED_SPARSE_LONG_FIELD = "sparse_long";
  private static final String NESTED_SPARSE_DOUBLE_FIELD = "sparse_double";
  private static final String NESTED_SPARSE_MIXED_NUMERIC_FIELD = "sparse_mixed_numeric";
  private static final String NESTED_SPARSE_MIXED_FIELD = "sparse_mixed";

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
    this.helper = AggregationTestHelper.createScanQueryAggregationTestHelper(
        mods,
        tempFolder
    );
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
    List<Segment> segs = NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder, closer);

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
            helper,
            tempFolder,
            closer,
            NestedDataTestUtils.NUMERIC_DATA_FILE,
            NestedDataTestUtils.NUMERIC_PARSER_FILE,
            NestedDataTestUtils.SIMPLE_AGG_FILE,
            Granularities.YEAR,
            true,
            1000
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
                                                 new NestedFieldVirtualColumn("nester", "$.", "nester_root")
                                             )
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .limit(100)
                                             .context(ImmutableMap.of())
                                             .build();
    List<Segment> realtimeSegs = ImmutableList.of(
        NestedDataTestUtils.createDefaultHourlyIncrementalIndex()
    );
    List<Segment> segs = NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder, closer);


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
                                                     NestedDataComplexTypeSerde.TYPE,
                                                     null,
                                                     true,
                                                     "$.x[0]",
                                                     false
                                                 ),
                                                 new NestedFieldVirtualColumn(
                                                     "nester",
                                                     "y_c_1",
                                                     NestedDataComplexTypeSerde.TYPE,
                                                     null,
                                                     true,
                                                     "$.y.c[1]",
                                                     false
                                                 ),
                                                 new NestedFieldVirtualColumn(
                                                     "nester",
                                                     "nester_root",
                                                     NestedDataComplexTypeSerde.TYPE,
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
        NestedDataTestUtils.createDefaultHourlyIncrementalIndex()
    );
    List<Segment> segs = NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder, closer);


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
    List<Segment> segs = NestedDataTestUtils.createDefaultHourlySegmentsTsv(helper, tempFolder, closer);

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
    List<Segment> segs = NestedDataTestUtils.createSegments(
        helper,
        tempFolder,
        closer,
        Granularities.HOUR,
        true,
        3
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
    List<Segment> segs = NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder, closer);

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
    List<Segment> segs = NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder, closer);

    final Sequence<ScanResultValue> seq = helper.runQueryOnSegmentsObjs(segs, scanQuery);

    List<ScanResultValue> results = seq.toList();
    logResults(results);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(4, ((List) results.get(0).getEvents()).size());
  }

  @Test
  public void testExpectedTypes() throws Exception
  {
    // "Line matches the illegal pattern 'ObjectColumnSelector, LongColumnSelector, FloatColumnSelector
    // and DoubleColumnSelector must not be used in an instanceof statement, see Javadoc of those interfaces."
    //CHECKSTYLE.OFF: Regexp
    ColumnSelectorFactory columnSelectorFactory = getNumericColumnSelectorFactory(
        makeNestedNumericVirtualColumns()
    );

    ColumnValueSelector longValueSelector = columnSelectorFactory.makeColumnValueSelector(
        NESTED_LONG_FIELD
    );
    Assert.assertNotNull(longValueSelector);
    Assert.assertTrue(longValueSelector instanceof LongColumnSelector);

    ColumnValueSelector doubleValueSelector = columnSelectorFactory.makeColumnValueSelector(
        NESTED_DOUBLE_FIELD
    );
    Assert.assertNotNull(doubleValueSelector);
    Assert.assertTrue(doubleValueSelector instanceof DoubleColumnSelector);

    ColumnValueSelector mixedNumericValueSelector = columnSelectorFactory.makeColumnValueSelector(
        NESTED_MIXED_NUMERIC_FIELD
    );
    Assert.assertNotNull(mixedNumericValueSelector);
    Assert.assertTrue(mixedNumericValueSelector instanceof DimensionDictionarySelector);

    ColumnValueSelector mixedValueSelector = columnSelectorFactory.makeColumnValueSelector(
        NESTED_MIXED_FIELD
    );
    Assert.assertNotNull(mixedValueSelector);
    Assert.assertTrue(mixedValueSelector instanceof DimensionDictionarySelector);


    ColumnValueSelector sparseLongValueSelector = columnSelectorFactory.makeColumnValueSelector(
        NESTED_SPARSE_LONG_FIELD
    );
    Assert.assertNotNull(sparseLongValueSelector);
    Assert.assertTrue(sparseLongValueSelector instanceof LongColumnSelector);

    ColumnValueSelector sparseDoubleValueSelector = columnSelectorFactory.makeColumnValueSelector(
        NESTED_SPARSE_DOUBLE_FIELD
    );
    Assert.assertNotNull(sparseDoubleValueSelector);
    Assert.assertTrue(sparseDoubleValueSelector instanceof DoubleColumnSelector);

    ColumnValueSelector sparseMixedNumericValueSelector = columnSelectorFactory.makeColumnValueSelector(
        NESTED_SPARSE_MIXED_NUMERIC_FIELD
    );
    Assert.assertNotNull(sparseMixedNumericValueSelector);
    Assert.assertTrue(sparseMixedNumericValueSelector instanceof DimensionDictionarySelector);

    ColumnValueSelector sparseMixedValueSelector = columnSelectorFactory.makeColumnValueSelector(
        NESTED_SPARSE_MIXED_FIELD
    );
    Assert.assertNotNull(sparseMixedValueSelector);
    Assert.assertTrue(sparseMixedValueSelector instanceof DimensionDictionarySelector);
    //CHECKSTYLE.ON: Regexp
  }

  @Test
  public void testExpectedTypesVectorSelectors() throws Exception
  {
    // "Line matches the illegal pattern 'ObjectColumnSelector, LongColumnSelector, FloatColumnSelector
    // and DoubleColumnSelector must not be used in an instanceof statement, see Javadoc of those interfaces."
    //CHECKSTYLE.OFF: Regexp
    VectorColumnSelectorFactory factory = getVectorColumnSelectorFactory(
        makeNestedNumericVirtualColumns()
    );

    // can make numeric value selectors for single typed numeric types
    VectorValueSelector longValueSelector = factory.makeValueSelector(
        NESTED_LONG_FIELD
    );
    Assert.assertNotNull(longValueSelector);
    Assert.assertTrue(longValueSelector instanceof BaseLongVectorValueSelector);

    VectorValueSelector doubleValueSelector = factory.makeValueSelector(
        NESTED_DOUBLE_FIELD
    );
    Assert.assertNotNull(doubleValueSelector);
    Assert.assertTrue(doubleValueSelector instanceof BaseDoubleVectorValueSelector);

    Assert.assertThrows(UOE.class, () -> factory.makeValueSelector(NESTED_MIXED_NUMERIC_FIELD));
    Assert.assertThrows(UOE.class, () -> factory.makeValueSelector(NESTED_MIXED_FIELD));

    // can also make single value dimension selectors for all nested column types
    SingleValueDimensionVectorSelector longDimensionSelector = factory.makeSingleValueDimensionSelector(
        DefaultDimensionSpec.of(NESTED_LONG_FIELD)
    );
    Assert.assertNotNull(longDimensionSelector);

    SingleValueDimensionVectorSelector doubleDimensionSelector = factory.makeSingleValueDimensionSelector(
        DefaultDimensionSpec.of(NESTED_DOUBLE_FIELD)
    );
    Assert.assertNotNull(doubleDimensionSelector);

    SingleValueDimensionVectorSelector mixedNumericValueSelector = factory.makeSingleValueDimensionSelector(
        DefaultDimensionSpec.of(NESTED_MIXED_NUMERIC_FIELD)
    );
    Assert.assertNotNull(mixedNumericValueSelector);

    SingleValueDimensionVectorSelector mixedValueSelector = factory.makeSingleValueDimensionSelector(
        DefaultDimensionSpec.of(NESTED_MIXED_FIELD)
    );
    Assert.assertNotNull(mixedValueSelector);

    // and object selectors
    VectorObjectSelector longObjectSelector = factory.makeObjectSelector(
        NESTED_LONG_FIELD
    );
    Assert.assertNotNull(longObjectSelector);

    VectorObjectSelector doubleObjectSelector = factory.makeObjectSelector(
        NESTED_DOUBLE_FIELD
    );
    Assert.assertNotNull(doubleObjectSelector);

    VectorObjectSelector mixedNumericObjectSelector = factory.makeObjectSelector(
        NESTED_MIXED_NUMERIC_FIELD
    );
    Assert.assertNotNull(mixedNumericObjectSelector);

    VectorObjectSelector mixedObjectSelector = factory.makeObjectSelector(
        NESTED_MIXED_FIELD
    );
    Assert.assertNotNull(mixedObjectSelector);
    //CHECKSTYLE.ON: Regexp
  }

  private VirtualColumns makeNestedNumericVirtualColumns()
  {
    List<NestedPathPart> longParts = NestedPathFinder.parseJqPath(".long");
    List<NestedPathPart> doubleParts = NestedPathFinder.parseJqPath(".double");
    List<NestedPathPart> mixedNumericParts = NestedPathFinder.parseJqPath(".mixed_numeric");
    List<NestedPathPart> mixedParts = NestedPathFinder.parseJqPath(".mixed");
    List<NestedPathPart> sparseLongParts = NestedPathFinder.parseJqPath(".sparse_long");
    List<NestedPathPart> sparseDoubleParts = NestedPathFinder.parseJqPath(".sparse_double");
    List<NestedPathPart> sparseMixedNumericParts = NestedPathFinder.parseJqPath(".sparse_mixed_numeric");
    List<NestedPathPart> sparseMixedParts = NestedPathFinder.parseJqPath(".sparse_mixed");

    NestedFieldVirtualColumn longVirtualColumn = new NestedFieldVirtualColumn(
        "nest",
        NESTED_LONG_FIELD,
        ColumnType.LONG,
        longParts,
        false,
        null,
        null
    );
    NestedFieldVirtualColumn doubleVirtualColumn = new NestedFieldVirtualColumn(
        "nest",
        NESTED_DOUBLE_FIELD,
        ColumnType.DOUBLE,
        doubleParts,
        false,
        null,
        null
    );
    NestedFieldVirtualColumn mixedNumericVirtualColumn = new NestedFieldVirtualColumn(
        "nest",
        NESTED_MIXED_NUMERIC_FIELD,
        null,
        mixedNumericParts,
        false,
        null,
        null
    );
    NestedFieldVirtualColumn mixedVirtualColumn = new NestedFieldVirtualColumn(
        "nest",
        NESTED_MIXED_FIELD,
        null,
        mixedParts,
        false,
        null,
        null
    );

    NestedFieldVirtualColumn sparseLongVirtualColumn = new NestedFieldVirtualColumn(
        "nest",
        NESTED_SPARSE_LONG_FIELD,
        ColumnType.LONG,
        sparseLongParts,
        false,
        null,
        null
    );
    NestedFieldVirtualColumn sparseDoubleVirtualColumn = new NestedFieldVirtualColumn(
        "nest",
        NESTED_SPARSE_DOUBLE_FIELD,
        ColumnType.DOUBLE,
        sparseDoubleParts,
        false,
        null,
        null
    );
    NestedFieldVirtualColumn sparseMixedNumericVirtualColumn = new NestedFieldVirtualColumn(
        "nest",
        NESTED_SPARSE_MIXED_NUMERIC_FIELD,
        null,
        sparseMixedNumericParts,
        false,
        null,
        null
    );
    NestedFieldVirtualColumn sparseMixedVirtualColumn = new NestedFieldVirtualColumn(
        "nest",
        NESTED_SPARSE_MIXED_FIELD,
        null,
        sparseMixedParts,
        false,
        null,
        null
    );

    return VirtualColumns.create(
        ImmutableList.of(
            longVirtualColumn,
            doubleVirtualColumn,
            mixedNumericVirtualColumn,
            mixedVirtualColumn,
            sparseLongVirtualColumn,
            sparseDoubleVirtualColumn,
            sparseMixedNumericVirtualColumn,
            sparseMixedVirtualColumn
        )
    );
  }

  private ColumnSelectorFactory getNumericColumnSelectorFactory(VirtualColumns virtualColumns) throws Exception
  {
    List<Segment> segments = NestedDataTestUtils.createSegments(
        helper,
        tempFolder,
        closer,
        NestedDataTestUtils.NUMERIC_DATA_FILE,
        NestedDataTestUtils.NUMERIC_PARSER_FILE,
        NestedDataTestUtils.SIMPLE_AGG_FILE,
        Granularities.DAY,
        true,
        1000
    );
    Assert.assertEquals(1, segments.size());
    StorageAdapter storageAdapter = segments.get(0).asStorageAdapter();
    Sequence<Cursor> cursorSequence = storageAdapter.makeCursors(
        null,
        Intervals.ETERNITY,
        virtualColumns,
        Granularities.DAY,
        false,
        null
    );
    final Yielder<Cursor> yielder = Yielders.each(cursorSequence);
    closer.register(yielder);
    final Cursor cursor = yielder.get();
    return cursor.getColumnSelectorFactory();
  }

  private VectorColumnSelectorFactory getVectorColumnSelectorFactory(VirtualColumns virtualColumns) throws Exception
  {
    List<Segment> segments = NestedDataTestUtils.createSegments(
        helper,
        tempFolder,
        closer,
        NestedDataTestUtils.NUMERIC_DATA_FILE,
        NestedDataTestUtils.NUMERIC_PARSER_FILE,
        NestedDataTestUtils.SIMPLE_AGG_FILE,
        Granularities.DAY,
        true,
        1000
    );
    Assert.assertEquals(1, segments.size());
    StorageAdapter storageAdapter = segments.get(0).asStorageAdapter();
    VectorCursor cursor = storageAdapter.makeVectorCursor(
        null,
        Intervals.ETERNITY,
        virtualColumns,
        false,
        512,
        null
    );
    return cursor.getColumnSelectorFactory();
  }

  private static void logResults(List<ScanResultValue> results)
  {
    StringBuilder bob = new StringBuilder();
    for (Object event : (List) results.get(0).getEvents()) {
      bob.append("[").append(event).append("]").append("\n");
    }
    LOG.info("results:\n%s", bob);
  }
}
