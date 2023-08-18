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

package org.apache.druid.segment.nested;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.transform.TransformSpec;
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
import java.util.List;

public class NestedFieldColumnSelectorsTest extends InitializedNullHandlingTest
{
  private static final String NESTED_LONG_FIELD = "long";
  private static final String NESTED_DOUBLE_FIELD = "double";
  private static final String NESTED_MIXED_NUMERIC_FIELD = "mixed_numeric";
  private static final String NESTED_MIXED_FIELD = "mixed";
  private static final String NESTED_SPARSE_LONG_FIELD = "sparse_long";
  private static final String NESTED_SPARSE_DOUBLE_FIELD = "sparse_double";
  private static final String NESTED_SPARSE_MIXED_NUMERIC_FIELD = "sparse_mixed_numeric";
  private static final String NESTED_SPARSE_MIXED_FIELD = "sparse_mixed";


  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final AggregationTestHelper helper;
  private final Closer closer;

  public NestedFieldColumnSelectorsTest()
  {
    NestedDataModule.registerHandlersAndSerde();
    List<? extends Module> mods = NestedDataModule.getJacksonModulesList();
    this.helper = AggregationTestHelper.createScanQueryAggregationTestHelper(
        mods,
        tempFolder
    );
    this.closer = Closer.create();
  }

  @After
  public void teardown() throws IOException
  {
    closer.close();
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
    Assert.assertTrue(mixedNumericValueSelector instanceof ColumnValueSelector);

    ColumnValueSelector mixedValueSelector = columnSelectorFactory.makeColumnValueSelector(
        NESTED_MIXED_FIELD
    );
    Assert.assertNotNull(mixedValueSelector);
    Assert.assertTrue(mixedValueSelector instanceof ColumnValueSelector);


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
    Assert.assertTrue(sparseMixedNumericValueSelector instanceof ColumnValueSelector);

    ColumnValueSelector sparseMixedValueSelector = columnSelectorFactory.makeColumnValueSelector(
        NESTED_SPARSE_MIXED_FIELD
    );
    Assert.assertNotNull(sparseMixedValueSelector);
    Assert.assertTrue(sparseMixedValueSelector instanceof ColumnValueSelector);
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

    Assert.assertThrows(DruidException.class, () -> factory.makeValueSelector(NESTED_MIXED_FIELD));

    VectorValueSelector mixedNumericValueSelector = factory.makeValueSelector(
        NESTED_MIXED_NUMERIC_FIELD
    );
    Assert.assertTrue(mixedNumericValueSelector instanceof BaseDoubleVectorValueSelector);

    // can also make single value dimension selectors for all nested column types
    SingleValueDimensionVectorSelector longDimensionSelector = factory.makeSingleValueDimensionSelector(
        DefaultDimensionSpec.of(NESTED_LONG_FIELD)
    );
    Assert.assertNotNull(longDimensionSelector);

    SingleValueDimensionVectorSelector doubleDimensionSelector = factory.makeSingleValueDimensionSelector(
        DefaultDimensionSpec.of(NESTED_DOUBLE_FIELD)
    );
    Assert.assertNotNull(doubleDimensionSelector);

    SingleValueDimensionVectorSelector mixedNumericDimensionValueSelector = factory.makeSingleValueDimensionSelector(
        DefaultDimensionSpec.of(NESTED_MIXED_NUMERIC_FIELD)
    );
    Assert.assertNotNull(mixedNumericDimensionValueSelector);

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
        tempFolder,
        closer,
        NestedDataTestUtils.NUMERIC_DATA_FILE,
        NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
        NestedDataTestUtils.TIMESTAMP_SPEC,
        NestedDataTestUtils.AUTO_DISCOVERY,
        TransformSpec.NONE,
        NestedDataTestUtils.COUNT,
        Granularities.NONE,
        true,
        IndexSpec.DEFAULT
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
        tempFolder,
        closer,
        NestedDataTestUtils.NUMERIC_DATA_FILE,
        NestedDataTestUtils.DEFAULT_JSON_INPUT_FORMAT,
        NestedDataTestUtils.TIMESTAMP_SPEC,
        NestedDataTestUtils.AUTO_DISCOVERY,
        TransformSpec.NONE,
        NestedDataTestUtils.COUNT,
        Granularities.NONE,
        true,
        IndexSpec.DEFAULT
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
}
