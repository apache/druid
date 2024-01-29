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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

public class AutoTypeColumnIndexerTest extends InitializedNullHandlingTest
{
  private static final String TIME_COL = "time";
  private static final String STRING_COL = "string";
  private static final String STRING_ARRAY_COL = "string_array";
  private static final String LONG_COL = "long";
  private static final String DOUBLE_COL = "double";
  private static final String VARIANT_COL = "variant";
  private static final String NESTED_COL = "nested";

  @BeforeClass
  public static void setup()
  {
    NestedDataModule.registerHandlersAndSerde();
  }

  @Test
  public void testKeySizeEstimation()
  {
    AutoTypeColumnIndexer indexer = new AutoTypeColumnIndexer("test", null);
    int baseCardinality = NullHandling.sqlCompatible() ? 0 : 2;
    Assert.assertEquals(baseCardinality, indexer.getCardinality());

    EncodedKeyComponent<StructuredData> key;
    // new raw value, new field, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of("x", "foo"), false);
    Assert.assertEquals(228, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 1, indexer.getCardinality());
    // adding same value only adds estimated size of value itself
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of("x", "foo"), false);
    Assert.assertEquals(112, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 1, indexer.getCardinality());
    // new raw value, new field, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(10L, false);
    Assert.assertEquals(94, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 2, indexer.getCardinality());
    // adding same value only adds estimated size of value itself
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(10L, false);
    Assert.assertEquals(16, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 2, indexer.getCardinality());
    // new raw value, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(11L, false);
    Assert.assertEquals(48, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 3, indexer.getCardinality());

    // new raw value, new fields
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(1L, 2L, 10L), false);
    Assert.assertEquals(168, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 6, indexer.getCardinality());
    // new raw value, re-use fields and dictionary
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(1L, 2L, 10L), false);
    Assert.assertEquals(104, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 6, indexer.getCardinality());
    // new raw value, new fields
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(
        ImmutableMap.of("x", ImmutableList.of(1L, 2L, 10L)),
        false
    );
    Assert.assertEquals(166, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 6, indexer.getCardinality());
    // new raw value
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(
        ImmutableMap.of("x", ImmutableList.of(1L, 2L, 10L)),
        false
    );
    Assert.assertEquals(166, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 6, indexer.getCardinality());

    key = indexer.processRowValsToUnsortedEncodedKeyComponent("", false);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0, key.getEffectiveSizeBytes());
      Assert.assertEquals(baseCardinality + 7, indexer.getCardinality());
    } else {
      Assert.assertEquals(104, key.getEffectiveSizeBytes());
      Assert.assertEquals(baseCardinality + 7, indexer.getCardinality());
    }

    key = indexer.processRowValsToUnsortedEncodedKeyComponent(0L, false);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(16, key.getEffectiveSizeBytes());
      Assert.assertEquals(baseCardinality + 7, indexer.getCardinality());
    } else {
      Assert.assertEquals(48, key.getEffectiveSizeBytes());
      Assert.assertEquals(baseCardinality + 8, indexer.getCardinality());
    }
  }

  @Test
  public void testNestedColumnIndexerSchemaDiscoveryRootString() throws IndexSizeExceededException
  {
    long minTimestamp = System.currentTimeMillis();
    IncrementalIndex index = makeIncrementalIndex(minTimestamp);

    index.add(makeInputRow(minTimestamp + 1, true, STRING_COL, "a"));
    index.add(makeInputRow(minTimestamp + 2, true, STRING_COL, "b"));
    index.add(makeInputRow(minTimestamp + 3, true, STRING_COL, "c"));
    index.add(makeInputRow(minTimestamp + 4, true, STRING_COL, null));
    index.add(makeInputRow(minTimestamp + 5, false, STRING_COL, null));

    IncrementalIndexStorageAdapter storageAdapter = new IncrementalIndexStorageAdapter(index);
    Sequence<Cursor> cursorSequence = storageAdapter.makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.NONE,
        false,
        null
    );
    final DimensionSpec dimensionSpec = new DefaultDimensionSpec(STRING_COL, STRING_COL, ColumnType.STRING);
    List<Cursor> cursorList = cursorSequence.toList();
    ColumnSelectorFactory columnSelectorFactory = cursorList.get(0).getColumnSelectorFactory();

    ColumnValueSelector valueSelector = columnSelectorFactory.makeColumnValueSelector(STRING_COL);
    DimensionSelector dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    Assert.assertEquals("a", valueSelector.getObject());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertEquals("a", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals("a", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(1).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(STRING_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    Assert.assertEquals("b", valueSelector.getObject());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertEquals("b", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals("b", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(2).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(STRING_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    Assert.assertEquals("c", valueSelector.getObject());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertEquals("c", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals("c", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(3).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(STRING_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    Assert.assertNull(valueSelector.getObject());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertNull(dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertNull(dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(4).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(STRING_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    Assert.assertNull(valueSelector.getObject());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertNull(dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertNull(dimensionSelector.getObject());

    Assert.assertEquals(ColumnType.STRING, storageAdapter.getColumnCapabilities(STRING_COL).toColumnType());
  }

  @Test
  public void testNestedColumnIndexerSchemaDiscoveryRootLong() throws IndexSizeExceededException
  {
    long minTimestamp = System.currentTimeMillis();
    IncrementalIndex index = makeIncrementalIndex(minTimestamp);

    index.add(makeInputRow(minTimestamp + 1, true, LONG_COL, 1L));
    index.add(makeInputRow(minTimestamp + 2, true, LONG_COL, 2L));
    index.add(makeInputRow(minTimestamp + 3, true, LONG_COL, 3L));
    index.add(makeInputRow(minTimestamp + 4, true, LONG_COL, null));
    index.add(makeInputRow(minTimestamp + 5, false, LONG_COL, null));

    IncrementalIndexStorageAdapter storageAdapter = new IncrementalIndexStorageAdapter(index);
    Sequence<Cursor> cursorSequence = storageAdapter.makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.NONE,
        false,
        null
    );
    final DimensionSpec dimensionSpec = new DefaultDimensionSpec(LONG_COL, LONG_COL, ColumnType.LONG);
    List<Cursor> cursorList = cursorSequence.toList();
    ColumnSelectorFactory columnSelectorFactory = cursorList.get(0).getColumnSelectorFactory();

    ColumnValueSelector valueSelector = columnSelectorFactory.makeColumnValueSelector(LONG_COL);
    DimensionSelector dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    Assert.assertEquals(1L, valueSelector.getObject());
    Assert.assertEquals(1L, valueSelector.getLong());
    Assert.assertFalse(valueSelector.isNull());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertEquals("1", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals("1", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(1).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(LONG_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    Assert.assertEquals(2L, valueSelector.getObject());
    Assert.assertEquals(2L, valueSelector.getLong());
    Assert.assertFalse(valueSelector.isNull());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertEquals("2", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals("2", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(2).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(LONG_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    Assert.assertEquals(3L, valueSelector.getObject());
    Assert.assertEquals(3L, valueSelector.getLong());
    Assert.assertFalse(valueSelector.isNull());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertEquals("3", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals("3", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(3).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(LONG_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    if (NullHandling.sqlCompatible()) {
      Assert.assertNull(valueSelector.getObject());
      Assert.assertTrue(valueSelector.isNull());
      Assert.assertEquals(1, dimensionSelector.getRow().size());
      Assert.assertNull(dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
      Assert.assertNull(dimensionSelector.getObject());
    } else {
      Assert.assertEquals(NullHandling.defaultLongValue(), valueSelector.getObject());
      Assert.assertFalse(valueSelector.isNull());
      Assert.assertEquals(1, dimensionSelector.getRow().size());
      Assert.assertEquals(
          String.valueOf(NullHandling.defaultLongValue()),
          dimensionSelector.lookupName(dimensionSelector.getRow().get(0))
      );
      Assert.assertEquals(String.valueOf(NullHandling.defaultLongValue()), dimensionSelector.getObject());
    }


    columnSelectorFactory = cursorList.get(4).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(LONG_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    if (NullHandling.sqlCompatible()) {
      Assert.assertNull(valueSelector.getObject());
      Assert.assertTrue(valueSelector.isNull());
      Assert.assertEquals(1, dimensionSelector.getRow().size());
      Assert.assertNull(dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
      Assert.assertNull(dimensionSelector.getObject());
    } else {
      Assert.assertEquals(NullHandling.defaultLongValue(), valueSelector.getObject());
      Assert.assertFalse(valueSelector.isNull());
      Assert.assertEquals(1, dimensionSelector.getRow().size());
      Assert.assertEquals(
          String.valueOf(NullHandling.defaultLongValue()),
          dimensionSelector.lookupName(dimensionSelector.getRow().get(0))
      );
      Assert.assertEquals(String.valueOf(NullHandling.defaultLongValue()), dimensionSelector.getObject());
    }
    Assert.assertEquals(ColumnType.LONG, storageAdapter.getColumnCapabilities(LONG_COL).toColumnType());
  }

  @Test
  public void testNestedColumnIndexerSchemaDiscoveryRootDouble() throws IndexSizeExceededException
  {
    long minTimestamp = System.currentTimeMillis();
    IncrementalIndex index = makeIncrementalIndex(minTimestamp);

    index.add(makeInputRow(minTimestamp + 1, true, DOUBLE_COL, 1.1));
    index.add(makeInputRow(minTimestamp + 2, true, DOUBLE_COL, 2.2));
    index.add(makeInputRow(minTimestamp + 3, true, DOUBLE_COL, 3.3));
    index.add(makeInputRow(minTimestamp + 4, true, DOUBLE_COL, null));
    index.add(makeInputRow(minTimestamp + 5, false, DOUBLE_COL, null));

    IncrementalIndexStorageAdapter storageAdapter = new IncrementalIndexStorageAdapter(index);
    Sequence<Cursor> cursorSequence = storageAdapter.makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.NONE,
        false,
        null
    );
    final DimensionSpec dimensionSpec = new DefaultDimensionSpec(DOUBLE_COL, DOUBLE_COL, ColumnType.DOUBLE);
    List<Cursor> cursorList = cursorSequence.toList();
    ColumnSelectorFactory columnSelectorFactory = cursorList.get(0).getColumnSelectorFactory();

    ColumnValueSelector valueSelector = columnSelectorFactory.makeColumnValueSelector(DOUBLE_COL);
    DimensionSelector dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    Assert.assertEquals(1.1, valueSelector.getObject());
    Assert.assertEquals(1.1, valueSelector.getDouble(), 0.0);
    Assert.assertFalse(valueSelector.isNull());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertEquals("1.1", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals("1.1", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(1).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(DOUBLE_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    Assert.assertEquals(2.2, valueSelector.getObject());
    Assert.assertEquals(2.2, valueSelector.getDouble(), 0.0);
    Assert.assertFalse(valueSelector.isNull());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertEquals("2.2", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals("2.2", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(2).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(DOUBLE_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    Assert.assertEquals(3.3, valueSelector.getObject());
    Assert.assertEquals(3.3, valueSelector.getDouble(), 0.0);
    Assert.assertFalse(valueSelector.isNull());
    Assert.assertEquals(1, dimensionSelector.getRow().size());
    Assert.assertEquals("3.3", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals("3.3", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(3).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(DOUBLE_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    if (NullHandling.sqlCompatible()) {
      Assert.assertNull(valueSelector.getObject());
      Assert.assertTrue(valueSelector.isNull());
      Assert.assertEquals(1, dimensionSelector.getRow().size());
      Assert.assertNull(dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
      Assert.assertNull(dimensionSelector.getObject());
    } else {
      Assert.assertEquals(NullHandling.defaultDoubleValue(), valueSelector.getObject());
      Assert.assertFalse(valueSelector.isNull());
      Assert.assertEquals(1, dimensionSelector.getRow().size());
      Assert.assertEquals(
          String.valueOf(NullHandling.defaultDoubleValue()),
          dimensionSelector.lookupName(dimensionSelector.getRow().get(0))
      );
      Assert.assertEquals(String.valueOf(NullHandling.defaultDoubleValue()), dimensionSelector.getObject());
    }

    columnSelectorFactory = cursorList.get(4).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(DOUBLE_COL);
    dimensionSelector = columnSelectorFactory.makeDimensionSelector(dimensionSpec);
    if (NullHandling.sqlCompatible()) {
      Assert.assertNull(valueSelector.getObject());
      Assert.assertTrue(valueSelector.isNull());
      Assert.assertEquals(1, dimensionSelector.getRow().size());
      Assert.assertNull(dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
      Assert.assertNull(dimensionSelector.getObject());
    } else {
      Assert.assertEquals(NullHandling.defaultDoubleValue(), valueSelector.getObject());
      Assert.assertFalse(valueSelector.isNull());
      Assert.assertEquals(1, dimensionSelector.getRow().size());
      Assert.assertEquals(
          String.valueOf(NullHandling.defaultDoubleValue()),
          dimensionSelector.lookupName(dimensionSelector.getRow().get(0))
      );
      Assert.assertEquals(String.valueOf(NullHandling.defaultDoubleValue()), dimensionSelector.getObject());
    }
    Assert.assertEquals(ColumnType.DOUBLE, storageAdapter.getColumnCapabilities(DOUBLE_COL).toColumnType());
  }

  @Test
  public void testNestedColumnIndexerSchemaDiscoveryRootStringArray() throws IndexSizeExceededException
  {
    long minTimestamp = System.currentTimeMillis();
    IncrementalIndex index = makeIncrementalIndex(minTimestamp);

    index.add(makeInputRow(minTimestamp + 1, true, STRING_ARRAY_COL, new String[]{"a"}));
    index.add(makeInputRow(minTimestamp + 2, true, STRING_ARRAY_COL, new Object[]{"b", "c"}));
    index.add(makeInputRow(minTimestamp + 3, true, STRING_ARRAY_COL, ImmutableList.of("d", "e")));
    index.add(makeInputRow(minTimestamp + 4, true, STRING_ARRAY_COL, null));
    index.add(makeInputRow(minTimestamp + 5, false, STRING_ARRAY_COL, null));

    IncrementalIndexStorageAdapter storageAdapter = new IncrementalIndexStorageAdapter(index);
    Sequence<Cursor> cursorSequence = storageAdapter.makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.NONE,
        false,
        null
    );
    final DimensionSpec dimensionSpec = new DefaultDimensionSpec(STRING_ARRAY_COL, STRING_ARRAY_COL, ColumnType.STRING);
    List<Cursor> cursorList = cursorSequence.toList();
    ColumnSelectorFactory columnSelectorFactory = cursorList.get(0).getColumnSelectorFactory();

    ColumnValueSelector valueSelector = columnSelectorFactory.makeColumnValueSelector(STRING_ARRAY_COL);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> cursorList.get(0).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec)
    );
    Assert.assertArrayEquals(new Object[]{"a"}, (Object[]) valueSelector.getObject());

    columnSelectorFactory = cursorList.get(1).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(STRING_ARRAY_COL);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> cursorList.get(1).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec)
    );
    Assert.assertArrayEquals(new Object[]{"b", "c"}, (Object[]) valueSelector.getObject());

    columnSelectorFactory = cursorList.get(2).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(STRING_ARRAY_COL);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> cursorList.get(2).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec)
    );
    Assert.assertArrayEquals(new Object[]{"d", "e"}, (Object[]) valueSelector.getObject());

    columnSelectorFactory = cursorList.get(3).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(STRING_ARRAY_COL);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> cursorList.get(3).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec)
    );
    Assert.assertNull(valueSelector.getObject());

    columnSelectorFactory = cursorList.get(4).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(STRING_ARRAY_COL);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> cursorList.get(4).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec)
    );
    Assert.assertNull(valueSelector.getObject());
    Assert.assertEquals(ColumnType.STRING_ARRAY, storageAdapter.getColumnCapabilities(STRING_ARRAY_COL).toColumnType());
  }

  @Test
  public void testNestedColumnIndexerSchemaDiscoveryRootVariant() throws IndexSizeExceededException
  {
    long minTimestamp = System.currentTimeMillis();
    IncrementalIndex index = makeIncrementalIndex(minTimestamp);

    index.add(makeInputRow(minTimestamp + 1, true, VARIANT_COL, "a"));
    index.add(makeInputRow(minTimestamp + 2, true, VARIANT_COL, 2L));
    index.add(makeInputRow(minTimestamp + 3, true, VARIANT_COL, 3.3));
    index.add(makeInputRow(minTimestamp + 4, true, VARIANT_COL, null));
    index.add(makeInputRow(minTimestamp + 5, false, VARIANT_COL, null));

    IncrementalIndexStorageAdapter storageAdapter = new IncrementalIndexStorageAdapter(index);
    Sequence<Cursor> cursorSequence = storageAdapter.makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.NONE,
        false,
        null
    );
    final DimensionSpec dimensionSpec = new DefaultDimensionSpec(VARIANT_COL, VARIANT_COL, ColumnType.STRING);
    List<Cursor> cursorList = cursorSequence.toList();
    ColumnSelectorFactory columnSelectorFactory = cursorList.get(0).getColumnSelectorFactory();

    ColumnValueSelector valueSelector = columnSelectorFactory.makeColumnValueSelector(VARIANT_COL);
    DimensionSelector dimensionSelector = cursorList.get(0).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
    Assert.assertEquals("a", valueSelector.getObject());
    Assert.assertEquals("a", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(1).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(VARIANT_COL);
    dimensionSelector = cursorList.get(1).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
    Assert.assertEquals(2L, valueSelector.getObject());
    Assert.assertFalse(valueSelector.isNull());
    Assert.assertEquals("2", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(2).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(VARIANT_COL);
    dimensionSelector = cursorList.get(2).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
    Assert.assertEquals(3.3, valueSelector.getObject());
    Assert.assertFalse(valueSelector.isNull());
    Assert.assertEquals("3.3", dimensionSelector.getObject());


    columnSelectorFactory = cursorList.get(3).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(VARIANT_COL);
    dimensionSelector = cursorList.get(3).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
    Assert.assertNull(valueSelector.getObject());
    Assert.assertNull(dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(4).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(VARIANT_COL);
    dimensionSelector = cursorList.get(4).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
    Assert.assertNull(valueSelector.getObject());
    Assert.assertNull(dimensionSelector.getObject());
    Assert.assertEquals(ColumnType.STRING, storageAdapter.getColumnCapabilities(VARIANT_COL).toColumnType());
  }

  @Test
  public void testNestedColumnIndexerSchemaDiscoveryNested() throws IndexSizeExceededException
  {
    long minTimestamp = System.currentTimeMillis();
    IncrementalIndex index = makeIncrementalIndex(minTimestamp);

    index.add(makeInputRow(minTimestamp + 1, true, NESTED_COL, "a"));
    index.add(makeInputRow(minTimestamp + 2, true, NESTED_COL, 2L));
    index.add(makeInputRow(minTimestamp + 3, true, NESTED_COL, ImmutableMap.of("x", 1.1, "y", 2L)));
    index.add(makeInputRow(minTimestamp + 4, true, NESTED_COL, null));
    index.add(makeInputRow(minTimestamp + 5, false, NESTED_COL, null));

    IncrementalIndexStorageAdapter storageAdapter = new IncrementalIndexStorageAdapter(index);
    Sequence<Cursor> cursorSequence = storageAdapter.makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.NONE,
        false,
        null
    );
    final DimensionSpec dimensionSpec = new DefaultDimensionSpec(NESTED_COL, NESTED_COL, ColumnType.STRING);
    List<Cursor> cursorList = cursorSequence.toList();
    ColumnSelectorFactory columnSelectorFactory = cursorList.get(0).getColumnSelectorFactory();

    ColumnValueSelector valueSelector = columnSelectorFactory.makeColumnValueSelector(NESTED_COL);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> cursorList.get(0).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec)
    );
    Assert.assertEquals(StructuredData.wrap("a"), valueSelector.getObject());

    columnSelectorFactory = cursorList.get(1).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(NESTED_COL);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> cursorList.get(1).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec)
    );
    Assert.assertEquals(StructuredData.wrap(2L), valueSelector.getObject());

    columnSelectorFactory = cursorList.get(2).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(NESTED_COL);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> cursorList.get(2).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec)
    );
    Assert.assertEquals(StructuredData.wrap(ImmutableMap.of("x", 1.1, "y", 2L)), valueSelector.getObject());

    columnSelectorFactory = cursorList.get(3).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(NESTED_COL);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> cursorList.get(3).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec)
    );
    Assert.assertNull(valueSelector.getObject());

    columnSelectorFactory = cursorList.get(4).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(NESTED_COL);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> cursorList.get(4).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec)
    );
    Assert.assertNull(valueSelector.getObject());
    Assert.assertEquals(ColumnType.NESTED_DATA, storageAdapter.getColumnCapabilities(NESTED_COL).toColumnType());
  }

  @Test
  public void testNestedColumnIndexerSchemaDiscoveryTypeCoercion() throws IndexSizeExceededException
  {
    // coerce nested column to STRING type, throwing parse exceptions for nested data
    // and casting anything else to string
    long minTimestamp = System.currentTimeMillis();
    IncrementalIndex index = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema(
                minTimestamp,
                new TimestampSpec(TIME_COL, "millis", null),
                Granularities.NONE,
                VirtualColumns.EMPTY,
                DimensionsSpec.builder()
                              .setDimensions(ImmutableList.of(new AutoTypeColumnSchema(NESTED_COL, ColumnType.STRING)))
                              .useSchemaDiscovery(true)
                              .build(),
                new AggregatorFactory[0],
                false
            )
        )
        .setMaxRowCount(1000)
        .build();

    index.add(makeInputRow(minTimestamp + 1, true, NESTED_COL, "a"));
    index.add(makeInputRow(minTimestamp + 2, true, NESTED_COL, 2L));
    IncrementalIndexAddResult result = index.add(makeInputRow(minTimestamp + 3, true, NESTED_COL, ImmutableMap.of("x", 1.1, "y", 2L)));
    Assert.assertTrue(result.hasParseException());
    index.add(makeInputRow(minTimestamp + 4, true, NESTED_COL, null));
    index.add(makeInputRow(minTimestamp + 5, false, NESTED_COL, null));

    IncrementalIndexStorageAdapter storageAdapter = new IncrementalIndexStorageAdapter(index);
    Sequence<Cursor> cursorSequence = storageAdapter.makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.NONE,
        false,
        null
    );
    final DimensionSpec dimensionSpec = new DefaultDimensionSpec(NESTED_COL, NESTED_COL, ColumnType.STRING);
    List<Cursor> cursorList = cursorSequence.toList();
    ColumnSelectorFactory columnSelectorFactory = cursorList.get(0).getColumnSelectorFactory();

    ColumnValueSelector valueSelector = columnSelectorFactory.makeColumnValueSelector(NESTED_COL);
    DimensionSelector dimensionSelector = cursorList.get(0).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
    Assert.assertEquals("a", valueSelector.getObject());
    Assert.assertEquals("a", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(1).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(NESTED_COL);
    dimensionSelector = cursorList.get(1).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
    Assert.assertEquals("2", valueSelector.getObject());
    Assert.assertFalse(valueSelector.isNull());
    Assert.assertEquals("2", dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(2).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(NESTED_COL);
    dimensionSelector = cursorList.get(2).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
    Assert.assertNull(valueSelector.getObject());
    Assert.assertNull(dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(3).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(NESTED_COL);
    dimensionSelector = cursorList.get(3).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
    Assert.assertNull(valueSelector.getObject());
    Assert.assertNull(dimensionSelector.getObject());

    columnSelectorFactory = cursorList.get(4).getColumnSelectorFactory();
    valueSelector = columnSelectorFactory.makeColumnValueSelector(NESTED_COL);
    dimensionSelector = cursorList.get(4).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
    Assert.assertNull(valueSelector.getObject());
    Assert.assertNull(dimensionSelector.getObject());

    Assert.assertEquals(ColumnType.STRING, storageAdapter.getColumnCapabilities(NESTED_COL).toColumnType());
  }

  @Test
  public void testConstantNull()
  {
    int baseCardinality = NullHandling.sqlCompatible() ? 0 : 2;
    AutoTypeColumnIndexer indexer = new AutoTypeColumnIndexer("test", null);
    EncodedKeyComponent<StructuredData> key;

    key = indexer.processRowValsToUnsortedEncodedKeyComponent(null, true);
    Assert.assertEquals(0, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(null, true);

    Assert.assertEquals(0, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(null, true);
    Assert.assertEquals(0, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality, indexer.getCardinality());


    Assert.assertTrue(indexer.hasNulls);
    Assert.assertFalse(indexer.hasNestedData);
    Assert.assertTrue(indexer.isConstant());
    Assert.assertEquals(ColumnType.STRING, indexer.getLogicalType());
  }

  @Test
  public void testConstantString()
  {
    int baseCardinality = NullHandling.sqlCompatible() ? 0 : 2;
    AutoTypeColumnIndexer indexer = new AutoTypeColumnIndexer("test", null);
    EncodedKeyComponent<StructuredData> key;

    key = indexer.processRowValsToUnsortedEncodedKeyComponent("abcd", true);
    Assert.assertEquals(166, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 1, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent("abcd", true);

    Assert.assertEquals(52, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 1, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent("abcd", true);
    Assert.assertEquals(52, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 1, indexer.getCardinality());

    Assert.assertFalse(indexer.hasNulls);
    Assert.assertFalse(indexer.hasNestedData);
    Assert.assertTrue(indexer.isConstant());
    Assert.assertEquals(ColumnType.STRING, indexer.getLogicalType());
  }

  @Test
  public void testConstantLong()
  {
    int baseCardinality = NullHandling.sqlCompatible() ? 0 : 2;
    AutoTypeColumnIndexer indexer = new AutoTypeColumnIndexer("test", null);
    EncodedKeyComponent<StructuredData> key;

    key = indexer.processRowValsToUnsortedEncodedKeyComponent(1234L, true);
    Assert.assertEquals(94, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 1, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(1234L, true);

    Assert.assertEquals(16, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 1, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(1234L, true);
    Assert.assertEquals(16, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 1, indexer.getCardinality());

    Assert.assertFalse(indexer.hasNulls);
    Assert.assertFalse(indexer.hasNestedData);
    Assert.assertTrue(indexer.isConstant());
    Assert.assertEquals(ColumnType.LONG, indexer.getLogicalType());
  }

  @Test
  public void testConstantEmptyArray()
  {
    int baseCardinality = NullHandling.sqlCompatible() ? 0 : 2;
    AutoTypeColumnIndexer indexer = new AutoTypeColumnIndexer("test", null);
    EncodedKeyComponent<StructuredData> key;

    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(), true);
    Assert.assertEquals(54, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 1, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(), true);

    Assert.assertEquals(8, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 1, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(), true);
    Assert.assertEquals(8, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 1, indexer.getCardinality());

    Assert.assertFalse(indexer.hasNulls);
    Assert.assertFalse(indexer.hasNestedData);
    Assert.assertTrue(indexer.isConstant());
    Assert.assertEquals(ColumnType.LONG_ARRAY, indexer.getLogicalType());
  }

  @Test
  public void testConstantArray()
  {
    int baseCardinality = NullHandling.sqlCompatible() ? 0 : 2;
    AutoTypeColumnIndexer indexer = new AutoTypeColumnIndexer("test", null);
    EncodedKeyComponent<StructuredData> key;

    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(1L, 2L, 3L), true);
    Assert.assertEquals(246, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 4, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(1L, 2L, 3L), true);

    Assert.assertEquals(104, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 4, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(1L, 2L, 3L), true);
    Assert.assertEquals(104, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality + 4, indexer.getCardinality());

    Assert.assertFalse(indexer.hasNulls);
    Assert.assertFalse(indexer.hasNestedData);
    Assert.assertTrue(indexer.isConstant());
    Assert.assertEquals(ColumnType.LONG_ARRAY, indexer.getLogicalType());
  }

  @Test
  public void testConstantEmptyObject()
  {
    int baseCardinality = NullHandling.sqlCompatible() ? 0 : 2;
    AutoTypeColumnIndexer indexer = new AutoTypeColumnIndexer("test", null);
    EncodedKeyComponent<StructuredData> key;

    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of(), true);
    Assert.assertEquals(16, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of(), true);

    Assert.assertEquals(16, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality, indexer.getCardinality());
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of(), true);
    Assert.assertEquals(16, key.getEffectiveSizeBytes());
    Assert.assertEquals(baseCardinality, indexer.getCardinality());

    Assert.assertFalse(indexer.hasNulls);
    Assert.assertTrue(indexer.hasNestedData);
    Assert.assertTrue(indexer.isConstant());
    Assert.assertEquals(ColumnType.NESTED_DATA, indexer.getLogicalType());
  }

  @Nonnull
  private static IncrementalIndex makeIncrementalIndex(long minTimestamp)
  {
    IncrementalIndex index = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema(
                minTimestamp,
                new TimestampSpec(TIME_COL, "millis", null),
                Granularities.NONE,
                VirtualColumns.EMPTY,
                DimensionsSpec.builder().useSchemaDiscovery(true).build(),
                new AggregatorFactory[0],
                false
            )
        )
        .setMaxRowCount(1000)
        .build();
    return index;
  }

  private MapBasedInputRow makeInputRow(
      long timestamp,
      boolean explicitNull,
      Object... kv
  )
  {
    final Map<String, Object> event = TestHelper.makeMap(explicitNull, kv);
    event.put("time", timestamp);
    return new MapBasedInputRow(timestamp, ImmutableList.copyOf(event.keySet()), event);
  }
}
