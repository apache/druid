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

package org.apache.druid.segment.vector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import junitparams.converters.Nullable;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryableIndexVectorColumnSelectorFactoryTest extends InitializedNullHandlingTest
{
  private static final String TS = "t";
  private static final String STRING = "string_column";
  private static final String MULTI_STRING = "multi_string_column";
  private static final String DOUBLE = "double_column";
  private static final String FLOAT = "float_column";
  private static final String LONG = "long_column";

  private static final List<Map<String, Object>> RAW_ROWS = ImmutableList.of(
      makeRow("2022-01-01T00:00Z", "a", "aa", 1.0, 1.0f, 1L),
      makeRow("2022-01-01T00:01Z", "b", ImmutableList.of("bb", "cc"), null, 3.3f, 1999L),
      makeRow("2022-01-01T00:02Z", null, ImmutableList.of("aa", "dd"), 9.9, null, -500L),
      makeRow("2022-01-01T00:03Z", "c", ImmutableList.of("dd", "ee"), -1.1, -999.999f, null),
      makeRow("2022-01-01T00:04Z", "d", ImmutableList.of("aa", "ff"), -90998.132, 1234.5678f, 1234L),
      makeRow("2022-01-01T00:05Z", "e", null, 3.3, 11f, -9000L)
  );

  private static final DimensionsSpec DIMS = new DimensionsSpec(
      ImmutableList.of(
          new StringDimensionSchema(STRING),
          new StringDimensionSchema(MULTI_STRING),
          new DoubleDimensionSchema(DOUBLE),
          new FloatDimensionSchema(FLOAT),
          new LongDimensionSchema(LONG)
      )
  );

  private static final MapInputRowParser OLD_SCHOOL = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TS, "iso", null),
          DIMS
      )
  );

  private static Map<String, Object> makeRow(
      Object t,
      @Nullable Object str,
      @Nullable Object mStr,
      @Nullable Object d,
      @Nullable Object f,
      @Nullable Object l
  )
  {
    Map<String, Object> row = Maps.newHashMapWithExpectedSize(6);
    row.put(TS, t);
    if (str != null) {
      row.put(STRING, str);
    }
    if (mStr != null) {
      row.put(MULTI_STRING, mStr);
    }
    if (d != null) {
      row.put(DOUBLE, d);
    }
    if (f != null) {
      row.put(FLOAT, f);
    }
    if (l != null) {
      row.put(LONG, l);
    }
    return row;
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  Closer closer;
  ColumnCache theCache;

  QueryableIndex index;

  @Before
  public void setup() throws IOException
  {
    closer = Closer.create();
    index = IndexBuilder.create(TestHelper.makeJsonMapper())
                        .tmpDir(temporaryFolder.newFolder())
                        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                        .schema(
                            new IncrementalIndexSchema.Builder()
                                .withDimensionsSpec(DIMS)
                                .withMetrics(new CountAggregatorFactory("chocula"))
                                .withRollup(false)
                                .build()
                        )
                        .rows(
                            RAW_ROWS.stream().sequential().map(r -> OLD_SCHOOL.parseBatch(r).get(0)).collect(Collectors.toList())
                        )
                        .buildMMappedIndex();

    closer.register(index);
    theCache = new ColumnCache(index, closer);
  }

  @After
  public void teardown() throws IOException
  {
    closer.close();
  }

  @Test
  public void testSingleValueSelector()
  {
    NoFilterVectorOffset offset = new NoFilterVectorOffset(4, 0, RAW_ROWS.size());
    QueryableIndexVectorColumnSelectorFactory factory = new QueryableIndexVectorColumnSelectorFactory(
        index,
        offset,
        theCache,
        VirtualColumns.EMPTY
    );

    // cannot make single value selector on multi-value string
    Assert.assertThrows(ISE.class, () -> factory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of(MULTI_STRING)));
    // we make nil selectors for number columns though
    Assert.assertTrue(factory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of(DOUBLE)) instanceof NilVectorSelector);
    Assert.assertTrue(factory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of(FLOAT)) instanceof NilVectorSelector);
    Assert.assertTrue(factory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of(LONG)) instanceof NilVectorSelector);

    // but we can for real multi-value strings
    SingleValueDimensionVectorSelector vectorSelector = factory.makeSingleValueDimensionSelector(
        DefaultDimensionSpec.of(STRING)
    );

    VectorObjectSelector objectSelector = factory.makeObjectSelector(STRING);

    int rowCounter = 0;
    while (!offset.isDone()) {
      int[] ints = vectorSelector.getRowVector();
      Assert.assertNotNull(ints);
      for (int i = 0; i < vectorSelector.getCurrentVectorSize(); i++) {
        Assert.assertEquals(RAW_ROWS.get(rowCounter + i).get(STRING), vectorSelector.lookupName(ints[i]));
      }

      Object[] objects = objectSelector.getObjectVector();
      for (int i = 0; i < vectorSelector.getCurrentVectorSize(); i++) {
        Assert.assertEquals("row " + i, RAW_ROWS.get(rowCounter + i).get(STRING), objects[i]);
      }
      rowCounter += objectSelector.getCurrentVectorSize();
      offset.advance();
    }
  }

  @Test
  public void testMultiValueSelector()
  {
    NoFilterVectorOffset offset = new NoFilterVectorOffset(4, 0, RAW_ROWS.size());
    QueryableIndexVectorColumnSelectorFactory factory = new QueryableIndexVectorColumnSelectorFactory(
        index,
        offset,
        theCache,
        VirtualColumns.EMPTY
    );

    // cannot make these for anything except for multi-value strings
    Assert.assertThrows(ISE.class, () -> factory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of(STRING)));
    Assert.assertThrows(ISE.class, () -> factory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of(DOUBLE)));
    Assert.assertThrows(ISE.class, () -> factory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of(FLOAT)));
    Assert.assertThrows(ISE.class, () -> factory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of(LONG)));

    // but we can for real multi-value strings
    MultiValueDimensionVectorSelector vectorSelector = factory.makeMultiValueDimensionSelector(
        DefaultDimensionSpec.of(MULTI_STRING)
    );

    VectorObjectSelector objectSelector = factory.makeObjectSelector(MULTI_STRING);

    int rowCounter = 0;
    while (!offset.isDone()) {
      IndexedInts[] indexedInts = vectorSelector.getRowVector();
      Assert.assertNotNull(indexedInts);
      for (int i = 0; i < vectorSelector.getCurrentVectorSize(); i++) {
        IndexedInts currentRow = indexedInts[i];
        if (currentRow.size() == 0) {
          Assert.assertNull(RAW_ROWS.get(rowCounter + i).get(MULTI_STRING));
        } else if (currentRow.size() == 1) {
          Assert.assertEquals(RAW_ROWS.get(rowCounter + i).get(MULTI_STRING), vectorSelector.lookupName(currentRow.get(0)));
        } else {
          // noinspection SSBasedInspection
          for (int j = 0; j < currentRow.size(); j++) {
            List expected = (List) RAW_ROWS.get(rowCounter + i).get(MULTI_STRING);
            Assert.assertEquals(expected.get(j), vectorSelector.lookupName(currentRow.get(j)));
          }
        }
      }

      Object[] objects = objectSelector.getObjectVector();
      for (int i = 0; i < vectorSelector.getCurrentVectorSize(); i++) {
        Assert.assertEquals("row " + i, RAW_ROWS.get(rowCounter + i).get(MULTI_STRING), objects[i]);
      }
      rowCounter += objectSelector.getCurrentVectorSize();
      offset.advance();
    }
  }

  @Test
  public void testNumericSelectors()
  {
    NoFilterVectorOffset offset = new NoFilterVectorOffset(4, 0, RAW_ROWS.size());
    QueryableIndexVectorColumnSelectorFactory factory = new QueryableIndexVectorColumnSelectorFactory(
        index,
        offset,
        theCache,
        VirtualColumns.EMPTY
    );

    // cannot make these for anything except for multi-value strings
    Assert.assertThrows(UOE.class, () -> factory.makeValueSelector(STRING));
    Assert.assertThrows(UOE.class, () -> factory.makeValueSelector(MULTI_STRING));

    VectorValueSelector doubleSelector = factory.makeValueSelector(DOUBLE);
    VectorValueSelector floatSelector = factory.makeValueSelector(FLOAT);
    VectorValueSelector longSelector = factory.makeValueSelector(LONG);

    int rowCounter = 0;
    while (!offset.isDone()) {
      double[] doubles = doubleSelector.getDoubleVector();
      boolean[] doubleNulls = doubleSelector.getNullVector();
      for (int i = 0; i < doubleSelector.getCurrentVectorSize(); i++) {
        final Object raw = RAW_ROWS.get(rowCounter + i).get(DOUBLE);
        if (doubleNulls != null && doubleNulls[i]) {
          Assert.assertNull(raw);
        } else {
          if (raw == null) {
            Assert.assertEquals(0.0, doubles[i], 0.0);
          } else {
            Assert.assertEquals((double) raw, doubles[i], 0.0);
          }
        }
      }

      float[] floats = floatSelector.getFloatVector();
      boolean[] floatNulls = floatSelector.getNullVector();
      for (int i = 0; i < floatSelector.getCurrentVectorSize(); i++) {
        final Object raw = RAW_ROWS.get(rowCounter + i).get(FLOAT);
        if (floatNulls != null && floatNulls[i]) {
          Assert.assertNull(raw);
        } else {
          if (raw == null) {
            Assert.assertEquals(0.0f, floats[i], 0.0);
          } else {
            Assert.assertEquals((float) raw, floats[i], 0.0);
          }
        }
      }

      long[] longs = longSelector.getLongVector();
      boolean[] longNulls = longSelector.getNullVector();
      for (int i = 0; i < longSelector.getCurrentVectorSize(); i++) {
        final Object raw = RAW_ROWS.get(rowCounter + i).get(LONG);
        if (longNulls != null && longNulls[i]) {
          Assert.assertNull(raw);
        } else {
          if (raw == null) {
            Assert.assertEquals(0L, longs[i], 0.0);
          } else {
            Assert.assertEquals((long) raw, longs[i]);
          }
        }
      }

      rowCounter += doubleSelector.getCurrentVectorSize();
      offset.advance();
    }
  }
}
