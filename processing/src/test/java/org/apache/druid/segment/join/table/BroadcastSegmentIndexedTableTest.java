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

package org.apache.druid.segment.join.table;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.jackson.SegmentizerModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.loading.MMappedQueryableSegmentizerFactory;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class BroadcastSegmentIndexedTableTest extends InitializedNullHandlingTest
{
  private static final String STRING_COL_1 = "market";
  private static final String LONG_COL_1 = "longNumericNull";
  private static final String DOUBLE_COL_1 = "doubleNumericNull";
  private static final String FLOAT_COL_1 = "floatNumericNull";
  private static final String STRING_COL_2 = "partial_null_column";
  private static final String MULTI_VALUE_COLUMN = "placementish";
  private static final String DIM_NOT_EXISTS = "DIM_NOT_EXISTS";
  private static final String DATASOURCE = "DATASOURCE";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private QueryableIndexSegment backingSegment;
  private BroadcastSegmentIndexedTable broadcastTable;
  private List<String> columnNames;
  private final Set<String> keyColumns = ImmutableSet.<String>builder()
                                                     .add(STRING_COL_1)
                                                     .add(STRING_COL_2)
                                                     .add(LONG_COL_1)
                                                     .add(DOUBLE_COL_1)
                                                     .add(FLOAT_COL_1)
                                                     .add(MULTI_VALUE_COLUMN)
                                                     .add(DIM_NOT_EXISTS)
                                                     .build();

  @Before
  public void setup() throws IOException, SegmentLoadingException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerModule(new SegmentizerModule());
    final IndexIO indexIO = new IndexIO(mapper, () -> 0);
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), mapper)
            .addValue(IndexIO.class, indexIO)
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
    );

    final IndexMerger indexMerger =
        new IndexMergerV9(mapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
    Interval testInterval = Intervals.of("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z");
    IncrementalIndex data = TestIndex.makeRealtimeIndex("druid.sample.numeric.tsv");
    File segment = new File(temporaryFolder.newFolder(), "segment");
    File persisted = indexMerger.persist(
        data,
        testInterval,
        segment,
        new IndexSpec(),
        null
    );
    File factoryJson = new File(persisted, "factory.json");
    Assert.assertTrue(factoryJson.exists());
    SegmentizerFactory factory = mapper.readValue(factoryJson, SegmentizerFactory.class);
    Assert.assertTrue(factory instanceof MMappedQueryableSegmentizerFactory);

    DataSegment dataSegment = new DataSegment(
        DATASOURCE,
        testInterval,
        DateTimes.nowUtc().toString(),
        ImmutableMap.of(),
        columnNames,
        ImmutableList.of(),
        null,
        null,
        segment.getTotalSpace()
    );
    backingSegment = (QueryableIndexSegment) factory.factorize(dataSegment, segment, false, SegmentLazyLoadFailCallback.NOOP);

    columnNames = ImmutableList.<String>builder().add(ColumnHolder.TIME_COLUMN_NAME)
                                                 .addAll(backingSegment.asQueryableIndex().getColumnNames()).build();
    broadcastTable = new BroadcastSegmentIndexedTable(backingSegment, keyColumns, dataSegment.getVersion());
  }

  @Test
  public void testInitShouldGenerateCorrectTable()
  {
    Assert.assertEquals(1209, broadcastTable.numRows());
  }

  @Test
  public void testStringKeyColumn()
  {
    // lets try a few values out
    final String[] vals = new String[] {"spot", "total_market", "upfront"};
    checkIndexAndReader(STRING_COL_1, vals);
  }

  @Test
  public void testNullableStringKeyColumn()
  {
    final String[] vals = new String[] {null, "value"};
    checkIndexAndReader(STRING_COL_2, vals);
  }

  @Test
  public void testMultiValueStringKeyColumn()
  {
    final Object[] nonMatchingVals = new Object[] {ImmutableList.of("a", "preferred")};
    checkIndexAndReader(MULTI_VALUE_COLUMN, new Object[0], nonMatchingVals);
  }

  @Test
  public void testLongKeyColumn()
  {
    final Long[] vals = new Long[] {NullHandling.replaceWithDefault() ? 0L : null, 10L, 20L};
    checkIndexAndReader(LONG_COL_1, vals);
  }

  @Test
  public void testFloatKeyColumn()
  {
    final Float[] vals = new Float[] {NullHandling.replaceWithDefault() ? 0.0f : null, 10.0f, 20.0f};
    checkIndexAndReader(FLOAT_COL_1, vals);
  }

  @Test
  public void testDoubleKeyColumn()
  {
    final Double[] vals = new Double[] {NullHandling.replaceWithDefault() ? 0.0 : null, 10.0, 20.0};
    checkIndexAndReader(DOUBLE_COL_1, vals);
  }

  @Test
  public void testTimestampColumn()
  {
    checkNonIndexedReader(ColumnHolder.TIME_COLUMN_NAME);
  }

  @Test
  public void testStringNonKeyColumn()
  {
    checkNonIndexedReader("qualityNumericString");
  }

  @Test
  public void testLongNonKeyColumn()
  {
    checkNonIndexedReader("qualityLong");
  }

  @Test
  public void testFloatNonKeyColumn()
  {
    checkNonIndexedReader("qualityFloat");
  }

  @Test
  public void testDoubleNonKeyColumn()
  {
    checkNonIndexedReader("qualityDouble");
  }

  @Test
  public void testIsCacheable()
  {
    Assert.assertTrue(broadcastTable.isCacheable());
  }

  @Test
  public void testNonexistentColumn()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Column[-1] is not a valid column");
    broadcastTable.columnReader(columnNames.indexOf(DIM_NOT_EXISTS));
  }

  @Test
  public void testNonexistentColumnOutOfRange()
  {
    final int non = columnNames.size();
    expectedException.expect(IAE.class);
    expectedException.expectMessage(StringUtils.format("Column[%s] is not a valid column", non));
    broadcastTable.columnReader(non);
  }

  private void checkIndexAndReader(String columnName, Object[] vals)
  {
    checkIndexAndReader(columnName, vals, new Object[0]);
  }

  private void checkIndexAndReader(String columnName, Object[] vals, Object[] nonmatchingVals)
  {
    checkColumnSelectorFactory(columnName);
    try (final Closer closer = Closer.create()) {
      final int columnIndex = columnNames.indexOf(columnName);
      final IndexedTable.Reader reader = broadcastTable.columnReader(columnIndex);
      closer.register(reader);
      final IndexedTable.Index valueIndex = broadcastTable.columnIndex(columnIndex);

      // lets try a few values out
      for (Object val : vals) {
        final IntList valIndex = valueIndex.find(val);
        if (val == null) {
          Assert.assertEquals(0, valIndex.size());
        } else {
          Assert.assertTrue(valIndex.size() > 0);
          for (int i = 0; i < valIndex.size(); i++) {
            Assert.assertEquals(val, reader.read(valIndex.getInt(i)));
          }
        }
      }
      for (Object val : nonmatchingVals) {
        final IntList valIndex = valueIndex.find(val);
        Assert.assertEquals(0, valIndex.size());
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkNonIndexedReader(String columnName)
  {
    checkColumnSelectorFactory(columnName);
    try (final Closer closer = Closer.create()) {
      final int columnIndex = columnNames.indexOf(columnName);
      final int numRows = backingSegment.asStorageAdapter().getNumRows();
      final IndexedTable.Reader reader = broadcastTable.columnReader(columnIndex);
      closer.register(reader);
      final SimpleAscendingOffset offset = new SimpleAscendingOffset(numRows);
      final BaseColumn theColumn = backingSegment.asQueryableIndex()
                                                 .getColumnHolder(columnName)
                                                 .getColumn();
      closer.register(theColumn);
      final BaseObjectColumnValueSelector<?> selector = theColumn.makeColumnValueSelector(offset);
      // compare with selector make sure reader can read correct values
      for (int row = 0; row < numRows; row++) {
        offset.setCurrentOffset(row);
        Assert.assertEquals(selector.getObject(), reader.read(row));
      }
      // make sure it doesn't have an index since it isn't a key column
      try {
        Assert.assertEquals(null, broadcastTable.columnIndex(columnIndex));
      }
      catch (IAE iae) {
        Assert.assertEquals(StringUtils.format("Column[%d] is not a key column", columnIndex), iae.getMessage());
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkColumnSelectorFactory(String columnName)
  {
    try (final Closer closer = Closer.create()) {
      final int numRows = backingSegment.asStorageAdapter().getNumRows();

      final SimpleAscendingOffset offset = new SimpleAscendingOffset(numRows);
      final BaseColumn theColumn = backingSegment.asQueryableIndex()
                                                 .getColumnHolder(columnName)
                                                 .getColumn();
      closer.register(theColumn);
      final BaseObjectColumnValueSelector<?> selector = theColumn.makeColumnValueSelector(offset);

      ColumnSelectorFactory tableFactory = broadcastTable.makeColumnSelectorFactory(offset, false, closer);
      final BaseObjectColumnValueSelector<?> tableSelector = tableFactory.makeColumnValueSelector(columnName);

      // compare with base segment selector to make sure tables selector can read correct values
      for (int row = 0; row < numRows; row++) {
        offset.setCurrentOffset(row);
        Assert.assertEquals(selector.getObject(), tableSelector.getObject());
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
