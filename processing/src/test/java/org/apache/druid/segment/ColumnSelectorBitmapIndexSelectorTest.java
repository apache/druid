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

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringDictionaryEncodedColumn;
import org.apache.druid.segment.data.Indexed;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ColumnSelectorBitmapIndexSelectorTest
{
  private static final String STRING_DICTIONARY_COLUMN_NAME = "string";
  private static final String NON_STRING_DICTIONARY_COLUMN_NAME = "not-string";

  BitmapFactory bitmapFactory;
  VirtualColumns virtualColumns;
  ColumnSelector index;

  ColumnSelectorBitmapIndexSelector bitmapIndexSelector;

  @Before
  public void setup()
  {
    bitmapFactory = EasyMock.createMock(BitmapFactory.class);
    virtualColumns = EasyMock.createMock(VirtualColumns.class);
    index = EasyMock.createMock(ColumnSelector.class);
    bitmapIndexSelector = new ColumnSelectorBitmapIndexSelector(bitmapFactory, virtualColumns, index);

    EasyMock.expect(virtualColumns.getVirtualColumn(STRING_DICTIONARY_COLUMN_NAME)).andReturn(null).anyTimes();
    EasyMock.expect(virtualColumns.getVirtualColumn(NON_STRING_DICTIONARY_COLUMN_NAME)).andReturn(null).anyTimes();

    ColumnHolder holder = EasyMock.createMock(ColumnHolder.class);
    EasyMock.expect(index.getColumnHolder(STRING_DICTIONARY_COLUMN_NAME)).andReturn(holder).anyTimes();
    StringDictionaryEncodedColumn stringColumn = EasyMock.createMock(StringDictionaryEncodedColumn.class);
    EasyMock.expect(holder.getCapabilities()).andReturn(
        ColumnCapabilitiesImpl.createDefault()
                              .setType(ColumnType.STRING)
                              .setDictionaryEncoded(true)
                              .setDictionaryValuesUnique(true)
                              .setDictionaryValuesSorted(true)
                              .setHasBitmapIndexes(true)
    ).anyTimes();
    EasyMock.expect(holder.getColumn()).andReturn(stringColumn).anyTimes();
    BitmapIndex someIndex = EasyMock.createMock(BitmapIndex.class);
    EasyMock.expect(holder.getBitmapIndex()).andReturn(someIndex).anyTimes();
    ImmutableBitmap someBitmap = EasyMock.createMock(ImmutableBitmap.class);
    EasyMock.expect(someIndex.getIndex("foo")).andReturn(0).anyTimes();
    EasyMock.expect(someIndex.getBitmap(0)).andReturn(someBitmap).anyTimes();
    EasyMock.expect(someIndex.getBitmapForValue("foo")).andReturn(someBitmap).anyTimes();


    ColumnHolder nonStringHolder = EasyMock.createMock(ColumnHolder.class);
    EasyMock.expect(index.getColumnHolder(NON_STRING_DICTIONARY_COLUMN_NAME)).andReturn(nonStringHolder).anyTimes();

    EasyMock.expect(nonStringHolder.getCapabilities()).andReturn(
        ColumnCapabilitiesImpl.createDefault()
                              .setType(ColumnType.ofComplex("testBlob"))
                              .setDictionaryEncoded(true)
                              .setDictionaryValuesUnique(true)
                              .setDictionaryValuesSorted(true)
                              .setHasBitmapIndexes(true)
                              .setFilterable(true)
    ).anyTimes();

    EasyMock.replay(bitmapFactory, virtualColumns, index, holder, stringColumn, nonStringHolder, someIndex, someBitmap);
  }

  @Test
  public void testStringDictionaryUseIndex()
  {
    BitmapIndex bitmapIndex = bitmapIndexSelector.getBitmapIndex(STRING_DICTIONARY_COLUMN_NAME);
    Assert.assertNotNull(bitmapIndex);
    Indexed<String> vals = bitmapIndexSelector.getDimensionValues(STRING_DICTIONARY_COLUMN_NAME);

    Assert.assertNotNull(vals);

    ImmutableBitmap valueIndex = bitmapIndexSelector.getBitmapIndex(STRING_DICTIONARY_COLUMN_NAME, "foo");
    Assert.assertNotNull(valueIndex);
    EasyMock.verify(bitmapFactory, virtualColumns, index);
  }

  @Test
  public void testNonStringDictionaryDoNotUseIndex()
  {
    BitmapIndex bitmapIndex = bitmapIndexSelector.getBitmapIndex(NON_STRING_DICTIONARY_COLUMN_NAME);
    Assert.assertNull(bitmapIndex);
    Indexed<String> vals = bitmapIndexSelector.getDimensionValues(NON_STRING_DICTIONARY_COLUMN_NAME);

    Assert.assertNull(vals);

    ImmutableBitmap valueIndex = bitmapIndexSelector.getBitmapIndex(NON_STRING_DICTIONARY_COLUMN_NAME, "foo");
    Assert.assertNull(valueIndex);
    EasyMock.verify(bitmapFactory, virtualColumns, index);
  }
}
