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
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.StringDictionaryEncodedColumn;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ColumnSelectorColumnIndexSelectorTest
{
  private static final String STRING_DICTIONARY_COLUMN_NAME = "string";
  private static final String NON_STRING_DICTIONARY_COLUMN_NAME = "not-string";

  BitmapFactory bitmapFactory;
  VirtualColumns virtualColumns;
  ColumnSelector index;

  ColumnSelectorColumnIndexSelector indexSelector;
  ColumnIndexSupplier indexSupplier;

  @Before
  public void setup()
  {
    bitmapFactory = EasyMock.createMock(BitmapFactory.class);
    virtualColumns = EasyMock.createMock(VirtualColumns.class);
    index = EasyMock.createMock(ColumnSelector.class);
    indexSelector = new ColumnSelectorColumnIndexSelector(bitmapFactory, virtualColumns, index);
    indexSupplier = EasyMock.createMock(ColumnIndexSupplier.class);

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
    EasyMock.expect(holder.getIndexSupplier()).andReturn(indexSupplier).anyTimes();
    StringValueSetIndex someIndex = EasyMock.createMock(StringValueSetIndex.class);
    EasyMock.expect(indexSupplier.getIndex(StringValueSetIndex.class)).andReturn(someIndex).anyTimes();
    DictionaryEncodedStringValueIndex valueIndex = EasyMock.createMock(DictionaryEncodedStringValueIndex.class);
    EasyMock.expect(indexSupplier.getIndex(DictionaryEncodedStringValueIndex.class)).andReturn(valueIndex).anyTimes();
    ImmutableBitmap someBitmap = EasyMock.createMock(ImmutableBitmap.class);
    EasyMock.expect(valueIndex.getIndex("foo")).andReturn(0).anyTimes();
    EasyMock.expect(valueIndex.getBitmap(0)).andReturn(someBitmap).anyTimes();

    EasyMock.expect(someIndex.getBitmapForValue("foo")).andReturn(someBitmap).anyTimes();


    ColumnHolder nonStringHolder = EasyMock.createMock(ColumnHolder.class);
    EasyMock.expect(index.getColumnHolder(NON_STRING_DICTIONARY_COLUMN_NAME)).andReturn(nonStringHolder).anyTimes();
    EasyMock.expect(nonStringHolder.getIndexSupplier()).andReturn(null).anyTimes();
    EasyMock.expect(nonStringHolder.getCapabilities()).andReturn(
        ColumnCapabilitiesImpl.createDefault()
                              .setType(ColumnType.ofComplex("testBlob"))
                              .setDictionaryEncoded(true)
                              .setDictionaryValuesUnique(true)
                              .setDictionaryValuesSorted(true)
                              .setHasBitmapIndexes(true)
                              .setFilterable(true)
    ).anyTimes();

    EasyMock.replay(bitmapFactory, virtualColumns, index, indexSupplier, holder, stringColumn, nonStringHolder, someIndex, valueIndex, someBitmap);
  }

  @Test
  public void testStringDictionaryUseIndex()
  {
    DictionaryEncodedStringValueIndex bitmapIndex = indexSelector.as(
        STRING_DICTIONARY_COLUMN_NAME,
        DictionaryEncodedStringValueIndex.class
    );
    Assert.assertNotNull(bitmapIndex);

    StringValueSetIndex valueIndex = indexSelector.as(STRING_DICTIONARY_COLUMN_NAME, StringValueSetIndex.class);
    Assert.assertNotNull(valueIndex);
    ImmutableBitmap valueBitmap = valueIndex.getBitmapForValue("foo");
    Assert.assertNotNull(valueBitmap);
    EasyMock.verify(bitmapFactory, virtualColumns, index, indexSupplier);
  }

  @Test
  public void testNonStringDictionaryDoNotUseIndex()
  {
    DictionaryEncodedStringValueIndex bitmapIndex = indexSelector.as(
        NON_STRING_DICTIONARY_COLUMN_NAME,
        DictionaryEncodedStringValueIndex.class
    );
    Assert.assertNull(bitmapIndex);

    StringValueSetIndex valueIndex = indexSelector.as(NON_STRING_DICTIONARY_COLUMN_NAME, StringValueSetIndex.class);
    Assert.assertNull(valueIndex);
    EasyMock.verify(bitmapFactory, virtualColumns, index, indexSupplier);
  }
}
