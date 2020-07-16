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

import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

public class RowBasedColumnSelectorFactoryTest
{
  private static final String STRING_COLUMN_NAME = "string";
  private static final String LONG_COLUMN_NAME = "long";
  private static final String FLOAT_COLUMN_NAME = "float";
  private static final String DOUBLE_COLUMN_NAME = "double";
  private static final String COMPLEX_COLUMN_NAME = "complex";

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add(ColumnHolder.TIME_COLUMN_NAME, ValueType.LONG)
                                                                .add(STRING_COLUMN_NAME, ValueType.STRING)
                                                                .add(LONG_COLUMN_NAME, ValueType.LONG)
                                                                .add(FLOAT_COLUMN_NAME, ValueType.FLOAT)
                                                                .add(DOUBLE_COLUMN_NAME, ValueType.DOUBLE)
                                                                .add(COMPLEX_COLUMN_NAME, ValueType.COMPLEX)
                                                                .build();

  @Test
  public void testCapabilitiesTime()
  {
    // time column takes a special path
    ColumnCapabilities caps =
        RowBasedColumnSelectorFactory.getColumnCapabilities(ROW_SIGNATURE, ColumnHolder.TIME_COLUMN_NAME);
    Assert.assertEquals(ValueType.LONG, caps.getType());
    Assert.assertFalse(caps.hasBitmapIndexes());
    Assert.assertFalse(caps.isDictionaryEncoded());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertFalse(caps.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
  }

  @Test
  public void testCapabilitiesString()
  {
    ColumnCapabilities caps =
        RowBasedColumnSelectorFactory.getColumnCapabilities(ROW_SIGNATURE, STRING_COLUMN_NAME);
    Assert.assertEquals(ValueType.STRING, caps.getType());
    Assert.assertFalse(caps.hasBitmapIndexes());
    Assert.assertFalse(caps.isDictionaryEncoded());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertTrue(caps.hasMultipleValues().isUnknown());
    Assert.assertFalse(caps.hasSpatialIndexes());
  }

  @Test
  public void testCapabilitiesLong()
  {
    ColumnCapabilities caps =
        RowBasedColumnSelectorFactory.getColumnCapabilities(ROW_SIGNATURE, LONG_COLUMN_NAME);
    Assert.assertEquals(ValueType.LONG, caps.getType());
    Assert.assertFalse(caps.hasBitmapIndexes());
    Assert.assertFalse(caps.isDictionaryEncoded());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertFalse(caps.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
  }

  @Test
  public void testCapabilitiesFloat()
  {
    ColumnCapabilities caps =
        RowBasedColumnSelectorFactory.getColumnCapabilities(ROW_SIGNATURE, FLOAT_COLUMN_NAME);
    Assert.assertEquals(ValueType.FLOAT, caps.getType());
    Assert.assertFalse(caps.hasBitmapIndexes());
    Assert.assertFalse(caps.isDictionaryEncoded());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertFalse(caps.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
  }

  @Test
  public void testCapabilitiesDouble()
  {
    ColumnCapabilities caps =
        RowBasedColumnSelectorFactory.getColumnCapabilities(ROW_SIGNATURE, DOUBLE_COLUMN_NAME);
    Assert.assertEquals(ValueType.DOUBLE, caps.getType());
    Assert.assertFalse(caps.hasBitmapIndexes());
    Assert.assertFalse(caps.isDictionaryEncoded());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertFalse(caps.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
  }

  @Test
  public void testCapabilitiesComplex()
  {
    ColumnCapabilities caps =
        RowBasedColumnSelectorFactory.getColumnCapabilities(ROW_SIGNATURE, COMPLEX_COLUMN_NAME);
    Assert.assertEquals(ValueType.COMPLEX, caps.getType());
    Assert.assertFalse(caps.hasBitmapIndexes());
    Assert.assertFalse(caps.isDictionaryEncoded());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertTrue(caps.hasMultipleValues().isUnknown());
    Assert.assertFalse(caps.hasSpatialIndexes());
  }

  @Test
  public void testCapabilitiesUnknownColumn()
  {
    ColumnCapabilities caps =
        RowBasedColumnSelectorFactory.getColumnCapabilities(ROW_SIGNATURE, "wat");
    Assert.assertNull(caps);
  }
}
