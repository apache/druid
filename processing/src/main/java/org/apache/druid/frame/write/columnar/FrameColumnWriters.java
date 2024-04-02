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

package org.apache.druid.frame.write.columnar;

import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.Nullable;

public class FrameColumnWriters
{
  public static final byte TYPE_LONG = 1;
  public static final byte TYPE_FLOAT = 2;
  public static final byte TYPE_DOUBLE = 3;
  public static final byte TYPE_STRING = 4;
  public static final byte TYPE_COMPLEX = 5;
  public static final byte TYPE_STRING_ARRAY = 6;
  public static final byte TYPE_LONG_ARRAY = 7;
  public static final byte TYPE_FLOAT_ARRAY = 8;
  public static final byte TYPE_DOUBLE_ARRAY = 9;

  private FrameColumnWriters()
  {
    // No instantiation.
  }

  /**
   * Helper used by {@link ColumnarFrameWriterFactory}.
   *
   * @throws UnsupportedColumnTypeException if "type" cannot be handled
   */
  static FrameColumnWriter create(
      final ColumnSelectorFactory columnSelectorFactory,
      final MemoryAllocator allocator,
      final String column,
      final ColumnType type
  )
  {
    if (type == null) {
      throw new UnsupportedColumnTypeException(column, null);
    }

    switch (type.getType()) {
      case LONG:
        return makeLongWriter(columnSelectorFactory, allocator, column);
      case FLOAT:
        return makeFloatWriter(columnSelectorFactory, allocator, column);
      case DOUBLE:
        return makeDoubleWriter(columnSelectorFactory, allocator, column);
      case STRING:
        return makeStringWriter(columnSelectorFactory, allocator, column);
      case ARRAY:
        switch (type.getElementType().getType()) {
          case STRING:
            return makeStringArrayWriter(columnSelectorFactory, allocator, column);
          case LONG:
            return makeLongArrayWriter(columnSelectorFactory, allocator, column);
          case FLOAT:
            return makeFloatArrayWriter(columnSelectorFactory, allocator, column);
          case DOUBLE:
            return makeDoubleArrayWriter(columnSelectorFactory, allocator, column);
          default:
            throw new UnsupportedColumnTypeException(column, type);
        }
      case COMPLEX:
        return makeComplexWriter(columnSelectorFactory, allocator, column, type.getComplexTypeName());
      default:
        throw new UnsupportedColumnTypeException(column, type);
    }
  }

  private static LongFrameColumnWriter makeLongWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(columnName);
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new LongFrameColumnWriter(selector, allocator, hasNullsForNumericWriter(capabilities));
  }

  private static FloatFrameColumnWriter makeFloatWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(columnName);
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new FloatFrameColumnWriter(selector, allocator, hasNullsForNumericWriter(capabilities));
  }

  private static DoubleFrameColumnWriter makeDoubleWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(columnName);
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new DoubleFrameColumnWriter(selector, allocator, hasNullsForNumericWriter(capabilities));
  }

  private static StringFrameColumnWriter makeStringWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(columnName);
    final DimensionSelector selector = selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
    return new StringFrameColumnWriterImpl(
        selector,
        allocator,
        capabilities == null ? ColumnCapabilities.Capable.UNKNOWN : capabilities.hasMultipleValues()
    );
  }

  private static StringFrameColumnWriter makeStringArrayWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new StringArrayFrameColumnWriterImpl(selector, allocator);
  }

  private static NumericArrayFrameColumnWriter makeLongArrayWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new LongArrayFrameColumnWriter(selector, allocator);
  }

  private static NumericArrayFrameColumnWriter makeFloatArrayWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new FloatArrayFrameColumnWriter(selector, allocator);
  }

  private static NumericArrayFrameColumnWriter makeDoubleArrayWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new DoubleArrayFrameColumnWriter(selector, allocator);
  }

  private static ComplexFrameColumnWriter makeComplexWriter(
      final ColumnSelectorFactory selectorFactory,
      final MemoryAllocator allocator,
      final String columnName,
      @Nullable final String columnTypeName
  )
  {
    if (columnTypeName == null) {
      throw new ISE("No complexTypeName, cannot write column [%s]", columnName);
    }

    final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(columnTypeName);
    if (serde == null) {
      throw new ISE("No serde for complexTypeName[%s], cannot write column [%s]", columnTypeName, columnName);
    }

    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new ComplexFrameColumnWriter(selector, allocator, serde);
  }

  private static boolean hasNullsForNumericWriter(final ColumnCapabilities capabilities)
  {
    if (capabilities == null) {
      return true;
    } else if (capabilities.getType().isNumeric()) {
      return capabilities.hasNulls().isMaybeTrue();
    } else {
      // Reading
      return true;
    }
  }
}
