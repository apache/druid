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

package org.apache.druid.frame.field;

import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.write.RowBasedFrameWriterFactory;
import org.apache.druid.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.frame.write.cast.TypeCastSelectors;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.Nullable;

/**
 * Helper used to write field values to row-based frames or {@link RowKey}.
 *
 * Most callers should use {@link org.apache.druid.frame.write.FrameWriters} to build frames from
 * {@link ColumnSelectorFactory}, rather than using this class directly.
 */
public class FieldWriters
{
  private FieldWriters()
  {
    // No instantiation.
  }

  /**
   * Helper used by {@link RowBasedFrameWriterFactory}.
   *
   * The returned {@link FieldWriter} objects are not thread-safe.
   *
   * @throws UnsupportedColumnTypeException if "type" cannot be handled
   */
  public static FieldWriter create(
      final ColumnSelectorFactory columnSelectorFactory,
      final String columnName,
      final ColumnType columnType,
      final boolean removeNullBytes
  )
  {
    if (columnType == null) {
      throw new UnsupportedColumnTypeException(columnName, null);
    }

    switch (columnType.getType()) {
      case LONG:
        return makeLongWriter(columnSelectorFactory, columnName);

      case FLOAT:
        return makeFloatWriter(columnSelectorFactory, columnName);

      case DOUBLE:
        return makeDoubleWriter(columnSelectorFactory, columnName);

      case STRING:
        return makeStringWriter(columnSelectorFactory, columnName, removeNullBytes);

      case COMPLEX:
        return makeComplexWriter(columnSelectorFactory, columnName, columnType.getComplexTypeName());

      case ARRAY:
        switch (columnType.getElementType().getType()) {
          case STRING:
            return makeStringArrayWriter(columnSelectorFactory, columnName, removeNullBytes);
          case LONG:
            return makeLongArrayWriter(columnSelectorFactory, columnName);
          case FLOAT:
            return makeFloatArrayWriter(columnSelectorFactory, columnName);
          case DOUBLE:
            return makeDoubleArrayWriter(columnSelectorFactory, columnName);
        }
      default:
        throw new UnsupportedColumnTypeException(columnName, columnType);
    }
  }

  private static FieldWriter makeLongWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(selectorFactory, columnName, ColumnType.LONG);
    return LongFieldWriter.forPrimitive(selector);
  }

  private static FieldWriter makeFloatWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(selectorFactory, columnName, ColumnType.FLOAT);
    return FloatFieldWriter.forPrimitive(selector);
  }

  private static FieldWriter makeDoubleWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(selectorFactory, columnName, ColumnType.DOUBLE);
    return DoubleFieldWriter.forPrimitive(selector);
  }

  private static FieldWriter makeStringWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName,
      final boolean removeNullBytes
  )
  {
    final DimensionSelector selector = selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
    return new StringFieldWriter(selector, removeNullBytes);
  }

  private static FieldWriter makeStringArrayWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName,
      final boolean removeNullBytes
  )
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(selectorFactory, columnName, ColumnType.STRING_ARRAY);
    return new StringArrayFieldWriter(selector, removeNullBytes);
  }

  private static FieldWriter makeLongArrayWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(selectorFactory, columnName, ColumnType.LONG_ARRAY);
    return NumericArrayFieldWriter.getLongArrayFieldWriter(selector);
  }

  private static FieldWriter makeFloatArrayWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(selectorFactory, columnName, ColumnType.FLOAT_ARRAY);
    return NumericArrayFieldWriter.getFloatArrayFieldWriter(selector);
  }

  private static FieldWriter makeDoubleArrayWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(selectorFactory, columnName, ColumnType.DOUBLE_ARRAY);
    return NumericArrayFieldWriter.getDoubleArrayFieldWriter(selector);
  }

  private static FieldWriter makeComplexWriter(
      final ColumnSelectorFactory selectorFactory,
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

    final ColumnValueSelector<?> selector =
        TypeCastSelectors.makeColumnValueSelector(selectorFactory, columnName, ColumnType.ofComplex(columnTypeName));
    return new ComplexFieldWriter(serde, selector);
  }
}
