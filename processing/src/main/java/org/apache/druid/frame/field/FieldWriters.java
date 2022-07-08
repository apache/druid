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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.Nullable;
import java.util.List;

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
      final ColumnType columnType
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
        return makeStringWriter(columnSelectorFactory, columnName);
      case ARRAY:
        switch (columnType.getElementType().getType()) {
          case STRING:
            return makeStringArrayWriter(columnSelectorFactory, columnName);
          default:
            throw new UnsupportedColumnTypeException(columnName, columnType);
        }
      case COMPLEX:
        return makeComplexWriter(columnSelectorFactory, columnName, columnType.getComplexTypeName());
      default:
        throw new UnsupportedColumnTypeException(columnName, columnType);
    }
  }

  private static FieldWriter makeLongWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new LongFieldWriter(selector);
  }

  private static FieldWriter makeFloatWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new FloatFieldWriter(selector);
  }

  private static FieldWriter makeDoubleWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName
  )
  {
    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new DoubleFieldWriter(selector);
  }

  private static FieldWriter makeStringWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName
  )
  {
    final DimensionSelector selector = selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
    return new StringFieldWriter(selector);
  }

  private static FieldWriter makeStringArrayWriter(
      final ColumnSelectorFactory selectorFactory,
      final String columnName
  )
  {
    //noinspection unchecked
    final ColumnValueSelector<List<String>> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new StringArrayFieldWriter(selector);
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

    final ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(columnName);
    return new ComplexFieldWriter(serde, selector);
  }
}
