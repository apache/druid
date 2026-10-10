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

package org.apache.druid.iceberg.input;

import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.vectorized.ColumnVector;
import org.apache.iceberg.arrow.vectorized.ColumnarBatch;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class IcebergArrowRowsAndColumns implements RowsAndColumns
{
  private final ColumnarBatch batch;
  private final Layout layout;
  private final Column[] columns;

  IcebergArrowRowsAndColumns(final ColumnarBatch batch, final Layout layout)
  {
    this.batch = batch;
    this.layout = layout;
    this.columns = new Column[batch.numCols()];
    for (int i = 0; i < batch.numCols(); i++) {
      columns[i] = new ColumnAccessorBasedColumn(
          new IcebergArrowColumnAccessor(batch.column(i), layout.fields.get(i).type(), batch.numRows())
      );
    }
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return layout.columnNames;
  }

  @Override
  public int numRows()
  {
    return batch.numRows();
  }

  @Nullable
  @Override
  public Column findColumn(final String name)
  {
    final Integer position = layout.columnPositions.get(name);
    return position == null ? null : columns[position];
  }

  @Nullable
  @Override
  public <T> T as(final Class<T> clazz)
  {
    return null;
  }

  static final class Layout
  {
    private final List<Types.NestedField> fields;
    private final List<String> columnNames;
    private final Map<String, Integer> columnPositions;

    Layout(final Schema schema)
    {
      this.fields = schema.columns();
      this.columnNames = Collections.unmodifiableList(
          fields.stream().map(Types.NestedField::name).collect(Collectors.toList())
      );
      final Map<String, Integer> positions = new LinkedHashMap<>();
      for (int i = 0; i < fields.size(); i++) {
        positions.put(fields.get(i).name(), i);
      }
      this.columnPositions = Collections.unmodifiableMap(positions);
    }
  }

  private static final class IcebergArrowColumnAccessor implements ColumnAccessor
  {
    private final ColumnVector vector;
    private final Type icebergType;
    private final ColumnType columnType;
    private final int numRows;

    IcebergArrowColumnAccessor(
        final ColumnVector vector,
        final Type icebergType,
        final int numRows
    )
    {
      this.vector = vector;
      this.icebergType = icebergType;
      this.columnType = toColumnType(icebergType);
      this.numRows = numRows;
    }

    @Override
    public ColumnType getType()
    {
      return columnType;
    }

    @Override
    public int numRows()
    {
      return numRows;
    }

    @Override
    public boolean isNull(final int rowNum)
    {
      return vector.isNullAt(rowNum);
    }

    @Nullable
    @Override
    public Object getObject(final int rowNum)
    {
      return isNull(rowNum) ? null : IcebergArrowInputSourceReader.extractValue(vector, icebergType, rowNum);
    }

    @Override
    public double getDouble(final int rowNum)
    {
      final Object value = getObject(rowNum);
      return value instanceof Number ? ((Number) value).doubleValue() : 0D;
    }

    @Override
    public float getFloat(final int rowNum)
    {
      final Object value = getObject(rowNum);
      return value instanceof Number ? ((Number) value).floatValue() : 0F;
    }

    @Override
    public long getLong(final int rowNum)
    {
      final Object value = getObject(rowNum);
      return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    @Override
    public int getInt(final int rowNum)
    {
      final Object value = getObject(rowNum);
      return value instanceof Number ? ((Number) value).intValue() : 0;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public int compareRows(final int lhsRowNum, final int rhsRowNum)
    {
      final Object lhs = getObject(lhsRowNum);
      final Object rhs = getObject(rhsRowNum);
      if (lhs == null) {
        return rhs == null ? 0 : -1;
      }
      if (rhs == null) {
        return 1;
      }
      if (lhs instanceof byte[] && rhs instanceof byte[]) {
        return Arrays.compareUnsigned((byte[]) lhs, (byte[]) rhs);
      }
      if (lhs instanceof Number && rhs instanceof Number) {
        return new BigDecimal(lhs.toString()).compareTo(new BigDecimal(rhs.toString()));
      }
      return ((Comparable) lhs).compareTo(rhs);
    }

    private static ColumnType toColumnType(final Type type)
    {
      switch (type.typeId()) {
        case INTEGER:
        case LONG:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case TIMESTAMP_NANO:
          return ColumnType.LONG;
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
          return ColumnType.DOUBLE;
        case BOOLEAN:
        case STRING:
        case BINARY:
        case FIXED:
        case UUID:
          return ColumnType.STRING;
        default:
          return ColumnType.NESTED_DATA;
      }
    }
  }
}
