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

package org.apache.druid.query.rowsandcols.concrete;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ReadableOffset;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements a RowsAndColumns column based on a ColumnHolder.  There is a bit of an impedence mis-match between
 * these interfaces in that the ColumnHolder gets at the values of a row by using a BaseColumn which in turn
 * hands off to `ColumnValueSelector` to actually build the thing.  The ColumnAccessor from RowsAndColumns, however,
 * believes that it can provide effectively direct access to values.
 * <p>
 * It would be really good to eventually reach down and interact directly with the columns rather than doing this
 * round-about stuff.
 * <p>
 * This implementation is also very bad with Objects and data types, it masterfully avoids almost every optimization
 * that we have in place.  This should also be revisited at some point in time.
 */
public class ColumnHolderRACColumn implements Column, Closeable
{
  private final ColumnHolder holder;

  private BaseColumn baseColumn;

  public ColumnHolderRACColumn(
      ColumnHolder holder
  )
  {
    this.holder = holder;
  }

  @Nonnull
  @Override
  public ColumnAccessor toAccessor()
  {
    final BaseColumn baseColumn = getBaseColumn();

    AtomicInteger offset = new AtomicInteger(0);
    final ColumnValueSelector<?> valueSelector = baseColumn.makeColumnValueSelector(
        new AtomicIntegerReadableOffset(offset)
    );
    final ColumnType columnType = holder.getCapabilities().toColumnType();
    final Comparator<Object> comparator = Comparator.nullsFirst(columnType.getStrategy());

    return new ColumnAccessor()
    {
      @Override
      public ColumnType getType()
      {
        return columnType;
      }

      @Override
      public int numRows()
      {
        return holder.getLength();
      }

      @Override
      public boolean isNull(int rowNum)
      {
        offset.set(rowNum);
        return valueSelector.getObject() == null;
      }

      @Nullable
      @Override
      public Object getObject(int rowNum)
      {
        offset.set(rowNum);
        return valueSelector.getObject();
      }

      @Override
      public double getDouble(int rowNum)
      {
        offset.set(rowNum);
        return valueSelector.getDouble();

      }

      @Override
      public float getFloat(int rowNum)
      {
        offset.set(rowNum);
        return valueSelector.getFloat();

      }

      @Override
      public long getLong(int rowNum)
      {
        offset.set(rowNum);
        return valueSelector.getLong();
      }

      @Override
      public int getInt(int rowNum)
      {
        offset.set(rowNum);
        return (int) valueSelector.getLong();

      }

      @Override
      public int compareRows(int lhsRowNum, int rhsRowNum)
      {
        return comparator.compare(getObject(lhsRowNum), getObject(rhsRowNum));
      }
    };
  }

  @Nullable
  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    return null;
  }

  @Override
  public void close() throws IOException
  {
    if (baseColumn != null) {
      baseColumn.close();
    }
  }

  public BaseColumn getBaseColumn()
  {
    if (baseColumn == null) {
      baseColumn = holder.getColumn();
    }
    return baseColumn;
  }

  private static class AtomicIntegerReadableOffset implements ReadableOffset
  {
    private final AtomicInteger offset;

    public AtomicIntegerReadableOffset(AtomicInteger offset)
    {
      this.offset = offset;
    }

    @Override
    public int getOffset()
    {
      return offset.get();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }
  }
}
