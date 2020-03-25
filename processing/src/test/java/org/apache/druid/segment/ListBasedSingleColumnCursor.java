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

import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;

public class ListBasedSingleColumnCursor<T> implements Cursor
{
  private final Class<T> type;
  private final List<T> rows;
  private int offset;

  public ListBasedSingleColumnCursor(Class<T> type, List<T> rows)
  {
    this.type = type;
    this.rows = rows;
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return new ColumnSelectorFactory()
    {
      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public ColumnValueSelector<T> makeColumnValueSelector(String columnName)
      {
        return new ListBasedColumnValueSelector();
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return null;
      }
    };
  }

  @Override
  public DateTime getTime()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void advance()
  {
    offset++;
  }

  @Override
  public void advanceUninterruptibly()
  {
    advance();
  }

  @Override
  public boolean isDone()
  {
    return offset == rows.size();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone();
  }

  @Override
  public void reset()
  {
    offset = 0;
  }

  private class ListBasedColumnValueSelector implements ColumnValueSelector<T>
  {
    @Override
    public double getDouble()
    {
      return Numbers.parseDouble(getObject());
    }

    @Override
    public float getFloat()
    {
      return Numbers.parseFloat(getObject());
    }

    @Override
    public long getLong()
    {
      return Numbers.parseLong(getObject());
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }

    @Override
    public boolean isNull()
    {
      return rows.get(offset) == null;
    }

    @Nullable
    @Override
    public T getObject()
    {
      return rows.get(offset);
    }

    @Override
    public Class<? extends T> classOfObject()
    {
      return type;
    }
  }
}
