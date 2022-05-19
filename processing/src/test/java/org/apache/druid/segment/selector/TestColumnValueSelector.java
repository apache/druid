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

package org.apache.druid.segment.selector;

import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TestColumnValueSelector<T> implements ColumnValueSelector<Object>, Cursor
{
  private final Class<T> clazz;
  private final Supplier<Iterator<Object>> iteratorSupplier;
  private final DateTime time;

  private Iterator<Object> iterator;
  private Object value;

  public static <T> TestColumnValueSelector<T> of(Class<T> clazz, Collection<Object> collection, DateTime time)
  {
    return new TestColumnValueSelector<>(clazz, collection::iterator, time);
  }

  public static <T> TestColumnValueSelector<T> of(Class<T> clazz, Stream<Object> stream, DateTime time)
  {
    return new TestColumnValueSelector<>(clazz, stream::iterator, time);
  }

  protected TestColumnValueSelector(Class<T> clazz, Supplier<Iterator<Object>> iteratorSupplier, DateTime time)
  {
    this.clazz = clazz;
    this.iteratorSupplier = iteratorSupplier;
    this.time = time;
    this.iterator = iteratorSupplier.get();
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return new ColumnSelectorFactory()
    {
      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public ColumnValueSelector makeColumnValueSelector(String columnName)
      {
        return TestColumnValueSelector.this;
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
    return time;
  }

  @Override
  public void advance()
  {
    value = iterator.next();
  }

  @Override
  public void advanceUninterruptibly()
  {
    advance();
  }

  @Override
  public boolean isDone()
  {
    return !iterator.hasNext();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone();
  }

  @Override
  public void reset()
  {
    iterator = iteratorSupplier.get();
  }

  @Override
  public double getDouble()
  {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else {
      return Double.parseDouble(value.toString());
    }
  }

  @Override
  public float getFloat()
  {
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else {
      return Float.parseFloat(value.toString());
    }
  }

  @Override
  public long getLong()
  {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else {
      return Long.parseLong(value.toString());
    }
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
  }

  @Override
  public boolean isNull()
  {
    return value == null;
  }

  @Nullable
  @Override
  public Object getObject()
  {
    return value;
  }

  @Override
  public Class<? extends T> classOfObject()
  {
    return clazz;
  }
}
