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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.SemanticCreator;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.segment.CloseableShapeshifter;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class QueryableIndexRowsAndColumns implements RowsAndColumns, AutoCloseable, CloseableShapeshifter
{
  private static final Map<Class<?>, Function<QueryableIndexRowsAndColumns, ?>> AS_MAP = RowsAndColumns
      .makeAsMap(QueryableIndexRowsAndColumns.class);

  private final QueryableIndex index;

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Closer closer = Closer.create();
  private final AtomicInteger numRows = new AtomicInteger(-1);

  public QueryableIndexRowsAndColumns(
      QueryableIndex index
  )
  {
    this.index = index;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return index.getColumnNames();
  }

  @Override
  public int numRows()
  {
    int retVal = numRows.get();
    if (retVal < 0) {
      retVal = index.getNumRows();
      numRows.set(retVal);
    }
    return retVal;
  }

  @Nullable
  @Override
  public Column findColumn(String name)
  {
    if (closed.get()) {
      throw DruidException.defensive("Cannot be accessed after being closed!?");
    }
    final ColumnHolder columnHolder = index.getColumnHolder(name);
    if (columnHolder == null) {
      return null;
    }

    return closer.register(new ColumnHolderRACColumn(columnHolder));
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    //noinspection ReturnOfNull
    return (T) AS_MAP.getOrDefault(clazz, arg -> null).apply(this);
  }

  @Override
  public void close() throws IOException
  {
    if (closed.compareAndSet(false, true)) {
      closer.close();
    }
  }

  @SuppressWarnings("unused")
  @SemanticCreator
  public StorageAdapter toStorageAdapter()
  {
    return new QueryableIndexStorageAdapter(index);
  }

  @SuppressWarnings("unused")
  @SemanticCreator
  public QueryableIndex toQueryableIndex()
  {
    return index;
  }
}
