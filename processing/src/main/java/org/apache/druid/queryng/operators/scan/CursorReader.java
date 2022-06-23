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

package org.apache.druid.queryng.operators.scan;

import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * The cursor reader is a leaf operator which uses a cursor to access data,
 * which is returned via the operator protocol. Converts cursor data into one
 * of two supported Druid formats. Enforces a query row limit.
 * <p>
 * Unlike most operators, this one is created on the fly by its parent
 * to scan a specific query known only at runtime. A storage adapter may
 * choose to create one or more queries: each is handled by an instance of this
 * class.
 * <p>
 * As a small performance boost, we create a layer of "accessors" on top of
 * the column selectors. The accesors encode the column-specific logic to
 * avoid the need for if-statements on every row.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryEngine}
 */
public class CursorReader implements Iterator<List<?>>
{
  private final Cursor cursor;
  private final List<String> selectedColumns;
  private final long limit;
  private final int batchSize;
  private final ResultFormat resultFormat;
  private final List<Supplier<Object>> columnAccessors;
  private final boolean hasTimeout;
  private final long timeoutAt;
  private final String queryId;
  private long targetCount;
  private long rowCount;

  public CursorReader(
      final Cursor cursor,
      final List<String> selectedColumns,
      final long limit,
      final int batchSize,
      final ResultFormat resultFormat,
      final boolean isLegacy,
      final long timeoutAt,
      final String queryId
  )
  {
    this.cursor = cursor;
    this.selectedColumns = selectedColumns;
    this.limit = limit;
    this.batchSize = batchSize;
    this.resultFormat = resultFormat;
    this.columnAccessors = buildAccessors(isLegacy);
    this.hasTimeout = timeoutAt < Long.MAX_VALUE;
    this.timeoutAt = timeoutAt;
    this.queryId = queryId;
  }

  private List<Supplier<Object>> buildAccessors(final boolean isLegacy)
  {
    List<Supplier<Object>> accessors = new ArrayList<>(selectedColumns.size());
    for (String column : selectedColumns) {
      final Supplier<Object> accessor;
      final BaseObjectColumnValueSelector<?> selector;
      if (isLegacy && ScanQueryOperator.LEGACY_TIMESTAMP_KEY.equals(column)) {
        selector = cursor.getColumnSelectorFactory()
                         .makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
        accessor = new Supplier<Object>() {
          @Override
          public Object get()
          {
            return DateTimes.utc((Long) selector.getObject());
          }
        };
      } else {
        selector = cursor.getColumnSelectorFactory().makeColumnValueSelector(column);
        if (selector == null) {
          accessor = new Supplier<Object>() {
            @Override
            public Object get()
            {
              return null;
            }
          };
        } else {
          accessor = new Supplier<Object>() {
            @Override
            public Object get()
            {
              return selector.getObject();
            }
          };
        }
      }
      accessors.add(accessor);
    }
    return accessors;
  }

  @Override
  public boolean hasNext()
  {
    return !cursor.isDone() && rowCount < limit;
  }

  @Override
  public List<?> next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    if (hasTimeout && System.currentTimeMillis() >= timeoutAt) {
      throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out", queryId));
    }
    targetCount = Math.min(limit - rowCount, rowCount + batchSize);
    switch (resultFormat) {
      case RESULT_FORMAT_LIST:
        return nextAsListOfMaps();
      case RESULT_FORMAT_COMPACTED_LIST:
        return nextAsCompactList();
      default:
        throw new UOE("resultFormat [%s] is not supported", resultFormat.toString());
    }
  }

  private void advance()
  {
    cursor.advance();
    rowCount++;
  }

  private boolean hasNextRow()
  {
    return !cursor.isDone() && rowCount < targetCount;
  }

  private Object getColumnValue(int i)
  {
    return columnAccessors.get(i).get();
  }

  /**
   * Convert a cursor row into a simple list of maps, where each map
   * represents a single event, and each map entry represents a column.
   */
  public List<?> nextAsListOfMaps()
  {
    final List<Map<String, Object>> events = new ArrayList<>(batchSize);
    while (hasNextRow()) {
      final Map<String, Object> theEvent = new LinkedHashMap<>();
      for (int j = 0; j < selectedColumns.size(); j++) {
        theEvent.put(selectedColumns.get(j), getColumnValue(j));
      }
      events.add(theEvent);
      advance();
    }
    return events;
  }

  public List<?> nextAsCompactList()
  {
    final List<List<Object>> events = new ArrayList<>(batchSize);
    while (hasNextRow()) {
      final List<Object> theEvent = new ArrayList<>(selectedColumns.size());
      for (int j = 0; j < selectedColumns.size(); j++) {
        theEvent.add(getColumnValue(j));
      }
      events.add(theEvent);
      advance();
    }
    return events;
  }
}
