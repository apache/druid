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

package org.apache.druid.query.scan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

class OrderBySequence extends BaseSequence<ScanResultValue, Iterator<ScanResultValue>>
{
  public OrderBySequence(IteratorMaker<ScanResultValue, Iterator<ScanResultValue>> maker)
  {
    super(maker);
  }

  static class OrderByIteratorMaker
      implements BaseSequence.IteratorMaker<ScanResultValue, Iterator<ScanResultValue>>
  {

    private final boolean legacy;
    private final Cursor cursor;
    private final boolean hasTimeout;
    private final long timeoutAt;
    private final ScanQuery query;
    private final SegmentId segmentId;
    private final List<String> allColumns;
    private final ResponseContext responseContext;
    private final Set<Long> topKOffset;

    public OrderByIteratorMaker(
        boolean legacy,
        Cursor cursor,
        boolean hasTimeout,
        long timeoutAt,
        ScanQuery query,
        SegmentId segmentId,
        List<String> allColumns,
        ResponseContext responseContext,
        Set<Long> topKOffset
    )
    {
      this.legacy = legacy;
      this.cursor = cursor;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
      this.query = query;
      this.segmentId = segmentId;
      this.allColumns = allColumns;
      this.responseContext = responseContext;
      this.topKOffset = topKOffset;
    }

    @Override
    public Iterator<ScanResultValue> make()
    {
      final List<BaseObjectColumnValueSelector> columnSelectors = new ArrayList<>(allColumns.size());

      for (String column : allColumns) {
        final BaseObjectColumnValueSelector selector;

        if (legacy && ScanQueryEngine.LEGACY_TIMESTAMP_KEY.equals(column)) {
          selector = cursor.getColumnSelectorFactory()
                           .makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
        } else {
          selector = cursor.getColumnSelectorFactory().makeColumnValueSelector(column);
        }

        columnSelectors.add(selector);
      }

      final int batchSize = query.getBatchSize();
      return new Iterator<ScanResultValue>()
      {
        private long offset = 0;

        @Override
        public boolean hasNext()
        {
          return !cursor.isDone();
        }

        @Override
        public ScanResultValue next()
        {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          if (hasTimeout && System.currentTimeMillis() >= timeoutAt) {
            throw new QueryTimeoutException(StringUtils.nonStrictFormat(
                "Query [%s] timed out",
                query.getId()
            ));
          }
          final long lastOffset = offset;
          final Object events;
          final ScanQuery.ResultFormat resultFormat = query.getResultFormat();
          if (ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST.equals(resultFormat)) {
            events = rowsToCompactedList();
          } else if (ScanQuery.ResultFormat.RESULT_FORMAT_LIST.equals(resultFormat)) {
            events = rowsToList();
          } else {
            throw new UOE("resultFormat[%s] is not supported", resultFormat.toString());
          }
          responseContext.addRowScanCount(offset - lastOffset);
          return new ScanResultValue(segmentId.toString(), allColumns, events);
        }

        @Override
        public void remove()
        {
          throw new UnsupportedOperationException();
        }

        private List<List<Object>> rowsToCompactedList()
        {
          final List<List<Object>> events = new ArrayList<>(batchSize);
          for (; !cursor.isDone(); cursor.advance(), offset++) {
            if (topKOffset.contains(this.offset)) {
              final List<Object> theEvent = new ArrayList<>(allColumns.size());
              for (int j = 0; j < allColumns.size(); j++) {
                theEvent.add(getColumnValue(j));
              }
              events.add(theEvent);
            }
          }
          return events;
        }

        private List<Map<String, Object>> rowsToList()
        {
          List<Map<String, Object>> events = Lists.newArrayListWithCapacity(batchSize);
          for (; !cursor.isDone(); cursor.advance(), offset++) {
            if (topKOffset.contains(this.offset)) {
              final Map<String, Object> theEvent = new LinkedHashMap<>();
              for (int j = 0; j < allColumns.size(); j++) {
                theEvent.put(allColumns.get(j), getColumnValue(j));
              }
              events.add(theEvent);
            }
          }
          return events;
        }

        private Object getColumnValue(int i)
        {
          final BaseObjectColumnValueSelector selector = columnSelectors.get(i);
          final Object value;

          if (legacy && allColumns.get(i).equals(ScanQueryEngine.LEGACY_TIMESTAMP_KEY)) {
            Preconditions.checkNotNull(selector);
            value = DateTimes.utc((long) selector.getObject());
          } else {
            value = selector == null ? null : selector.getObject();
          }

          return value;
        }
      };
    }

    @Override
    public void cleanup(Iterator<ScanResultValue> iterFromMake)
    {
    }
  }
}
