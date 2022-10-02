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

import org.apache.druid.collections.ListBasedMulticolumnSorter;
import org.apache.druid.collections.MultiColumnSorter;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class ListBasedSorterSequence extends BaseSequence<ScanResultValue, Iterator<ScanResultValue>>
{
  public ListBasedSorterSequence(IteratorMaker<ScanResultValue, Iterator<ScanResultValue>> maker)
  {
    super(maker);
  }

  static class ListBasedSorterIteratorMaker
      implements BaseSequence.IteratorMaker<ScanResultValue, Iterator<ScanResultValue>>
  {
    private final List<String> sortColumns;
    private final boolean legacy;
    private final Cursor cursor;
    private final boolean hasTimeout;
    private final long timeoutAt;
    private final ScanQuery query;
    private final SegmentId segmentId;
    private final List<String> allColumns;
    List<String> orderByDirection;

    public ListBasedSorterIteratorMaker(
        List<String> sortColumns,
        boolean legacy,
        Cursor cursor,
        boolean hasTimeout,
        long timeoutAt,
        ScanQuery query,
        SegmentId segmentId,
        List<String> allColumns,
        List<String> orderByDirection
    )
    {
      this.sortColumns = sortColumns;
      this.legacy = legacy;
      this.cursor = cursor;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
      this.query = query;
      this.segmentId = segmentId;
      this.allColumns = allColumns;
      this.orderByDirection = orderByDirection;
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
      return new BasedSorterIterator(
          sortColumns,
          legacy,
          cursor,
          hasTimeout,
          timeoutAt,
          query,
          segmentId,
          allColumns,
          orderByDirection,
          columnSelectors
      )
      {


        @Override
        MultiColumnSorter<Map<String, Object>> rowsToListMulticolumnSorter()
        {
          return new ListBasedMulticolumnSorter<Map<String, Object>>(rowsToListComparator());
        }

        @Override
        MultiColumnSorter<List<Object>> rowsToCompactedListMulticolumnSorter()
        {
          return new ListBasedMulticolumnSorter<List<Object>>(rowsToCompactedListComparator());
        }
      };
    }


    @Override
    public void cleanup(Iterator<ScanResultValue> iterFromMake)
    {
    }
  }
}
