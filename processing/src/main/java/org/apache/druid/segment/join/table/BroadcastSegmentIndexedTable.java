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

package org.apache.druid.segment.join.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class BroadcastSegmentIndexedTable implements IndexedTable
{
  private static final Logger LOG = new Logger(BroadcastSegmentIndexedTable.class);

  private final QueryableIndexSegment segment;
  private final QueryableIndexStorageAdapter adapter;
  private final QueryableIndex queryableIndex;
  private final Set<String> keyColumns;
  private final RowSignature rowSignature;
  private final String version;
  private final List<Map<Object, IntList>> keyColumnsIndex;

  public BroadcastSegmentIndexedTable(final QueryableIndexSegment theSegment, final Set<String> keyColumns, final String version)
  {
    this.keyColumns = keyColumns;
    this.version = version;
    this.segment = theSegment;
    this.adapter = (QueryableIndexStorageAdapter) segment.asStorageAdapter();
    this.queryableIndex = segment.asQueryableIndex();

    RowSignature.Builder sigBuilder = RowSignature.builder();
    sigBuilder.add(ColumnHolder.TIME_COLUMN_NAME, ValueType.LONG);
    for (String column : queryableIndex.getColumnNames()) {
      sigBuilder.add(column, adapter.getColumnCapabilities(column).getType());
    }
    this.rowSignature = sigBuilder.build();

    // initialize keycolumn index maps
    this.keyColumnsIndex = new ArrayList<>(rowSignature.size());
    final List<String> keyColumnNames = new ArrayList<>(keyColumns.size());
    for (int i = 0; i < rowSignature.size(); i++) {
      final Map<Object, IntList> m;
      final String columnName = rowSignature.getColumnName(i);
      if (keyColumns.contains(columnName)) {
        m = new HashMap<>();
        keyColumnNames.add(columnName);
      } else {
        m = null;
      }
      keyColumnsIndex.add(m);
    }

    // sort of like the dump segment tool, but build key column indexes when reading the segment
    final Sequence<Cursor> cursors = adapter.makeCursors(
        Filters.toFilter(null),
        queryableIndex.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    final Sequence<Integer> sequence = Sequences.map(
        cursors,
        cursor -> {
          int rowNumber = 0;
          ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

          // this should really be optimized to use dimension selectors where possible to populate indexes from bitmap
          // indexes, but, an optimization for another day
          final List<BaseObjectColumnValueSelector> selectors = keyColumnNames
              .stream()
              .map(columnSelectorFactory::makeColumnValueSelector)
              .collect(Collectors.toList());

          while (!cursor.isDone()) {
            for (int keyColumnSelectorIndex = 0; keyColumnSelectorIndex < selectors.size(); keyColumnSelectorIndex++) {
              final String keyColumnName = keyColumnNames.get(keyColumnSelectorIndex);
              final int columnPosition = rowSignature.indexOf(keyColumnName);
              final Map<Object, IntList> keyColumnValueIndex = keyColumnsIndex.get(columnPosition);

              final Object value = selectors.get(keyColumnSelectorIndex).getObject();
              final ValueType keyType = rowSignature.getColumnType(keyColumnName)
                                                    .orElse(IndexedTableJoinMatcher.DEFAULT_KEY_TYPE);
              // is this actually necessary or is value already cool? (RowBasedIndexedTable cargo cult represent)
              final Object key = DimensionHandlerUtils.convertObjectToType(value, keyType);

              if (key != null) {
                final IntList array = keyColumnValueIndex.computeIfAbsent(key, k -> new IntArrayList());
                array.add(rowNumber);
              }
            }

            if (rowNumber % 100_000 == 0) {
              if (rowNumber == 0) {
                LOG.info("Indexed first row for table %s", theSegment.getId());
              } else {
                LOG.info("Indexed row %s for table %s", rowNumber, theSegment.getId());
              }
            }
            rowNumber++;
            cursor.advance();
          }
          return rowNumber;
        }
    );

    Integer totalRows = sequence.accumulate(0, (accumulated, in) -> accumulated += in);
    LOG.info("Created BroadcastSegmentIndexedTable with %s rows.", totalRows);
  }

  @Override
  public String version()
  {
    return version;
  }

  @Override
  public Set<String> keyColumns()
  {
    return keyColumns;
  }

  @Override
  public RowSignature rowSignature()
  {
    return rowSignature;
  }

  @Override
  public int numRows()
  {
    return adapter.getNumRows();
  }

  @Override
  public Index columnIndex(int column)
  {
    return RowBasedIndexedTable.getKeyColumnIndex(column, keyColumnsIndex, rowSignature);
  }

  @Override
  public Reader columnReader(int column)
  {
    if (column < 0 || rowSignature.getColumnName(0) == null) {
      throw new IAE("Column[%d] is not a valid column", column);
    }
    final SimpleAscendingOffset offset = new SimpleAscendingOffset(adapter.getNumRows());
    final BaseObjectColumnValueSelector selector = queryableIndex.getColumnHolder(rowSignature.getColumnName(column))
                                                                 .getColumn()
                                                                 .makeColumnValueSelector(offset);
    return new Reader()
    {
      @Nullable
      @Override
      public Object read(int row)
      {
        offset.setCurrentOffset(row);
        return selector.getObject();
      }
    };
  }

  @Override
  public void close()
  {
    // the segment will close itself when it is dropped, no need to do it here
  }

  @Override
  public Optional<Closeable> acquireReferences()
  {
    return Optional.empty();
  }
}
