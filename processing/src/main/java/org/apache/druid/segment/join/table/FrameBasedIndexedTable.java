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

import com.google.common.base.Preconditions;
import com.google.common.math.IntMath;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.read.columnar.FrameColumnReader;
import org.apache.druid.frame.read.columnar.FrameColumnReaders;
import org.apache.druid.frame.segment.FrameStorageAdapter;
import org.apache.druid.frame.segment.columnar.FrameQueryableIndex;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.FrameBasedInlineDataSource;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class FrameBasedIndexedTable implements IndexedTable
{
  private static final Logger LOG = new Logger(FrameBasedIndexedTable.class);

  private final Set<String> keyColumns;
  private final RowSignature rowSignature;
  private final String version;
  private final List<IndexedTable.Index> keyColumnsIndexes;
  private final int numRows;
  private final List<QueryableIndex> frameQueryableIndexes = new ArrayList<>();
  private final List<Integer> cumulativeRowCount = new ArrayList<>();


  public FrameBasedIndexedTable(
      final FrameBasedInlineDataSource frameBasedInlineDataSource,
      final Set<String> keyColumns,
      final String version
  )
  {
    this.keyColumns = keyColumns;
    this.version = version;
    this.rowSignature = frameBasedInlineDataSource.getRowSignature();

    int rowCount = 0;
    for (FrameSignaturePair frameSignaturePair : frameBasedInlineDataSource.getFrames()) {
      Frame frame = frameSignaturePair.getFrame();
      RowSignature frameRowSignature = frameSignaturePair.getRowSignature();
      frameQueryableIndexes.add(new FrameQueryableIndex(
          frame,
          frameRowSignature,
          createColumnReaders(frameRowSignature)
      ));
      rowCount += frame.numRows();
      cumulativeRowCount.add(rowCount);
    }

    this.numRows = rowCount;

    final ArrayList<RowBasedIndexBuilder> indexBuilders = new ArrayList<>(rowSignature.size());
    final List<String> keyColumnNames = new ArrayList<>(keyColumns.size());

    for (int i = 0; i < rowSignature.size(); i++) {
      final RowBasedIndexBuilder m;
      final String columnName = rowSignature.getColumnName(i);
      if (keyColumns.contains(columnName)) {
        final ColumnType keyType =
            rowSignature.getColumnType(i).orElse(IndexedTableJoinMatcher.DEFAULT_KEY_TYPE);

        m = new RowBasedIndexBuilder(keyType);
        keyColumnNames.add(columnName);
      } else {
        m = null;
      }
      indexBuilders.add(m);
    }

    final Sequence<Cursor> cursors = Sequences.concat(
        frameBasedInlineDataSource
            .getFrames()
            .stream()
            .map(frameSignaturePair -> {
              Frame frame = frameSignaturePair.getFrame();
              RowSignature rowSignature = frameSignaturePair.getRowSignature();
              FrameStorageAdapter frameStorageAdapter =
                  new FrameStorageAdapter(frame, FrameReader.create(rowSignature), Intervals.ETERNITY);
              return frameStorageAdapter.makeCursors(
                                            null,
                                            Intervals.ETERNITY,
                                            VirtualColumns.EMPTY,
                                            Granularities.ALL,
                                            false,
                                            null
                                        );
            })
            .collect(Collectors.toList())
    );

    final Sequence<Integer> sequence = Sequences.map(
        cursors,
        cursor -> {
          if (cursor == null) {
            return 0;
          }
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
              final RowBasedIndexBuilder keyColumnIndexBuilder = indexBuilders.get(columnPosition);
              keyColumnIndexBuilder.add(selectors.get(keyColumnSelectorIndex).getObject());
            }

            if (rowNumber % 100_000 == 0) {
              if (rowNumber == 0) {
                LOG.debug("Indexed first row for frame based datasource");
              } else {
                LOG.debug("Indexed row %s for frame based datasource", rowNumber);
              }
            }
            rowNumber++;
            cursor.advance();
          }
          return rowNumber;
        }
    );

    Integer totalRows = sequence.accumulate(0, (accumulated, in) -> accumulated += in);

    this.keyColumnsIndexes = indexBuilders.stream()
                                          .map(builder -> builder != null ? builder.build() : null)
                                          .collect(Collectors.toList());

    LOG.info("Created FrameBasedIndexedTable with %s rows.", totalRows);
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
    return numRows;
  }

  @Override
  public Index columnIndex(int column)
  {
    return RowBasedIndexedTable.getKeyColumnIndex(column, keyColumnsIndexes);

  }

  @Override
  public Reader columnReader(int column)
  {

    if (!rowSignature.contains(column)) {
      throw new IAE("Column[%d] is not a valid column for the frame based datasource", column);
    }

    String columnName = rowSignature.getColumnName(column);
    final SimpleAscendingOffset offset = new SimpleAscendingOffset(numRows());
    final List<BaseObjectColumnValueSelector<?>> columnValueSelectors = new ArrayList<>();
    final Set<Closeable> closeables = new HashSet<>();

    for (QueryableIndex frameQueryableIndex : frameQueryableIndexes) {
      ColumnHolder columnHolder = frameQueryableIndex.getColumnHolder(columnName);
      if (columnHolder == null) {
        columnValueSelectors.add(NilColumnValueSelector.instance());
      } else {
        BaseColumn baseColumn = columnHolder.getColumn();
        columnValueSelectors.add(baseColumn.makeColumnValueSelector(offset));
        closeables.add(baseColumn);
      }
    }

    return new Reader()
    {
      @Nullable
      @Override
      public Object read(int row)
      {
        int frameIndex = binSearch(cumulativeRowCount, row);
        if (frameIndex == frameQueryableIndexes.size()) {
          throw new IndexOutOfBoundsException(
              StringUtils.format("Requested row index [%d], Max row count [%d]", row, numRows())
          );
        }
        // The offset needs to be changed as well
        int adjustedOffset = frameIndex == 0
                             ? row
                             : IntMath.checkedSubtract(row, cumulativeRowCount.get(frameIndex - 1));
        offset.setCurrentOffset(adjustedOffset);
        return columnValueSelectors.get(frameIndex).getObject();
      }

      @Override
      public void close() throws IOException
      {
        for (Closeable c : closeables) {
          c.close();
        }
      }
    };
  }

  @Override
  public boolean isCacheable()
  {
    return false;
  }

  @Override
  public void close()
  {

  }

  @Override
  public Optional<Closeable> acquireReferences()
  {
    return Optional.of(
        () -> {
        }
    );
  }

  private List<FrameColumnReader> createColumnReaders(RowSignature rowSignature)
  {
    final List<FrameColumnReader> columnReaders = new ArrayList<>(rowSignature.size());

    for (int columnNumber = 0; columnNumber < rowSignature.size(); columnNumber++) {
      ColumnType columnType = Preconditions.checkNotNull(
          rowSignature.getColumnType(columnNumber).orElse(null),
          "Type for column [%s]",
          rowSignature.getColumnName(columnNumber)
      );
      columnReaders.add(FrameColumnReaders.create(rowSignature.getColumnName(columnNumber), columnNumber, columnType));
    }

    return columnReaders;
  }

  /**
   * This method finds out the frame which contains the row indexed "row" from the cumulative array
   * This is basically a binary search where we have to find the FIRST element which is STRICTLY GREATER than
   * the "row" provided
   * <p>
   * Note: row is the index (therefore it is 0-indexed)
   */
  private int binSearch(List<Integer> arr, int row)
  {
    int start = 0;
    int end = arr.size();

    while (start < end) {

      // This will be rounded down in case (start + end) is odd. Therefore middle will always be less than
      // end and will be equal to or greater than start
      int middle = start + (end - start) / 2;

      // If the "middle" satisfies the below predicate, then we can move the end backward because every element after
      // middle would be satisfying the predicate
      if (arr.get(middle) > row) {
        end = middle;
      } else {
        start = middle + 1;
      }
    }

    return start; // Note: at this point, end == start
  }
}
