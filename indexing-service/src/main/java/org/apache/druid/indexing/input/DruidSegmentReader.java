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

package org.apache.druid.indexing.input;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntity.CleanableFile;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnProcessorFactory;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.utils.CloseableUtils;
import org.apache.druid.utils.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

public class DruidSegmentReader extends IntermediateRowParsingReader<Map<String, Object>>
{
  private DruidSegmentInputEntity source;
  private final IndexIO indexIO;
  private final ColumnsFilter columnsFilter;
  private final InputRowSchema inputRowSchema;
  private final DimFilter dimFilter;
  private final File temporaryDirectory;

  DruidSegmentReader(
      final InputEntity source,
      final IndexIO indexIO,
      final TimestampSpec timestampSpec,
      final DimensionsSpec dimensionsSpec,
      final ColumnsFilter columnsFilter,
      final DimFilter dimFilter,
      final File temporaryDirectory
  )
  {
    this.source = (DruidSegmentInputEntity) source;
    this.indexIO = indexIO;
    this.columnsFilter = columnsFilter;
    this.inputRowSchema = new InputRowSchema(
        timestampSpec,
        dimensionsSpec,
        columnsFilter
    );
    this.dimFilter = dimFilter;
    this.temporaryDirectory = temporaryDirectory;
  }

  @Override
  protected CloseableIterator<Map<String, Object>> intermediateRowIterator() throws IOException
  {
    final CleanableFile segmentFile = source().fetch(temporaryDirectory, null);
    final WindowedStorageAdapter storageAdapter = new WindowedStorageAdapter(
        new QueryableIndexStorageAdapter(
            indexIO.loadIndex(segmentFile.file())
        ),
        source.getIntervalFilter()
    );

    final Sequence<Cursor> cursors = storageAdapter.getAdapter().makeCursors(
        Filters.toFilter(dimFilter),
        storageAdapter.getInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    // Retain order of columns from the original segments. Useful for preserving dimension order if we're in
    // schemaless mode.
    final Set<String> columnsToRead = Sets.newLinkedHashSet(
        Iterables.filter(
            storageAdapter.getAdapter().getRowSignature().getColumnNames(),
            columnsFilter::apply
        )
    );

    final Sequence<Map<String, Object>> sequence = Sequences.concat(
        Sequences.map(
            cursors,
            cursor -> cursorToSequence(cursor, columnsToRead)
        )
    );

    return makeCloseableIteratorFromSequenceAndSegmentFile(sequence, segmentFile);
  }

  @Override
  protected InputEntity source()
  {
    return source;
  }

  @Override
  protected List<InputRow> parseInputRows(Map<String, Object> intermediateRow) throws ParseException
  {
    return Collections.singletonList(MapInputRowParser.parse(inputRowSchema, intermediateRow));
  }

  @Override
  protected List<Map<String, Object>> toMap(Map<String, Object> intermediateRow)
  {
    return Collections.singletonList(intermediateRow);
  }

  /**
   * Given a {@link Cursor} create a {@link Sequence} that returns the rows of the cursor as
   * Map<String, Object> intermediate rows, selecting the dimensions and metrics of this segment reader.
   *
   * @param cursor A cursor
   *
   * @return A sequence of intermediate rows
   */
  private Sequence<Map<String, Object>> cursorToSequence(final Cursor cursor, final Set<String> columnsToRead)
  {
    return Sequences.simple(
        () -> new IntermediateRowFromCursorIterator(cursor, columnsToRead)
    );
  }

  /**
   * @param sequence    A sequence of intermediate rows generated from a sequence of
   *                    cursors in {@link #intermediateRowIterator()}
   * @param segmentFile The underlying segment file containing the row data
   *
   * @return A CloseableIterator from a sequence of intermediate rows, closing the underlying segment file
   * when the iterator is closed.
   */
  @VisibleForTesting
  static CloseableIterator<Map<String, Object>> makeCloseableIteratorFromSequenceAndSegmentFile(
      final Sequence<Map<String, Object>> sequence,
      final CleanableFile segmentFile
  )
  {
    return new CloseableIterator<Map<String, Object>>()
    {
      Yielder<Map<String, Object>> rowYielder = Yielders.each(sequence);

      @Override
      public boolean hasNext()
      {
        return !rowYielder.isDone();
      }

      @Override
      public Map<String, Object> next()
      {
        final Map<String, Object> row = rowYielder.get();
        rowYielder = rowYielder.next(null);
        return row;
      }

      @Override
      public void close() throws IOException
      {
        CloseableUtils.closeAll(rowYielder, segmentFile);
      }
    };
  }

  /**
   * Reads columns for {@link IntermediateRowFromCursorIterator}.
   */
  private static class IntermediateRowColumnProcessorFactory implements ColumnProcessorFactory<Supplier<Object>>
  {
    private static final IntermediateRowColumnProcessorFactory INSTANCE = new IntermediateRowColumnProcessorFactory();

    @Override
    public ColumnType defaultType()
    {
      return ColumnType.STRING;
    }

    @Override
    public Supplier<Object> makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
    {
      return () -> {
        final IndexedInts vals = selector.getRow();

        int valsSize = vals.size();
        if (valsSize == 1) {
          return selector.lookupName(vals.get(0));
        } else if (valsSize > 1) {
          List<String> dimVals = new ArrayList<>(valsSize);
          for (int i = 0; i < valsSize; ++i) {
            dimVals.add(selector.lookupName(vals.get(i)));
          }

          return dimVals;
        }

        return null;
      };
    }

    @Override
    public Supplier<Object> makeFloatProcessor(BaseFloatColumnValueSelector selector)
    {
      return () -> selector.isNull() ? null : selector.getFloat();
    }

    @Override
    public Supplier<Object> makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
    {
      return () -> selector.isNull() ? null : selector.getDouble();
    }

    @Override
    public Supplier<Object> makeLongProcessor(BaseLongColumnValueSelector selector)
    {
      return () -> selector.isNull() ? null : selector.getLong();
    }

    @Override
    public Supplier<Object> makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
    {
      return selector::getObject;
    }
  }

  /**
   * Given a {@link Cursor}, a list of dimension names, and a list of metric names, this iterator
   * returns the rows of the cursor as Map<String, Object> intermediate rows.
   */
  private static class IntermediateRowFromCursorIterator implements Iterator<Map<String, Object>>
  {
    private final Cursor cursor;
    private final Map<String, Supplier<Object>> columnReaders;

    public IntermediateRowFromCursorIterator(
        final Cursor cursor,
        final Set<String> columnsToRead
    )
    {
      this.cursor = cursor;
      this.columnReaders = CollectionUtils.newLinkedHashMapWithExpectedSize(columnsToRead.size());

      for (String column : columnsToRead) {
        columnReaders.put(
            column,
            ColumnProcessors.makeProcessor(
                column,
                IntermediateRowColumnProcessorFactory.INSTANCE,
                cursor.getColumnSelectorFactory()
            )
        );
      }
    }

    @Override
    public boolean hasNext()
    {
      return !cursor.isDone();
    }

    @Override
    public Map<String, Object> next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final Map<String, Object> rowMap =
          CollectionUtils.newLinkedHashMapWithExpectedSize(columnReaders.size());

      for (Entry<String, Supplier<Object>> entry : columnReaders.entrySet()) {
        final Object value = entry.getValue().get();
        if (value != null) {
          rowMap.put(entry.getKey(), value);
        }
      }

      cursor.advance();
      return rowMap;
    }
  }
}
