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
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntity.CleanableFile;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

public class DruidSegmentReader extends IntermediateRowParsingReader<Map<String, Object>>
{
  private final DruidSegmentInputEntity source;
  private final IndexIO indexIO;
  private final List<String> dimensions;
  private final List<String> metrics;
  private final DimFilter dimFilter;
  private final File temporaryDirectory;

  DruidSegmentReader(
      InputEntity source,
      IndexIO indexIO,
      List<String> dimensions,
      List<String> metrics,
      DimFilter dimFilter,
      File temporaryDirectory
  )
  {
    Preconditions.checkArgument(source instanceof DruidSegmentInputEntity);
    this.source = (DruidSegmentInputEntity) source;
    this.indexIO = indexIO;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.dimFilter = dimFilter;
    this.temporaryDirectory = temporaryDirectory;
  }

  @Override
  protected CloseableIterator<Map<String, Object>> intermediateRowIterator() throws IOException
  {
    final CleanableFile segmentFile = source.fetch(temporaryDirectory, null);
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

    final Sequence<Map<String, Object>> sequence = Sequences.concat(
        Sequences.map(
            cursors,
            this::cursorToSequence
        )
    );

    return makeCloseableIteratorFromSequenceAndSegmentFile(sequence, segmentFile);
  }

  @Override
  protected List<InputRow> parseInputRows(Map<String, Object> intermediateRow) throws ParseException
  {
    final DateTime timestamp = (DateTime) intermediateRow.get(ColumnHolder.TIME_COLUMN_NAME);
    return Collections.singletonList(new MapBasedInputRow(timestamp.getMillis(), dimensions, intermediateRow));
  }

  @Override
  protected Map<String, Object> toMap(Map<String, Object> intermediateRow)
  {
    return intermediateRow;
  }

  /**
   * Given a {@link Cursor} create a {@link Sequence} that returns the rows of the cursor as
   * Map<String, Object> intermediate rows, selecting the dimensions and metrics of this segment reader.
   *
   * @param cursor A cursor
   * @return A sequence of intermediate rows
   */
  private Sequence<Map<String, Object>> cursorToSequence(
      final Cursor cursor
  )
  {
    return Sequences.simple(
        () -> new IntermediateRowFromCursorIterator(cursor, dimensions, metrics)
    );
  }

  /**
   * @param sequence    A sequence of intermediate rows generated from a sequence of
   *                    cursors in {@link #intermediateRowIterator()}
   * @param segmentFile The underlying segment file containing the row data
   * @return A CloseableIterator from a sequence of intermediate rows, closing the underlying segment file
   *         when the iterator is closed.
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
        Closer closer = Closer.create();
        closer.register(rowYielder);
        closer.register(segmentFile);
        closer.close();
      }
    };
  }

  /**
   * Given a {@link Cursor}, a list of dimension names, and a list of metric names, this iterator
   * returns the rows of the cursor as Map<String, Object> intermediate rows.
   */
  private static class IntermediateRowFromCursorIterator implements Iterator<Map<String, Object>>
  {
    private final Cursor cursor;
    private final BaseLongColumnValueSelector timestampColumnSelector;
    private final Map<String, DimensionSelector> dimSelectors;
    private final Map<String, BaseObjectColumnValueSelector> metSelectors;

    public IntermediateRowFromCursorIterator(
        Cursor cursor,
        List<String> dimensionNames,
        List<String> metricNames
    )
    {
      this.cursor = cursor;

      timestampColumnSelector = cursor
          .getColumnSelectorFactory()
          .makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

      dimSelectors = new HashMap<>();
      for (String dim : dimensionNames) {
        final DimensionSelector dimSelector = cursor
            .getColumnSelectorFactory()
            .makeDimensionSelector(new DefaultDimensionSpec(dim, dim));
        // dimSelector is null if the dimension is not present
        if (dimSelector != null) {
          dimSelectors.put(dim, dimSelector);
        }
      }

      metSelectors = new HashMap<>();
      for (String metric : metricNames) {
        final BaseObjectColumnValueSelector metricSelector = cursor
            .getColumnSelectorFactory()
            .makeColumnValueSelector(metric);
        metSelectors.put(metric, metricSelector);
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
      final Map<String, Object> theEvent =
          CollectionUtils.newLinkedHashMapWithExpectedSize(dimSelectors.size() + metSelectors.size() + 1);

      for (Entry<String, DimensionSelector> dimSelector : dimSelectors.entrySet()) {
        final String dim = dimSelector.getKey();
        final DimensionSelector selector = dimSelector.getValue();
        final IndexedInts vals = selector.getRow();

        int valsSize = vals.size();
        if (valsSize == 1) {
          final String dimVal = selector.lookupName(vals.get(0));
          theEvent.put(dim, dimVal);
        } else if (valsSize > 1) {
          List<String> dimVals = new ArrayList<>(valsSize);
          for (int i = 0; i < valsSize; ++i) {
            dimVals.add(selector.lookupName(vals.get(i)));
          }
          theEvent.put(dim, dimVals);
        }
      }

      for (Entry<String, BaseObjectColumnValueSelector> metSelector : metSelectors.entrySet()) {
        final String metric = metSelector.getKey();
        final BaseObjectColumnValueSelector selector = metSelector.getValue();
        Object value = selector.getObject();
        if (value != null) {
          theEvent.put(metric, value);
        }
      }

      // Timestamp is added last because we expect that the time column will always be a date time object.
      // If it is added earlier, it can be overwritten by metrics or dimenstions with the same name.
      //
      // If a user names a metric or dimension `__time` it will be overwritten. This case should be rare since
      // __time is reserved for the time column in druid segments.
      final long timestamp = timestampColumnSelector.getLong();
      theEvent.put(ColumnHolder.TIME_COLUMN_NAME, DateTimes.utc(timestamp));

      cursor.advance();
      return theEvent;
    }
  }
}
