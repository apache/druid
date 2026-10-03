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

package org.apache.druid.data.input;

import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Adapts batches to a reusable row cursor. A returned row is valid only until this iterator advances.
 */
@UnstableApi
public class BatchToInputRowIterator implements CloseableIterator<InputRow>
{
  private final CloseableIterator<RowsAndColumns> batches;
  private final InputRowSchema inputRowSchema;
  private final BatchBackedInputRow rowCursor;

  @Nullable
  private BatchContext currentBatch;
  private int nextRowNumber;

  public BatchToInputRowIterator(
      final CloseableIterator<RowsAndColumns> batches,
      final InputRowSchema inputRowSchema
  )
  {
    this.batches = batches;
    this.inputRowSchema = inputRowSchema;
    this.rowCursor = new BatchBackedInputRow(inputRowSchema.getTimestampSpec());
  }

  @Override
  public boolean hasNext()
  {
    while (currentBatch == null || nextRowNumber >= currentBatch.numRows()) {
      if (!batches.hasNext()) {
        return false;
      }
      currentBatch = new BatchContext(batches.next(), inputRowSchema);
      nextRowNumber = 0;
    }
    return true;
  }

  @Override
  public InputRow next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    rowCursor.moveTo(currentBatch, nextRowNumber++);
    return rowCursor;
  }

  @Override
  public void close() throws IOException
  {
    batches.close();
  }

  private static class BatchContext
  {
    private final RowsAndColumns rowsAndColumns;
    private final Map<String, ColumnAccessor> accessors;
    private final List<String> dimensions;

    BatchContext(final RowsAndColumns rowsAndColumns, final InputRowSchema inputRowSchema)
    {
      this.rowsAndColumns = rowsAndColumns;
      this.accessors = new LinkedHashMap<>();
      for (final String columnName : rowsAndColumns.getColumnNames()) {
        final Column column = rowsAndColumns.findColumn(columnName);
        if (column == null) {
          throw new ISE("Column [%s] is listed but cannot be found", columnName);
        }
        accessors.put(columnName, column.toAccessor());
      }
      this.dimensions = MapInputRowParser.findDimensions(
          inputRowSchema.getTimestampSpec(),
          inputRowSchema.getDimensionsSpec(),
          new HashSet<>(accessors.keySet())
      );
    }

    int numRows()
    {
      return rowsAndColumns.numRows();
    }

    @Nullable
    Object getRaw(final String columnName, final int rowNumber)
    {
      final ColumnAccessor accessor = accessors.get(columnName);
      return accessor == null ? null : accessor.getObject(rowNumber);
    }

    Map<String, Object> asMap(final int rowNumber)
    {
      final Map<String, Object> row = new LinkedHashMap<>();
      accessors.forEach((columnName, accessor) -> row.put(columnName, accessor.getObject(rowNumber)));
      return row;
    }
  }

  private static class BatchBackedInputRow implements InputRow
  {
    private final TimestampSpec timestampSpec;

    private BatchContext batch;
    private int rowNumber;
    private DateTime timestamp;

    BatchBackedInputRow(final TimestampSpec timestampSpec)
    {
      this.timestampSpec = timestampSpec;
    }

    void moveTo(final BatchContext batch, final int rowNumber)
    {
      this.batch = batch;
      this.rowNumber = rowNumber;
      final Object rawTimestamp = batch.getRaw(timestampSpec.getTimestampColumn(), rowNumber);
      this.timestamp = MapInputRowParser.parseTimestampOrThrowParseException(
          rawTimestamp,
          timestampSpec,
          () -> batch.asMap(rowNumber)
      );
    }

    @Override
    public List<String> getDimensions()
    {
      return batch.dimensions;
    }

    @Override
    public long getTimestampFromEpoch()
    {
      return timestamp.getMillis();
    }

    @Override
    public DateTime getTimestamp()
    {
      return timestamp;
    }

    @Override
    public List<String> getDimension(final String dimension)
    {
      return Rows.objectToStrings(getRaw(dimension));
    }

    @Nullable
    @Override
    public Object getRaw(final String columnName)
    {
      return batch.getRaw(columnName, rowNumber);
    }

    @Nullable
    @Override
    public Number getMetric(final String metric)
    {
      return Rows.objectToNumber(metric, getRaw(metric), true);
    }

    @Override
    public int compareTo(final Row other)
    {
      return timestamp.compareTo(other.getTimestamp());
    }

    @Override
    public String toString()
    {
      return batch.asMap(rowNumber).toString();
    }
  }
}
