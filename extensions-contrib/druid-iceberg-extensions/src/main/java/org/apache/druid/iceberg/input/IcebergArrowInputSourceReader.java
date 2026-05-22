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

package org.apache.druid.iceberg.input;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.iceberg.filter.IcebergFilter;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.arrow.vectorized.ArrowReader;
import org.apache.iceberg.arrow.vectorized.ColumnarBatch;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.TableScanUtil;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

/**
 * Reads an Iceberg table via iceberg-arrow's {@link ArrowReader}, yielding {@link InputRow} objects.
 *
 * Delete application (V2 equality and positional deletes), type coercion, and schema evolution are
 * handled entirely by the Iceberg library. Druid only consumes the resulting {@link ColumnarBatch}
 * batches and maps them to {@link MapBasedInputRow}.
 *
 * Column projection and predicate push-down are applied at scan planning time so only requested
 * columns and matching files are read from storage.
 *
 * Note: iceberg-arrow currently supports Parquet data files only. ORC and Avro files will throw
 * {@link UnsupportedOperationException} at read time; use the standard delegate path for those.
 */
public class IcebergArrowInputSourceReader implements InputSourceReader
{
  static final int DEFAULT_BATCH_SIZE = 1024;

  private final Table table;
  @Nullable
  private final IcebergFilter icebergFilter;
  @Nullable
  private final DateTime snapshotTime;
  private final boolean caseSensitive;
  private final InputRowSchema schema;
  private final int batchSize;

  public IcebergArrowInputSourceReader(
      final Table table,
      @Nullable final IcebergFilter icebergFilter,
      @Nullable final DateTime snapshotTime,
      final boolean caseSensitive,
      final InputRowSchema schema,
      final int batchSize
  )
  {
    this.table = table;
    this.icebergFilter = icebergFilter;
    this.snapshotTime = snapshotTime;
    this.caseSensitive = caseSensitive;
    this.schema = schema;
    this.batchSize = batchSize;
  }

  @Override
  public CloseableIterator<InputRow> read(final InputStats inputStats) throws IOException
  {
    final TableScan scan = buildScan();
    final CloseableIterable<CombinedScanTask> tasks = TableScanUtil.planTasks(
        scan.planFiles(),
        scan.targetSplitSize(),
        scan.splitLookback(),
        scan.splitOpenFileCost()
    );
    final ArrowReader arrowReader = new ArrowReader(scan, batchSize, true);
    final org.apache.iceberg.io.CloseableIterator<ColumnarBatch> batchIter = arrowReader.open(tasks);
    return new ArrowInputRowIterator(batchIter, arrowReader, tasks, inputStats);
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    final CloseableIterator<InputRow> rows = read(new NoopInputStats());
    return new CloseableIterator<InputRowListPlusRawValues>()
    {
      @Override
      public boolean hasNext()
      {
        return rows.hasNext();
      }

      @Override
      public InputRowListPlusRawValues next()
      {
        final InputRow row = rows.next();
        return InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent());
      }

      @Override
      public void close() throws IOException
      {
        rows.close();
      }
    };
  }

  private TableScan buildScan()
  {
    TableScan scan = table.newScan().caseSensitive(caseSensitive);

    // Push column projection into the scan planner — only requested columns are read from disk.
    final List<String> configuredDims = schema.getDimensionsSpec().getDimensionNames();
    final String timestampColumn = schema.getTimestampSpec().getTimestampColumn();
    if (!configuredDims.isEmpty()) {
      final List<String> selectCols = new ArrayList<>(configuredDims);
      if (timestampColumn != null && !selectCols.contains(timestampColumn)) {
        selectCols.add(timestampColumn);
      }
      scan = scan.select(selectCols);
    }

    // Push predicate into scan planner — reduces files opened and rows read.
    if (icebergFilter != null) {
      scan = icebergFilter.filter(scan);
    }

    // Snapshot pinning for time-travel reads.
    if (snapshotTime != null) {
      scan = scan.asOfTime(snapshotTime.getMillis());
    }

    return scan;
  }

  private InputRow batchRowToInputRow(final ColumnarBatch batch, final int rowIdx)
  {
    final int numCols = batch.numCols();
    final Map<String, Object> event = new HashMap<>(numCols);
    for (int col = 0; col < numCols; col++) {
      final FieldVector vec = batch.column(col).getFieldVector();
      if (!vec.isNull(rowIdx)) {
        event.put(vec.getName(), extractValue(vec, rowIdx));
      }
    }
    final long timestamp = schema.getTimestampSpec().extractTimestamp(event).getMillis();
    final List<String> dimensions = resolveDimensions(batch);
    return new MapBasedInputRow(timestamp, dimensions, event);
  }

  private List<String> resolveDimensions(final ColumnarBatch batch)
  {
    final List<String> configured = schema.getDimensionsSpec().getDimensionNames();
    if (!configured.isEmpty()) {
      return configured;
    }
    // Derive dimensions from the batch schema, excluding the timestamp column.
    final String tsCol = schema.getTimestampSpec().getTimestampColumn();
    final List<String> dims = new ArrayList<>(batch.numCols());
    for (int col = 0; col < batch.numCols(); col++) {
      final String name = batch.column(col).getFieldVector().getName();
      if (!name.equals(tsCol)) {
        dims.add(name);
      }
    }
    return dims;
  }

  /**
   * Type-safe extraction from Arrow vectors, avoiding getObject() boxing on the hot path.
   * Covers all scalar types supported by iceberg-arrow 1.10.0.
   * Falls back to getObject() for any type added in future Arrow/Iceberg versions.
   */
  static Object extractValue(final FieldVector vec, final int idx)
  {
    if (vec instanceof BigIntVector) {
      return ((BigIntVector) vec).get(idx);
    }
    if (vec instanceof IntVector) {
      return ((IntVector) vec).get(idx);
    }
    if (vec instanceof SmallIntVector) {
      return (int) ((SmallIntVector) vec).get(idx);
    }
    if (vec instanceof TinyIntVector) {
      return (int) ((TinyIntVector) vec).get(idx);
    }
    if (vec instanceof Float8Vector) {
      return ((Float8Vector) vec).get(idx);
    }
    if (vec instanceof Float4Vector) {
      return (double) ((Float4Vector) vec).get(idx);
    }
    if (vec instanceof BitVector) {
      return ((BitVector) vec).get(idx) == 1;
    }
    if (vec instanceof VarCharVector) {
      return new String(((VarCharVector) vec).get(idx), StandardCharsets.UTF_8);
    }
    if (vec instanceof VarBinaryVector) {
      return ((VarBinaryVector) vec).get(idx);
    }
    if (vec instanceof DecimalVector) {
      return ((DecimalVector) vec).getObject(idx);
    }
    // Timestamps: Iceberg stores timestamps as micros; convert to millis for Druid.
    if (vec instanceof TimeStampMicroTZVector) {
      return TimeUnit.MICROSECONDS.toMillis(((TimeStampMicroTZVector) vec).get(idx));
    }
    if (vec instanceof TimeStampMicroVector) {
      return TimeUnit.MICROSECONDS.toMillis(((TimeStampMicroVector) vec).get(idx));
    }
    if (vec instanceof TimeStampNanoTZVector) {
      return TimeUnit.NANOSECONDS.toMillis(((TimeStampNanoTZVector) vec).get(idx));
    }
    if (vec instanceof TimeStampNanoVector) {
      return TimeUnit.NANOSECONDS.toMillis(((TimeStampNanoVector) vec).get(idx));
    }
    if (vec instanceof TimeStampMilliTZVector) {
      return ((TimeStampMilliTZVector) vec).get(idx);
    }
    if (vec instanceof TimeStampMilliVector) {
      return ((TimeStampMilliVector) vec).get(idx);
    }
    if (vec instanceof DateDayVector) {
      // Days since epoch → millis since epoch
      return TimeUnit.DAYS.toMillis(((DateDayVector) vec).get(idx));
    }
    if (vec instanceof TimeMicroVector) {
      return TimeUnit.MICROSECONDS.toMillis(((TimeMicroVector) vec).get(idx));
    }
    if (vec instanceof FixedSizeBinaryVector) {
      return ((FixedSizeBinaryVector) vec).get(idx);
    }
    // Safe fallback for any Arrow type not explicitly handled (dict-encoded, future types).
    return vec.getObject(idx);
  }

  private static final class NoopInputStats implements InputStats
  {
    @Override
    public void incrementProcessedBytes(final long incrementByValue)
    {
    }

    @Override
    public long getProcessedBytes()
    {
      return 0;
    }
  }

  private class ArrowInputRowIterator implements CloseableIterator<InputRow>
  {
    private final org.apache.iceberg.io.CloseableIterator<ColumnarBatch> batchIter;
    private final ArrowReader arrowReader;
    private final CloseableIterable<CombinedScanTask> tasks;
    private final InputStats inputStats;

    private ColumnarBatch currentBatch = null;
    private int rowIndexInBatch = 0;
    private boolean exhausted = false;

    ArrowInputRowIterator(
        final org.apache.iceberg.io.CloseableIterator<ColumnarBatch> batchIter,
        final ArrowReader arrowReader,
        final CloseableIterable<CombinedScanTask> tasks,
        final InputStats inputStats
    )
    {
      this.batchIter = batchIter;
      this.arrowReader = arrowReader;
      this.tasks = tasks;
      this.inputStats = inputStats;
    }

    @Override
    public boolean hasNext()
    {
      if (exhausted) {
        return false;
      }
      if (currentBatch != null && rowIndexInBatch < currentBatch.numRows()) {
        return true;
      }
      return loadNextBatch();
    }

    @Override
    public InputRow next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return batchRowToInputRow(currentBatch, rowIndexInBatch++);
    }

    private boolean loadNextBatch()
    {
      while (batchIter.hasNext()) {
        currentBatch = batchIter.next();
        rowIndexInBatch = 0;
        if (currentBatch.numRows() > 0) {
          inputStats.incrementProcessedBytes(estimateBatchBytes(currentBatch));
          return true;
        }
      }
      exhausted = true;
      return false;
    }

    private long estimateBatchBytes(final ColumnarBatch batch)
    {
      long bytes = 0;
      for (int col = 0; col < batch.numCols(); col++) {
        bytes += batch.column(col).getFieldVector().getBufferSize();
      }
      return bytes;
    }

    @Override
    public void close() throws IOException
    {
      try {
        batchIter.close();
      }
      finally {
        try {
          arrowReader.close();
        }
        finally {
          tasks.close();
        }
      }
    }
  }
}
