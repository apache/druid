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

import com.google.common.collect.Maps;
import org.apache.arrow.vector.FieldVector;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.error.DruidException;
import org.apache.druid.iceberg.filter.IcebergFilter;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.arrow.vectorized.ArrowReader;
import org.apache.iceberg.arrow.vectorized.ColumnVector;
import org.apache.iceberg.arrow.vectorized.ColumnarBatch;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.TableScanUtil;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Reads an Iceberg table via iceberg-arrow's {@link ArrowReader}, yielding {@link InputRow} objects.
 *
 * Type coercion and compatible schema evolution are handled by the Iceberg library. Druid only consumes
 * the resulting {@link ColumnarBatch} batches and maps them to {@link MapBasedInputRow}.
 *
 * Column projection and predicate push-down are applied at scan planning time so only requested
 * columns and matching files are read from storage.
 *
 * Note: iceberg-arrow currently supports Parquet data files only. ORC and Avro files will throw
 * {@link UnsupportedOperationException} at read time. Delete-file snapshots are rejected because
 * iceberg-arrow does not apply equality or positional deletes.
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
  public CloseableIterator<InputRow> read(@Nullable final InputStats inputStats) throws IOException
  {
    final TableScan scan = buildScan();
    validateNoDeleteFiles(scan);
    final CloseableIterable<CombinedScanTask> tasks = TableScanUtil.planTasks(
        scan.planFiles(),
        scan.targetSplitSize(),
        scan.splitLookback(),
        scan.splitOpenFileCost()
    );
    final ClassLoader extensionClassLoader = IcebergArrowInputSourceReader.class.getClassLoader();
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(extensionClassLoader);
      final ArrowReader arrowReader = new ArrowReader(scan, batchSize, true);
      final org.apache.iceberg.io.CloseableIterator<ColumnarBatch> batchIter = arrowReader.open(tasks);
      return new ArrowInputRowIterator(
          batchIter,
          arrowReader,
          tasks,
          inputStats != null ? inputStats : new NoopInputStats(),
          scan.schema(),
          extensionClassLoader
      );
    }
    finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  private void validateNoDeleteFiles(final TableScan scan) throws IOException
  {
    try (CloseableIterable<FileScanTask> fileTasks = scan.planFiles()) {
      for (FileScanTask fileTask : fileTasks) {
        if (!fileTask.deletes().isEmpty()) {
          throw DruidException.forPersona(DruidException.Persona.USER)
                              .ofCategory(DruidException.Category.UNSUPPORTED)
                              .build(
                                  "Arrow reader does not support Iceberg snapshots with delete files. "
                                  + "Use a delete-aware input path."
                              );
        }
      }
    }
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

    if (snapshotTime != null) {
      scan = scan.asOfTime(snapshotTime.getMillis());
    }

    final List<String> projection = projectedColumns(scan.schema());
    if (projection != null) {
      scan = scan.select(projection);
    }
    if (icebergFilter != null) {
      scan = icebergFilter.filter(scan);
    }
    return scan;
  }

  /** Projection authority is ColumnsFilter, not DimensionsSpec. Mirrors DeltaInputSource#pruneSchema. */
  @Nullable
  private List<String> projectedColumns(final Schema scanSchema)
  {
    final ColumnsFilter filter = schema.getColumnsFilter();
    final List<String> allColumns = scanSchema.columns().stream()
                                         .map(Types.NestedField::name)
                                         .collect(Collectors.toList());
    final List<String> filtered = allColumns.stream()
                                            .filter(filter::apply)
                                            .collect(Collectors.toList());
    if (filtered.equals(allColumns)) {
      return null;
    }
    final String tsCol = schema.getTimestampSpec().getTimestampColumn();
    if (tsCol != null && allColumns.contains(tsCol) && !filtered.contains(tsCol)) {
      filtered.add(tsCol);
    }
    return filtered;
  }

  private InputRow batchRowToInputRow(
      final ColumnarBatch batch,
      final int rowIdx,
      final Schema readSchema
  )
  {
    final int numCols = batch.numCols();
    final Map<String, Object> event = Maps.newHashMapWithExpectedSize(numCols);
    for (int col = 0; col < numCols; col++) {
      final ColumnVector column = batch.column(col);
      final Types.NestedField field = readSchema.columns().get(col);
      if (!column.isNullAt(rowIdx)) {
        event.put(field.name(), extractValue(column, field.type(), rowIdx));
      }
    }
    final long timestamp = schema.getTimestampSpec().extractTimestamp(event).getMillis();
    final List<String> dimensions = resolveDimensions(readSchema);
    return new MapBasedInputRow(timestamp, dimensions, event);
  }

  private List<String> resolveDimensions(final Schema readSchema)
  {
    final List<String> configured = schema.getDimensionsSpec().getDimensionNames();
    if (!configured.isEmpty()) {
      return configured;
    }
    final String tsCol = schema.getTimestampSpec().getTimestampColumn();
    final List<String> dims = new ArrayList<>(readSchema.columns().size());
    for (final Types.NestedField field : readSchema.columns()) {
      if (!field.name().equals(tsCol)) {
        dims.add(field.name());
      }
    }
    return dims;
  }

  /**
   * Type-safe extraction from Iceberg column accessors so physical dictionary encoding is not exposed.
   * Covers all scalar types supported by iceberg-arrow 1.10.0.
   */
  static Object extractValue(final ColumnVector column, final Type type, final int idx)
  {
    switch (type.typeId()) {
      case BOOLEAN:
        return column.getBoolean(idx);
      case INTEGER:
        return column.getInt(idx);
      case LONG:
        return column.getLong(idx);
      case FLOAT:
        return (double) column.getFloat(idx);
      case DOUBLE:
        return column.getDouble(idx);
      case STRING:
        return column.getString(idx);
      case BINARY:
      case FIXED:
      case UUID:
        return column.getBinary(idx);
      case DATE:
        return TimeUnit.DAYS.toMillis(column.getInt(idx));
      case TIME:
        return TimeUnit.MICROSECONDS.toMillis(column.getLong(idx));
      case TIMESTAMP:
        return TimeUnit.MICROSECONDS.toMillis(column.getLong(idx));
      case TIMESTAMP_NANO:
        return TimeUnit.NANOSECONDS.toMillis(column.getLong(idx));
      case DECIMAL:
        final Types.DecimalType decimalType = (Types.DecimalType) type;
        return column.getDecimal(idx, decimalType.precision(), decimalType.scale());
      default:
        throw new IllegalArgumentException("Unsupported Iceberg type: " + type);
    }
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
    private final Schema readSchema;
    private final ClassLoader extensionClassLoader;

    private ColumnarBatch currentBatch = null;
    private int rowIndexInBatch = 0;
    private boolean exhausted = false;

    ArrowInputRowIterator(
        final org.apache.iceberg.io.CloseableIterator<ColumnarBatch> batchIter,
        final ArrowReader arrowReader,
        final CloseableIterable<CombinedScanTask> tasks,
        final InputStats inputStats,
        final Schema readSchema,
        final ClassLoader extensionClassLoader
    )
    {
      this.batchIter = batchIter;
      this.arrowReader = arrowReader;
      this.tasks = tasks;
      this.inputStats = inputStats;
      this.readSchema = readSchema;
      this.extensionClassLoader = extensionClassLoader;
    }

    @Override
    public boolean hasNext()
    {
      final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(extensionClassLoader);
        return hasNextInternal();
      }
      finally {
        Thread.currentThread().setContextClassLoader(originalClassLoader);
      }
    }

    private boolean hasNextInternal()
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
      final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(extensionClassLoader);
        if (!hasNextInternal()) {
          throw new NoSuchElementException();
        }
        return batchRowToInputRow(currentBatch, rowIndexInBatch++, readSchema);
      }
      finally {
        Thread.currentThread().setContextClassLoader(originalClassLoader);
      }
    }

    private boolean loadNextBatch()
    {
      try {
        while (batchIter.hasNext()) {
          currentBatch = batchIter.next();
          rowIndexInBatch = 0;
          if (currentBatch.numRows() > 0) {
            inputStats.incrementProcessedBytes(estimateBatchBytes(currentBatch));
            return true;
          }
        }
      }
      catch (NullPointerException e) {
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.UNSUPPORTED)
                            .build(
                                e,
                                "Arrow reader does not support snapshots with data files written using "
                                + "different schemas. Use the standard Iceberg reader."
                            );
      }
      exhausted = true;
      return false;
    }

    private long estimateBatchBytes(final ColumnarBatch batch)
    {
      long bytes = 0;
      for (int col = 0; col < batch.numCols(); col++) {
        final FieldVector vector = batch.column(col).getFieldVector();
        if (vector != null) {
          bytes += vector.getBufferSize();
        }
      }
      return bytes;
    }

    @Override
    public void close() throws IOException
    {
      final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(extensionClassLoader);
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
      finally {
        Thread.currentThread().setContextClassLoader(originalClassLoader);
      }
    }
  }
}
