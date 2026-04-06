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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * An {@link InputSourceReader} that uses Iceberg's native reader stack to read data files
 * with v2 delete file application. This reader uses {@link GenericReader} internally, which
 * handles both positional and equality deletes via {@link org.apache.iceberg.data.DeleteFilter}.
 *
 * The reader operates in a fully streaming fashion:
 * <ul>
 *   <li>FileScanTasks are iterated lazily, one at a time</li>
 *   <li>For each task, GenericReader opens a streaming CloseableIterable of Records with deletes applied</li>
 *   <li>Each Record is converted to a Map on demand via IcebergRecordConverter</li>
 *   <li>No bulk materialization of records into a List occurs at any point</li>
 * </ul>
 */
public class IcebergNativeRecordReader implements InputSourceReader
{
  private final FileIO fileIO;
  private final Schema tableSchema;
  private final Schema projectedSchema;
  private final Iterable<FileScanTask> fileScanTasks;
  private final InputRowSchema inputRowSchema;
  private final IcebergRecordConverter converter;
  private final boolean reuseContainers;

  public IcebergNativeRecordReader(
      final FileIO fileIO,
      final Schema tableSchema,
      final Schema projectedSchema,
      final Iterable<FileScanTask> fileScanTasks,
      final InputRowSchema inputRowSchema
  )
  {
    this.fileIO = fileIO;
    this.tableSchema = tableSchema;
    this.projectedSchema = projectedSchema;
    this.fileScanTasks = fileScanTasks;
    this.inputRowSchema = inputRowSchema;
    this.converter = new IcebergRecordConverter(projectedSchema);
    this.reuseContainers = false;
  }

  @Override
  public CloseableIterator<InputRow> read(final InputStats inputStats) throws IOException
  {
    return new FileScanTaskInputRowIterator(inputStats);
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    final CloseableIterator<InputRow> reader = read(null);
    return new CloseableIterator<InputRowListPlusRawValues>()
    {
      @Override
      public boolean hasNext()
      {
        return reader.hasNext();
      }

      @Override
      public InputRowListPlusRawValues next()
      {
        final InputRow row = reader.next();
        return InputRowListPlusRawValues.of(row, Collections.singletonList(row));
      }

      @Override
      public void close() throws IOException
      {
        reader.close();
      }
    };
  }

  /**
   * Streaming iterator that concatenates records across all FileScanTasks.
   * Opens one task at a time, iterates its records with deletes applied,
   * then closes it and opens the next.
   */
  private class FileScanTaskInputRowIterator implements CloseableIterator<InputRow>
  {
    private final InputStats inputStats;
    private final Iterator<FileScanTask> taskIterator;
    private CloseableIterable<Record> currentIterable;
    private Iterator<Record> currentRecordIterator;
    private boolean finished;

    FileScanTaskInputRowIterator(final InputStats inputStats)
    {
      this.inputStats = inputStats;
      this.taskIterator = fileScanTasks.iterator();
      this.finished = false;
    }

    @Override
    public boolean hasNext()
    {
      if (finished) {
        return false;
      }

      while (currentRecordIterator == null || !currentRecordIterator.hasNext()) {
        // Close current task's iterable before opening next
        closeCurrentIterable();

        if (!taskIterator.hasNext()) {
          finished = true;
          return false;
        }

        final FileScanTask task = taskIterator.next();
        openTask(task);
      }

      return true;
    }

    @Override
    public InputRow next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      final Record record = currentRecordIterator.next();
      final Map<String, Object> map = converter.convert(record);

      final List<InputRow> rows = MapInputRowParser.parse(
          inputRowSchema.getTimestampSpec(),
          inputRowSchema.getDimensionsSpec(),
          map
      );

      // MapInputRowParser.parse returns a single-element list
      return rows.get(0);
    }

    @Override
    public void close() throws IOException
    {
      closeCurrentIterable();
      finished = true;
    }

    private void openTask(final FileScanTask task)
    {
      final GenericReader reader = new GenericReader(
          fileIO,
          tableSchema,
          projectedSchema,
          /* caseSensitive */ true,
          reuseContainers
      );
      currentIterable = reader.open(task);
      currentRecordIterator = currentIterable.iterator();
    }

    private void closeCurrentIterable()
    {
      if (currentIterable != null) {
        try {
          currentIterable.close();
        }
        catch (IOException e) {
          throw new RuntimeException("Failed to close Iceberg record iterable", e);
        }
        currentIterable = null;
        currentRecordIterator = null;
      }
    }
  }
}
