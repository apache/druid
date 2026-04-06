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
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * An {@link InputSourceReader} that uses Iceberg's native reader stack ({@link IcebergGenerics})
 * to read data files with v2 delete file application. The underlying {@code GenericReader}
 * handles both positional and equality deletes transparently.
 *
 * The reader operates in a fully streaming fashion:
 * <ul>
 *   <li>{@link IcebergGenerics#read} returns a {@link CloseableIterable} of {@link Record}
 *       that lazily opens and reads one {@code FileScanTask} at a time</li>
 *   <li>Each Record is converted to a Map on demand via {@link IcebergRecordConverter}</li>
 *   <li>No bulk materialization of records occurs at any point</li>
 * </ul>
 */
public class IcebergNativeRecordReader implements InputSourceReader
{
  private final Table table;
  private final InputRowSchema inputRowSchema;
  @Nullable
  private final Expression filterExpression;
  @Nullable
  private final Long snapshotTimeMillis;

  public IcebergNativeRecordReader(
      final Table table,
      final InputRowSchema inputRowSchema,
      @Nullable final Expression filterExpression,
      @Nullable final Long snapshotTimeMillis
  )
  {
    this.table = table;
    this.inputRowSchema = inputRowSchema;
    this.filterExpression = filterExpression;
    this.snapshotTimeMillis = snapshotTimeMillis;
  }

  @Override
  public CloseableIterator<InputRow> read(final InputStats inputStats) throws IOException
  {
    final CloseableIterable<Record> records = buildRecordIterable();
    final IcebergRecordConverter converter = new IcebergRecordConverter(table.schema());

    return new CloseableIterator<InputRow>()
    {
      private final Iterator<Record> delegate = records.iterator();

      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public InputRow next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final Record record = delegate.next();
        final Map<String, Object> map = converter.convert(record);

        return MapInputRowParser.parse(inputRowSchema, map);
      }

      @Override
      public void close() throws IOException
      {
        records.close();
      }
    };
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
        return InputRowListPlusRawValues.of(row, Collections.emptyMap());
      }

      @Override
      public void close() throws IOException
      {
        reader.close();
      }
    };
  }

  /**
   * Builds the streaming record iterable using IcebergGenerics public API.
   * Internally, IcebergGenerics creates a GenericReader that applies delete
   * files (both positional and equality) for each FileScanTask.
   */
  private CloseableIterable<Record> buildRecordIterable()
  {
    IcebergGenerics.ScanBuilder builder = IcebergGenerics.read(table);

    if (filterExpression != null) {
      builder = builder.where(filterExpression);
    }

    if (snapshotTimeMillis != null) {
      builder = builder.asOfTime(snapshotTimeMillis);
    }

    return builder.build();
  }
}
