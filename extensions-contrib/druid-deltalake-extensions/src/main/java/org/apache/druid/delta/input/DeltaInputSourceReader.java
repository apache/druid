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

package org.apache.druid.delta.input;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.NoSuchElementException;

public class DeltaInputSourceReader implements InputSourceReader
{
  private static final Logger log = new Logger(DeltaInputSourceReader.class);

  private final io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator;
  private final InputRowSchema inputRowSchema;

  public DeltaInputSourceReader(
      io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator,
      InputRowSchema inputRowSchema
  )
  {
    this.filteredColumnarBatchCloseableIterator = filteredColumnarBatchCloseableIterator;
    this.inputRowSchema = inputRowSchema;
  }

  @Override
  public CloseableIterator<InputRow> read()
  {
    return new DeltaInputSourceIterator(filteredColumnarBatchCloseableIterator, inputRowSchema);
  }

  @Override
  public CloseableIterator<InputRow> read(InputStats inputStats) throws IOException
  {
    return new DeltaInputSourceIterator(filteredColumnarBatchCloseableIterator, inputRowSchema);
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    CloseableIterator<InputRow> inner = read();
    return new CloseableIterator<InputRowListPlusRawValues>()
    {
      @Override
      public void close() throws IOException
      {
        inner.close();
      }

      @Override
      public boolean hasNext()
      {
        return inner.hasNext();
      }

      @Override
      public InputRowListPlusRawValues next()
      {
        DeltaInputRow deltaInputRow = (DeltaInputRow) inner.next();
        return InputRowListPlusRawValues.of(deltaInputRow, deltaInputRow.getRawRowAsMap());
      }
    };
  }

  private static class DeltaInputSourceIterator implements CloseableIterator<InputRow>
  {
    private final io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator;

    private io.delta.kernel.utils.CloseableIterator<Row> currentBatch = null;
    private final InputRowSchema inputRowSchema;

    public DeltaInputSourceIterator(
        io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredColumnarBatchCloseableIterator,
        InputRowSchema inputRowSchema
    )
    {
      this.filteredColumnarBatchCloseableIterator = filteredColumnarBatchCloseableIterator;
      this.inputRowSchema = inputRowSchema;
    }

    @Override
    public boolean hasNext()
    {
      while (currentBatch == null || !currentBatch.hasNext()) {
        if (!filteredColumnarBatchCloseableIterator.hasNext()) {
          return false; // No more batches or records to read!
        }
        currentBatch = filteredColumnarBatchCloseableIterator.next().getRows();
      }
      return true;
    }

    @Override
    public InputRow next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Row dataRow = currentBatch.next();
      System.out.println("Datarow" + dataRow);
      // TODO: construct schema? remove this after debugging
      return new DeltaInputRow(dataRow, inputRowSchema);
    }

    @Override
    public void close() throws IOException
    {
      filteredColumnarBatchCloseableIterator.close();
    }
  }
}
