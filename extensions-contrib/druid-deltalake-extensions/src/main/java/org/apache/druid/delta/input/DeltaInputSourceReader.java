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
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A reader for the Delta Lake input source. It initializes an iterator {@link DeltaInputSourceIterator}
 * for a subset of Delta records given by {@link FilteredColumnarBatch} and schema {@link InputRowSchema}.
 *
 */
public class DeltaInputSourceReader implements InputSourceReader
{
  private final Iterator<io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch>> filteredColumnarBatchIterators;
  private final InputRowSchema inputRowSchema;

  public DeltaInputSourceReader(
      Iterator<io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch>> filteredColumnarBatchIterators,
      InputRowSchema inputRowSchema

  )
  {
    this.filteredColumnarBatchIterators = filteredColumnarBatchIterators;
    this.inputRowSchema = inputRowSchema;
  }

  @Override
  public CloseableIterator<InputRow> read()
  {
    return new DeltaInputSourceIterator(filteredColumnarBatchIterators, inputRowSchema);
  }

  @Override
  public CloseableIterator<InputRow> read(InputStats inputStats)
  {
    return new DeltaInputSourceIterator(filteredColumnarBatchIterators, inputRowSchema);
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample()
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
    private final Iterator<io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch>> filteredColumnarBatchIterators;

    private io.delta.kernel.utils.CloseableIterator<Row> currentBatch = null;
    private final InputRowSchema inputRowSchema;

    public DeltaInputSourceIterator(
        Iterator<io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch>> filteredColumnarBatchCloseableIterator,
        InputRowSchema inputRowSchema
    )
    {
      this.filteredColumnarBatchIterators = filteredColumnarBatchCloseableIterator;
      this.inputRowSchema = inputRowSchema;
    }

    @Override
    public boolean hasNext()
    {
      while (currentBatch == null || !currentBatch.hasNext()) {
        if (!filteredColumnarBatchIterators.hasNext()) {
          return false; // No more batches or records to read!
        }

        final io.delta.kernel.utils.CloseableIterator<FilteredColumnarBatch> filteredBatchIterator =
            filteredColumnarBatchIterators.next();

        while (filteredBatchIterator.hasNext()) {
          currentBatch = filteredBatchIterator.next().getRows();
          if (currentBatch.hasNext()) {
            return true;
          }
        }
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
      return new DeltaInputRow(dataRow, inputRowSchema);
    }

    @Override
    public void close() throws IOException
    {
      if (currentBatch != null) {
        currentBatch.close();
      }

      if (filteredColumnarBatchIterators.hasNext()) {
        filteredColumnarBatchIterators.next().close();
      }
    }
  }
}
