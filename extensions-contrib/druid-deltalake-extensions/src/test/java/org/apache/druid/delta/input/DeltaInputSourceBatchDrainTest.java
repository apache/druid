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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Regression test for https://github.com/apache/druid/issues/18606.
 *
 * The bug: {@code DeltaInputSourceIterator.hasNext()} used a local variable for
 * the per-file {@code CloseableIterator<FilteredColumnarBatch>}. When the method
 * returned {@code true} after the first non-empty batch of a file, the iterator
 * went out of scope. The next {@code hasNext()} call advanced to the next file,
 * skipping all remaining batches. With Delta kernel's default batch size of 1024
 * rows this caused exactly {@code 1024 × numFiles} rows to be returned regardless
 * of actual file size.
 *
 * The fix promotes the per-file iterator to a field ({@code currentFileIterator})
 * so all batches are drained before advancing to the next file.
 *
 * Test table: 2 Parquet files × 2000 rows = 4000 rows total.
 * Without the fix: 1024 × 2 = 2048 rows returned.
 * With the fix:    4000 rows returned.
 */
public class DeltaInputSourceBatchDrainTest
{
  @Test
  public void testAllRowsReturnedWhenFilesExceedOneBatch() throws IOException
  {
    final DeltaInputSource inputSource = new DeltaInputSource(
        LargeRowGroupDeltaTable.DELTA_TABLE_PATH,
        null,
        null,
        null
    );

    final InputSourceReader reader = inputSource.reader(
        LargeRowGroupDeltaTable.SCHEMA,
        null,
        null
    );

    final List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next());
      }
    }

    Assert.assertEquals(
        "Expected all rows to be read — regression check for GH-18606 "
        + "(DeltaInputSourceIterator only returned first 1024 rows per file). "
        + "Got " + rows.size() + " rows, expected " + LargeRowGroupDeltaTable.EXPECTED_ROW_COUNT + ".",
        LargeRowGroupDeltaTable.EXPECTED_ROW_COUNT,
        rows.size()
    );
  }
}