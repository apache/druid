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

import io.delta.kernel.Scan;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class DeltaInputRowTest
{
  @Test
  public void testDeltaInputRow() throws TableNotFoundException, IOException
  {
    final TableClient tableClient = DefaultTableClient.create(new Configuration());
    final Scan scan = DeltaTestUtils.getScan(tableClient);

    final Row scanState = scan.getScanState(tableClient);
    final StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(tableClient, scanState);

    final CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(tableClient);
    int totalRecordCount = 0;
    while (scanFileIter.hasNext()) {
      final FilteredColumnarBatch scanFileBatch = scanFileIter.next();
      final CloseableIterator<Row> scanFileRows = scanFileBatch.getRows();

      while (scanFileRows.hasNext()) {
        final Row scanFile = scanFileRows.next();
        final FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFile);

        final CloseableIterator<ColumnarBatch> physicalDataIter = tableClient.getParquetHandler().readParquetFiles(
            Utils.singletonCloseableIterator(fileStatus),
            physicalReadSchema,
            Optional.empty()
        );
        final CloseableIterator<FilteredColumnarBatch> dataIter = Scan.transformPhysicalData(
            tableClient,
            scanState,
            scanFile,
            physicalDataIter
        );

        while (dataIter.hasNext()) {
          FilteredColumnarBatch dataReadResult = dataIter.next();
          Row next = dataReadResult.getRows().next();
          DeltaInputRow deltaInputRow = new DeltaInputRow(next, DeltaTestUtils.FULL_SCHEMA);
          Assert.assertNotNull(deltaInputRow);
          Assert.assertEquals(DeltaTestUtils.DIMENSIONS, deltaInputRow.getDimensions());

          Map<String, Object> expectedRow = DeltaTestUtils.EXPECTED_ROWS.get(totalRecordCount);
          for (String key : expectedRow.keySet()) {
            if (DeltaTestUtils.FULL_SCHEMA.getTimestampSpec().getTimestampColumn().equals(key)) {
              final long expectedMillis = ((Long) expectedRow.get(key)) * 1000;
              Assert.assertEquals(expectedMillis, deltaInputRow.getTimestampFromEpoch());
            } else {
              Assert.assertEquals(expectedRow.get(key), deltaInputRow.getRaw(key));
            }
          }
          totalRecordCount += 1;
        }
      }
    }
    Assert.assertEquals(DeltaTestUtils.EXPECTED_ROWS.size(), totalRecordCount);
  }
}
