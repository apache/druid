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
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.hadoop.conf.Configuration;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DeltaInputRowTest
{
  public static Collection<Object[]> data()
  {
    Object[][] data = new Object[][]{
        {NonPartitionedDeltaTable.DELTA_TABLE_PATH, NonPartitionedDeltaTable.FULL_SCHEMA, NonPartitionedDeltaTable.DIMENSIONS, NonPartitionedDeltaTable.EXPECTED_ROWS},
        {PartitionedDeltaTable.DELTA_TABLE_PATH, PartitionedDeltaTable.FULL_SCHEMA, PartitionedDeltaTable.DIMENSIONS, PartitionedDeltaTable.EXPECTED_ROWS},
        {ComplexTypesDeltaTable.DELTA_TABLE_PATH, ComplexTypesDeltaTable.FULL_SCHEMA, ComplexTypesDeltaTable.DIMENSIONS, ComplexTypesDeltaTable.EXPECTED_ROWS},
        {SnapshotDeltaTable.DELTA_TABLE_PATH, SnapshotDeltaTable.FULL_SCHEMA, SnapshotDeltaTable.DIMENSIONS, SnapshotDeltaTable.LATEST_SNAPSHOT_EXPECTED_ROWS}
    };
    return Arrays.asList(data);
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testDeltaInputRow(
      final String deltaTablePath,
      final InputRowSchema schema,
      final List<String> dimensions,
      final List<Map<String, Object>> expectedRows
  ) throws TableNotFoundException, IOException
  {
    final Engine engine = DefaultEngine.create(new Configuration());
    final Scan scan = DeltaTestUtils.getScan(engine, deltaTablePath);

    final Row scanState = scan.getScanState(engine);
    final StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState);

    final CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(engine);
    int totalRecordCount = 0;
    while (scanFileIter.hasNext()) {
      final FilteredColumnarBatch scanFileBatch = scanFileIter.next();
      final CloseableIterator<Row> scanFileRows = scanFileBatch.getRows();

      while (scanFileRows.hasNext()) {
        final Row scanFile = scanFileRows.next();
        final FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFile);

        final CloseableIterator<ColumnarBatch> physicalDataIter = engine.getParquetHandler().readParquetFiles(
            Utils.singletonCloseableIterator(fileStatus),
            physicalReadSchema,
            Optional.empty()
        );
        final CloseableIterator<FilteredColumnarBatch> dataIter = Scan.transformPhysicalData(
            engine,
            scanState,
            scanFile,
            physicalDataIter
        );

        while (dataIter.hasNext()) {
          FilteredColumnarBatch dataReadResult = dataIter.next();
          Row next = dataReadResult.getRows().next();
          DeltaInputRow deltaInputRow = new DeltaInputRow(next, schema);
          Assert.assertNotNull(deltaInputRow);
          Assert.assertEquals(dimensions, deltaInputRow.getDimensions());

          Map<String, Object> expectedRow = expectedRows.get(totalRecordCount);
          for (String key : expectedRow.keySet()) {
            if (schema.getTimestampSpec().getTimestampColumn().equals(key)) {
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
    Assert.assertEquals(expectedRows.size(), totalRecordCount);
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testReadNonExistentTable()
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource("non-existent-table", null, null, null);

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> deltaInputSource.reader(null, null, null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "tablePath[non-existent-table] not found."
        )
    );
  }
}
