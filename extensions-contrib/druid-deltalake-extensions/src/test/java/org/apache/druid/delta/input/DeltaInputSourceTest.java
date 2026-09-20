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

import io.delta.kernel.exceptions.KernelException;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.delta.DeltaAssertions;
import org.apache.druid.delta.filter.DeltaAndFilter;
import org.apache.druid.delta.filter.DeltaEqualsFilter;
import org.apache.druid.delta.filter.DeltaFilter;
import org.apache.druid.delta.filter.DeltaGreaterThanFilter;
import org.apache.druid.delta.filter.DeltaGreaterThanOrEqualsFilter;
import org.apache.druid.delta.filter.DeltaLessThanOrEqualsFilter;
import org.apache.druid.delta.filter.DeltaNotFilter;
import org.apache.druid.delta.filter.DeltaOrFilter;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DeltaInputSourceTest
{
  @BeforeEach
  public void setUp()
  {
    System.setProperty("user.timezone", "UTC");
  }

  @Nested
  public class TablePathParameterTests
  {
    public static Object[][] data()
    {
      return new Object[][]{
          {
              NonPartitionedDeltaTable.DELTA_TABLE_PATH,
              NonPartitionedDeltaTable.FULL_SCHEMA,
              null,
              NonPartitionedDeltaTable.EXPECTED_ROWS
          },
          {
              NonPartitionedDeltaTable.DELTA_TABLE_PATH,
              NonPartitionedDeltaTable.SCHEMA_1,
              null,
              NonPartitionedDeltaTable.EXPECTED_ROWS
          },
          {
              NonPartitionedDeltaTable.DELTA_TABLE_PATH,
              NonPartitionedDeltaTable.SCHEMA_2,
              null,
              NonPartitionedDeltaTable.EXPECTED_ROWS
          },
          {
              PartitionedDeltaTable.DELTA_TABLE_PATH,
              PartitionedDeltaTable.FULL_SCHEMA,
              null,
              PartitionedDeltaTable.EXPECTED_ROWS
          },
          {
              ComplexTypesDeltaTable.DELTA_TABLE_PATH,
              ComplexTypesDeltaTable.FULL_SCHEMA,
              null,
              ComplexTypesDeltaTable.EXPECTED_ROWS
          },
          {
              SnapshotDeltaTable.DELTA_TABLE_PATH,
              SnapshotDeltaTable.FULL_SCHEMA,
              0L,
              SnapshotDeltaTable.V0_SNAPSHOT_EXPECTED_ROWS
          },
          {
              SnapshotDeltaTable.DELTA_TABLE_PATH,
              SnapshotDeltaTable.FULL_SCHEMA,
              1L,
              SnapshotDeltaTable.V1_SNAPSHOT_EXPECTED_ROWS
          },
          {
              SnapshotDeltaTable.DELTA_TABLE_PATH,
              SnapshotDeltaTable.FULL_SCHEMA,
              2L,
              SnapshotDeltaTable.V2_SNAPSHOT_EXPECTED_ROWS
          },
          {
              SnapshotDeltaTable.DELTA_TABLE_PATH,
              SnapshotDeltaTable.FULL_SCHEMA,
              3L,
              SnapshotDeltaTable.LATEST_SNAPSHOT_EXPECTED_ROWS
          },
          {
              SnapshotDeltaTable.DELTA_TABLE_PATH,
              SnapshotDeltaTable.FULL_SCHEMA,
              null,
              SnapshotDeltaTable.LATEST_SNAPSHOT_EXPECTED_ROWS
          }
      };
    }
    @MethodSource("data")
    @ParameterizedTest
    public void testSampleDeltaTable(
        String deltaTablePath,
        InputRowSchema schema,
        Long snapshotVersion,
        List<Map<String, Object>> expectedRows
    ) throws IOException
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource(deltaTablePath, null, null, snapshotVersion);
      final InputSourceReader inputSourceReader = deltaInputSource.reader(schema, null, null);

      List<InputRowListPlusRawValues> actualSampledRows = sampleAllRows(inputSourceReader);
      Assertions.assertEquals(expectedRows.size(), actualSampledRows.size());

      for (int idx = 0; idx < expectedRows.size(); idx++) {
        Map<String, Object> expectedRow = expectedRows.get(idx);
        InputRowListPlusRawValues actualSampledRow = actualSampledRows.get(idx);
        Assertions.assertNull(actualSampledRow.getParseException());

        Map<String, Object> actualSampledRawVals = actualSampledRow.getRawValues();
        Assertions.assertNotNull(actualSampledRawVals);
        Assertions.assertNotNull(actualSampledRow.getRawValuesList());
        Assertions.assertEquals(1, actualSampledRow.getRawValuesList().size());

        for (String key : expectedRow.keySet()) {
          if (!schema.getColumnsFilter().apply(key)) {
            Assertions.assertNull(actualSampledRawVals.get(key));
          } else {
            if (schema.getTimestampSpec().getTimestampColumn().equals(key)) {
              final long expectedMillis = (Long) expectedRow.get(key);
              Assertions.assertEquals(expectedMillis, actualSampledRawVals.get(key));
            } else {
              Assertions.assertEquals(expectedRow.get(key), actualSampledRawVals.get(key));
            }
          }
        }
      }
    }

    @MethodSource("data")
    @ParameterizedTest
    public void testReadDeltaTable(
        String deltaTablePath,
        InputRowSchema schema,
        Long snapshotVersion,
        List<Map<String, Object>> expectedRows
    ) throws IOException
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource(deltaTablePath, null, null, snapshotVersion);
      final InputSourceReader inputSourceReader = deltaInputSource.reader(schema, null, null);
      final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
      validateRows(expectedRows, actualReadRows, schema);
    }

  }

  @Nested
  public class FilterParameterTests
  {
    public static Object[][] data()
    {
      return new Object[][]{
          {
              PartitionedDeltaTable.DELTA_TABLE_PATH,
              new DeltaEqualsFilter("name", "Employee2"),
              PartitionedDeltaTable.FULL_SCHEMA,
              filterExpectedRows(
                  PartitionedDeltaTable.EXPECTED_ROWS,
                  row -> row.get("name").equals("Employee2")
              )
          },
          {
              PartitionedDeltaTable.DELTA_TABLE_PATH,
              new DeltaGreaterThanFilter("name", "Employee3"),
              PartitionedDeltaTable.FULL_SCHEMA,
              filterExpectedRows(
                  PartitionedDeltaTable.EXPECTED_ROWS,
                  row -> ((String) row.get("name")).compareTo("Employee3") > 0
              )
          },
          {
              PartitionedDeltaTable.DELTA_TABLE_PATH,
              new DeltaLessThanOrEqualsFilter("name", "Employee4"),
              PartitionedDeltaTable.FULL_SCHEMA,
              filterExpectedRows(
                  PartitionedDeltaTable.EXPECTED_ROWS,
                  row -> ((String) row.get("name")).compareTo("Employee4") <= 0
              )
          },
          {
              PartitionedDeltaTable.DELTA_TABLE_PATH,
              new DeltaAndFilter(
                  Arrays.asList(
                      new DeltaEqualsFilter("name", "Employee1"),
                      new DeltaEqualsFilter("name", "Employee4")
                  )
              ),
              PartitionedDeltaTable.FULL_SCHEMA,
              filterExpectedRows(
                  PartitionedDeltaTable.EXPECTED_ROWS,
                  row -> row.get("name").equals("Employee1") && row.get("name").equals("Employee4")
              )
          },
          {
              PartitionedDeltaTable.DELTA_TABLE_PATH,
              new DeltaOrFilter(
                  Arrays.asList(
                      new DeltaEqualsFilter("name", "Employee5"),
                      new DeltaEqualsFilter("name", "Employee1")
                  )
              ),
              PartitionedDeltaTable.FULL_SCHEMA,
              filterExpectedRows(
                  PartitionedDeltaTable.EXPECTED_ROWS,
                  row -> row.get("name").equals("Employee5") || row.get("name").equals("Employee1")
              )
          },
          {
              PartitionedDeltaTable.DELTA_TABLE_PATH,
              new DeltaNotFilter(
                  new DeltaOrFilter(
                      Arrays.asList(
                          new DeltaEqualsFilter("name", "Employee5"),
                          new DeltaEqualsFilter("name", "Employee1")
                      )
                  )
              ),
              PartitionedDeltaTable.FULL_SCHEMA,
              filterExpectedRows(
                  PartitionedDeltaTable.EXPECTED_ROWS,
                  row -> !(row.get("name").equals("Employee5") || row.get("name").equals("Employee1"))
              )
          },
          {
              PartitionedDeltaTable.DELTA_TABLE_PATH,
              new DeltaNotFilter(
                  new DeltaAndFilter(
                      Arrays.asList(
                          new DeltaEqualsFilter("name", "Employee1"),
                          new DeltaEqualsFilter("name", "Employee4")
                      )
                  )
              ),
              PartitionedDeltaTable.FULL_SCHEMA,
              filterExpectedRows(
                  PartitionedDeltaTable.EXPECTED_ROWS,
                  row -> (!(row.get("name").equals("Employee1") && row.get("name").equals("Employee4")))
              )
          },
          {
              PartitionedDeltaTable.DELTA_TABLE_PATH,
              new DeltaNotFilter(
                  new DeltaOrFilter(
                      Arrays.asList(
                          new DeltaEqualsFilter("name", "Employee1"),
                          new DeltaGreaterThanOrEqualsFilter("name", "Employee4")
                      )
                  )
              ),
              PartitionedDeltaTable.FULL_SCHEMA,
              filterExpectedRows(
                  PartitionedDeltaTable.EXPECTED_ROWS,
                  row -> (!(row.get("name").equals("Employee1") || ((String) row.get("name")).compareTo("Employee4") >= 0))
              )
          }
      };
    }

    @MethodSource("data")
    @ParameterizedTest
    public void testSampleDeltaTable(
        String deltaTablePath,
        DeltaFilter filter,
        InputRowSchema schema,
        List<Map<String, Object>> expectedRows
    ) throws IOException
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource(deltaTablePath, null, filter, null);
      final InputSourceReader inputSourceReader = deltaInputSource.reader(schema, null, null);

      List<InputRowListPlusRawValues> actualSampledRows = sampleAllRows(inputSourceReader);
      Assertions.assertEquals(expectedRows.size(), actualSampledRows.size());

      for (int idx = 0; idx < expectedRows.size(); idx++) {
        Map<String, Object> expectedRow = expectedRows.get(idx);
        InputRowListPlusRawValues actualSampledRow = actualSampledRows.get(idx);
        Assertions.assertNull(actualSampledRow.getParseException());

        Map<String, Object> actualSampledRawVals = actualSampledRow.getRawValues();
        Assertions.assertNotNull(actualSampledRawVals);
        Assertions.assertNotNull(actualSampledRow.getRawValuesList());
        Assertions.assertEquals(1, actualSampledRow.getRawValuesList().size());

        for (String key : expectedRow.keySet()) {
          if (!schema.getColumnsFilter().apply(key)) {
            Assertions.assertNull(actualSampledRawVals.get(key));
          } else {
            if (schema.getTimestampSpec().getTimestampColumn().equals(key)) {
              final long expectedMillis = (Long) expectedRow.get(key);
              Assertions.assertEquals(expectedMillis, actualSampledRawVals.get(key));
            } else {
              Assertions.assertEquals(expectedRow.get(key), actualSampledRawVals.get(key));
            }
          }
        }
      }
    }

    private static List<Map<String, Object>> filterExpectedRows(
        final List<Map<String, Object>> rows,
        final Predicate<Map<String, Object>> filter
    )
    {
      return rows.stream().filter(filter).collect(Collectors.toList());
    }

    @MethodSource("data")
    @ParameterizedTest
    public void testReadDeltaTable(
        String deltaTablePath,
        DeltaFilter filter,
        InputRowSchema schema,
        List<Map<String, Object>> expectedRows
    ) throws IOException
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource(deltaTablePath, null, filter, null);
      final InputSourceReader inputSourceReader = deltaInputSource.reader(schema, null, null);
      final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
      validateRows(expectedRows, actualReadRows, schema);
    }

  }

  @Nested
  public class InvalidInputTests
  {
    @Test
    public void testNullTable()
    {
      DeltaAssertions.assertInvalidInput(
          Assertions.assertThrows(
              DruidException.class,
              () -> new DeltaInputSource(null, null, null, null)
          ),
          "tablePath cannot be null."
      );
    }

    @Test
    public void testSplitNonExistentTable()
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource("non-existent-table", null, null, null);

      DeltaAssertions.assertInvalidInput(
          Assertions.assertThrows(
              DruidException.class,
              () -> deltaInputSource.createSplits(null, null)
          ),
          "tablePath[non-existent-table] not found."
      );
    }

    @Test
    public void testReadNonExistentTable()
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource("non-existent-table", null, null, null);

      DeltaAssertions.assertInvalidInput(
          Assertions.assertThrows(
              DruidException.class,
              () -> deltaInputSource.reader(null, null, null)
          ),
          "tablePath[non-existent-table] not found."
      );
    }

    @Test
    public void testReadNonExistentSnapshot()
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource(
          SnapshotDeltaTable.DELTA_TABLE_PATH,
          null,
          null,
          100L
      );

      Assertions.assertThrows(
          KernelException.class,
          () -> deltaInputSource.reader(null, null, null)
      );
    }
  }

  private static List<InputRowListPlusRawValues> sampleAllRows(InputSourceReader reader) throws IOException
  {
    List<InputRowListPlusRawValues> rows = new ArrayList<>();
    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }

  private static List<InputRow> readAllRows(InputSourceReader reader) throws IOException
  {
    final List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }

  /**
   * Regression test for https://github.com/apache/druid/issues/18606.
   *
   * {@link DeltaInputSourceReader.DeltaInputSourceIterator} used a local variable for the
   * per-file {@code CloseableIterator<FilteredColumnarBatch>}. When {@code hasNext()} returned
   * after the first non-empty batch of a file, that iterator went out of scope. The next
   * {@code hasNext()} call advanced to the next file, skipping all remaining batches of the
   * current file. With the Delta kernel default batch size of 1024 rows this produced exactly
   * {@code 1024 * numFiles} rows regardless of actual file size.
   *
   * Test table: 2 Parquet files x 2000 rows = 4000 rows total.
   * Without the fix: 1024 x 2 = 2048 rows.
   * With the fix:    4000 rows.
   */
  @Nested
  public class BatchDrainRegressionTests
  {
    @Test
    public void testAllRowsReturnedWhenFileExceedsOneBatch() throws IOException
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource(
          LargeRowGroupDeltaTable.DELTA_TABLE_PATH,
          null,
          null,
          null
      );
      final InputSourceReader inputSourceReader = deltaInputSource.reader(
          LargeRowGroupDeltaTable.SCHEMA,
          null,
          null
      );
      final List<InputRow> rows = readAllRows(inputSourceReader);
      Assertions.assertEquals(
          LargeRowGroupDeltaTable.EXPECTED_ROW_COUNT,
          rows.size(),
          "Expected all rows to be read. "
          + "If this fails with " + (1024 * 2) + " rows, the per-file batch drain bug (GH-18606) has regressed."
      );
    }
  }

  private static void validateRows(
      final List<Map<String, Object>> expectedRows,
      final List<InputRow> actualReadRows,
      final InputRowSchema schema
  )
  {
    Assertions.assertEquals(expectedRows.size(), actualReadRows.size());

    for (int idx = 0; idx < expectedRows.size(); idx++) {
      final Map<String, Object> expectedRow = expectedRows.get(idx);
      final InputRow actualInputRow = actualReadRows.get(idx);
      for (String key : expectedRow.keySet()) {
        if (!schema.getColumnsFilter().apply(key)) {
          Assertions.assertNull(actualInputRow.getRaw(key));
        } else {
          if (schema.getTimestampSpec().getTimestampColumn().equals(key)) {
            final long expectedMillis = (Long) expectedRow.get(key) * 1000;
            Assertions.assertEquals(expectedMillis, actualInputRow.getTimestampFromEpoch());
            Assertions.assertEquals(DateTimes.utc(expectedMillis), actualInputRow.getTimestamp());
          } else {
            Assertions.assertEquals(expectedRow.get(key), actualInputRow.getRaw(key));
          }
        }
      }
    }
  }
}
