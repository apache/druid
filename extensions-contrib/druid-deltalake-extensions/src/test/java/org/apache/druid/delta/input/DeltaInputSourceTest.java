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
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.delta.filter.DeltaAndFilter;
import org.apache.druid.delta.filter.DeltaEqualsFilter;
import org.apache.druid.delta.filter.DeltaFilter;
import org.apache.druid.delta.filter.DeltaGreaterThanFilter;
import org.apache.druid.delta.filter.DeltaGreaterThanOrEqualsFilter;
import org.apache.druid.delta.filter.DeltaLessThanOrEqualsFilter;
import org.apache.druid.delta.filter.DeltaNotFilter;
import org.apache.druid.delta.filter.DeltaOrFilter;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DeltaInputSourceTest
{
  @Before
  public void setUp()
  {
    System.setProperty("user.timezone", "UTC");
  }

  @RunWith(Parameterized.class)
  public static class TablePathParameterTests
  {
    @Parameterized.Parameters
    public static Object[][] data()
    {
      return new Object[][]{
          {
              NonPartitionedDeltaTable.DELTA_TABLE_PATH,
              NonPartitionedDeltaTable.FULL_SCHEMA,
              NonPartitionedDeltaTable.EXPECTED_ROWS
          },
          {
              NonPartitionedDeltaTable.DELTA_TABLE_PATH,
              NonPartitionedDeltaTable.SCHEMA_1,
              NonPartitionedDeltaTable.EXPECTED_ROWS
          },
          {
              NonPartitionedDeltaTable.DELTA_TABLE_PATH,
              NonPartitionedDeltaTable.SCHEMA_2,
              NonPartitionedDeltaTable.EXPECTED_ROWS
          },
          {
              PartitionedDeltaTable.DELTA_TABLE_PATH,
              PartitionedDeltaTable.FULL_SCHEMA,
              PartitionedDeltaTable.EXPECTED_ROWS
          }
      };
    }

    @Parameterized.Parameter(0)
    public String deltaTablePath;
    @Parameterized.Parameter(1)
    public InputRowSchema schema;
    @Parameterized.Parameter(2)
    public List<Map<String, Object>> expectedRows;

    @Test
    public void testSampleDeltaTable() throws IOException
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource(deltaTablePath, null, null);
      final InputSourceReader inputSourceReader = deltaInputSource.reader(schema, null, null);

      List<InputRowListPlusRawValues> actualSampledRows = sampleAllRows(inputSourceReader);
      Assert.assertEquals(expectedRows.size(), actualSampledRows.size());

      for (int idx = 0; idx < expectedRows.size(); idx++) {
        Map<String, Object> expectedRow = expectedRows.get(idx);
        InputRowListPlusRawValues actualSampledRow = actualSampledRows.get(idx);
        Assert.assertNull(actualSampledRow.getParseException());

        Map<String, Object> actualSampledRawVals = actualSampledRow.getRawValues();
        Assert.assertNotNull(actualSampledRawVals);
        Assert.assertNotNull(actualSampledRow.getRawValuesList());
        Assert.assertEquals(1, actualSampledRow.getRawValuesList().size());

        for (String key : expectedRow.keySet()) {
          if (!schema.getColumnsFilter().apply(key)) {
            Assert.assertNull(actualSampledRawVals.get(key));
          } else {
            if (schema.getTimestampSpec().getTimestampColumn().equals(key)) {
              final long expectedMillis = (Long) expectedRow.get(key);
              Assert.assertEquals(expectedMillis, actualSampledRawVals.get(key));
            } else {
              Assert.assertEquals(expectedRow.get(key), actualSampledRawVals.get(key));
            }
          }
        }
      }
    }

    @Test
    public void testReadDeltaTable() throws IOException
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource(deltaTablePath, null, null);
      final InputSourceReader inputSourceReader = deltaInputSource.reader(schema, null, null);
      final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
      validateRows(expectedRows, actualReadRows, schema);
    }
  }

  @RunWith(Parameterized.class)
  public static class FilterParameterTests
  {
    @Parameterized.Parameters
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

    @Parameterized.Parameter(0)
    public String deltaTablePath;
    @Parameterized.Parameter(1)
    public DeltaFilter filter;
    @Parameterized.Parameter(2)
    public InputRowSchema schema;
    @Parameterized.Parameter(3)
    public List<Map<String, Object>> expectedRows;

    @Test
    public void testSampleDeltaTable() throws IOException
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource(deltaTablePath, null, filter);
      final InputSourceReader inputSourceReader = deltaInputSource.reader(schema, null, null);

      List<InputRowListPlusRawValues> actualSampledRows = sampleAllRows(inputSourceReader);
      Assert.assertEquals(expectedRows.size(), actualSampledRows.size());

      for (int idx = 0; idx < expectedRows.size(); idx++) {
        Map<String, Object> expectedRow = expectedRows.get(idx);
        InputRowListPlusRawValues actualSampledRow = actualSampledRows.get(idx);
        Assert.assertNull(actualSampledRow.getParseException());

        Map<String, Object> actualSampledRawVals = actualSampledRow.getRawValues();
        Assert.assertNotNull(actualSampledRawVals);
        Assert.assertNotNull(actualSampledRow.getRawValuesList());
        Assert.assertEquals(1, actualSampledRow.getRawValuesList().size());

        for (String key : expectedRow.keySet()) {
          if (!schema.getColumnsFilter().apply(key)) {
            Assert.assertNull(actualSampledRawVals.get(key));
          } else {
            if (schema.getTimestampSpec().getTimestampColumn().equals(key)) {
              final long expectedMillis = (Long) expectedRow.get(key);
              Assert.assertEquals(expectedMillis, actualSampledRawVals.get(key));
            } else {
              Assert.assertEquals(expectedRow.get(key), actualSampledRawVals.get(key));
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

    @Test
    public void testReadDeltaTable() throws IOException
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource(deltaTablePath, null, filter);
      final InputSourceReader inputSourceReader = deltaInputSource.reader(schema, null, null);
      final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
      validateRows(expectedRows, actualReadRows, schema);
    }
  }

  public static class InvalidInputTests
  {
    @Test
    public void testNullTable()
    {
      MatcherAssert.assertThat(
          Assert.assertThrows(
              DruidException.class,
              () -> new DeltaInputSource(null, null, null)
          ),
          DruidExceptionMatcher.invalidInput().expectMessageIs(
              "tablePath cannot be null."
          )
      );
    }

    @Test
    public void testSplitNonExistentTable()
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource("non-existent-table", null, null);

      MatcherAssert.assertThat(
          Assert.assertThrows(
              DruidException.class,
              () -> deltaInputSource.createSplits(null, null)
          ),
          DruidExceptionMatcher.invalidInput().expectMessageIs(
              "tablePath[non-existent-table] not found."
          )
      );
    }

    @Test
    public void testReadNonExistentTable()
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource("non-existent-table", null, null);

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

  private static void validateRows(
      final List<Map<String, Object>> expectedRows,
      final List<InputRow> actualReadRows,
      final InputRowSchema schema
  )
  {
    Assert.assertEquals(expectedRows.size(), actualReadRows.size());

    for (int idx = 0; idx < expectedRows.size(); idx++) {
      final Map<String, Object> expectedRow = expectedRows.get(idx);
      final InputRow actualInputRow = actualReadRows.get(idx);
      for (String key : expectedRow.keySet()) {
        if (!schema.getColumnsFilter().apply(key)) {
          Assert.assertNull(actualInputRow.getRaw(key));
        } else {
          if (schema.getTimestampSpec().getTimestampColumn().equals(key)) {
            final long expectedMillis = (Long) expectedRow.get(key) * 1000;
            Assert.assertEquals(expectedMillis, actualInputRow.getTimestampFromEpoch());
            Assert.assertEquals(DateTimes.utc(expectedMillis), actualInputRow.getTimestamp());
          } else {
            Assert.assertEquals(expectedRow.get(key), actualInputRow.getRaw(key));
          }
        }
      }
    }
  }
}
