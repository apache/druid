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
import org.apache.druid.delta.filter.DeltaBinaryOperatorFilter;
import org.apache.druid.delta.filter.DeltaFilter;
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
import java.util.stream.Collectors;

public class DeltaInputSourceTest
{
  @Before
  public void setUp()
  {
    System.setProperty("user.timezone", "UTC");
  }

  @RunWith(Parameterized.class)
  public static class TableParameterTests
  {
    @Parameterized.Parameters
    public static Object[][] data()
    {
      return new Object[][]{
          {
              DeltaTableUnpartitioned.DELTA_TABLE_PATH,
              DeltaTableUnpartitioned.FULL_SCHEMA,
              DeltaTableUnpartitioned.EXPECTED_ROWS
          },
          {
              DeltaTableUnpartitioned.DELTA_TABLE_PATH,
              DeltaTableUnpartitioned.SCHEMA_1,
              DeltaTableUnpartitioned.EXPECTED_ROWS
          },
          {
              DeltaTableUnpartitioned.DELTA_TABLE_PATH,
              DeltaTableUnpartitioned.SCHEMA_2,
              DeltaTableUnpartitioned.EXPECTED_ROWS
          },
          {
              DeltaTablePartitioned.DELTA_TABLE_PATH,
              DeltaTablePartitioned.FULL_SCHEMA,
              DeltaTablePartitioned.EXPECTED_ROWS
          },
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
      final DeltaBinaryOperatorFilter.DeltaEqualsFilter equalsFilter = new DeltaBinaryOperatorFilter.DeltaEqualsFilter(
          "name",
          "Employee2"
      );
      final List<Map<String, Object>> equalsFilterExpectedResults = DeltaTablePartitioned.EXPECTED_ROWS
          .stream()
          .filter(k -> k.get("name").equals("Employee2"))
          .collect(Collectors.toList());

      final DeltaBinaryOperatorFilter.DeltaGreaterThanFilter gtFilter = new DeltaBinaryOperatorFilter.DeltaGreaterThanFilter(
          "name",
          "Employee3"
      );
      final List<Map<String, Object>> gtFilterExpectedResults = DeltaTablePartitioned.EXPECTED_ROWS
          .stream()
          .filter(k -> ((String) k.get("name")).compareTo("Employee3") > 0)
          .collect(Collectors.toList());

      final DeltaBinaryOperatorFilter.DeltaLessThanOrEqualsFilter lteFilter = new DeltaBinaryOperatorFilter.DeltaLessThanOrEqualsFilter(
          "name",
          "Employee4"
      );
      final List<Map<String, Object>> lteFilterExpectedResults = DeltaTablePartitioned.EXPECTED_ROWS
          .stream()
          .filter(k -> ((String) k.get("name")).compareTo("Employee4") <= 0)
          .collect(Collectors.toList());

      final DeltaAndFilter andFilter = new DeltaAndFilter(
          Arrays.asList(
              new DeltaBinaryOperatorFilter.DeltaEqualsFilter("name", "Employee1"),
              new DeltaBinaryOperatorFilter.DeltaEqualsFilter("name", "Employee4")
          )
      );
      final List<Map<String, Object>> andFilterExpectedResults = DeltaTablePartitioned.EXPECTED_ROWS
          .stream()
          .filter(k -> k.get("name").equals("Employee1") && k.get("name").equals("Employee4"))
          .collect(Collectors.toList());

      final DeltaOrFilter orFilter = new DeltaOrFilter(
          Arrays.asList(
              new DeltaBinaryOperatorFilter.DeltaEqualsFilter("name", "Employee5"),
              new DeltaBinaryOperatorFilter.DeltaEqualsFilter("name", "Employee1")
          )
      );
      final List<Map<String, Object>> orFilterExpectedResults = DeltaTablePartitioned.EXPECTED_ROWS
          .stream()
          .filter(k -> k.get("name").equals("Employee5") || k.get("name").equals("Employee1"))
          .collect(Collectors.toList());

      final DeltaNotFilter notFilter = new DeltaNotFilter(
          new DeltaBinaryOperatorFilter.DeltaEqualsFilter("name", "Employee3")
      );
      final List<Map<String, Object>> notFilterExpectedResults = DeltaTablePartitioned.EXPECTED_ROWS
          .stream()
          .filter(k -> !k.get("name").equals("Employee3"))
          .collect(Collectors.toList());

      final DeltaNotFilter notOfAndFilter = new DeltaNotFilter(andFilter);
      final List<Map<String, Object>> notofAndFilterExpectedResults = DeltaTablePartitioned.EXPECTED_ROWS
          .stream()
          .filter(k -> !(k.get("name").equals("Employee1") && k.get("name").equals("Employee4")))
          .collect(Collectors.toList());

      final DeltaNotFilter notOfOrFilter = new DeltaNotFilter(orFilter);
      final List<Map<String, Object>> notofOrFilterExpectedResults = DeltaTablePartitioned.EXPECTED_ROWS
          .stream()
          .filter(k -> !(k.get("name").equals("Employee5") || k.get("name").equals("Employee1")))
          .collect(Collectors.toList());

      return new Object[][]{
          {
              DeltaTablePartitioned.DELTA_TABLE_PATH,
              equalsFilter,
              DeltaTablePartitioned.FULL_SCHEMA,
              equalsFilterExpectedResults
          },
          {
              DeltaTablePartitioned.DELTA_TABLE_PATH,
              gtFilter,
              DeltaTablePartitioned.FULL_SCHEMA,
              gtFilterExpectedResults
          },
          {
              DeltaTablePartitioned.DELTA_TABLE_PATH,
              lteFilter,
              DeltaTablePartitioned.FULL_SCHEMA,
              lteFilterExpectedResults
          },
          {
              DeltaTablePartitioned.DELTA_TABLE_PATH,
              andFilter,
              DeltaTablePartitioned.FULL_SCHEMA,
              andFilterExpectedResults
          },
          {
              DeltaTablePartitioned.DELTA_TABLE_PATH,
              orFilter,
              DeltaTablePartitioned.FULL_SCHEMA,
              orFilterExpectedResults
          },
          {
              DeltaTablePartitioned.DELTA_TABLE_PATH,
              notFilter,
              DeltaTablePartitioned.FULL_SCHEMA,
              notFilterExpectedResults
          },
          {
              DeltaTablePartitioned.DELTA_TABLE_PATH,
              notOfAndFilter,
              DeltaTablePartitioned.FULL_SCHEMA,
              notofAndFilterExpectedResults
          },
          {
              DeltaTablePartitioned.DELTA_TABLE_PATH,
              notOfOrFilter,
              DeltaTablePartitioned.FULL_SCHEMA,
              notofOrFilterExpectedResults
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

    @Test
    public void testReadDeltaTable() throws IOException
    {
      final DeltaInputSource deltaInputSource = new DeltaInputSource(deltaTablePath, null, filter);
      final InputSourceReader inputSourceReader = deltaInputSource.reader(schema, null, null);
      final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
      validateRows(expectedRows, actualReadRows, schema);
    }
  }

  public static class InvalidTableTests
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
