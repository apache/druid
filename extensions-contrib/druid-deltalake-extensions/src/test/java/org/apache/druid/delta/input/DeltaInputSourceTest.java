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
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
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

  @Test
  public void testSampleDeltaTable() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(DeltaTestUtils.FULL_SCHEMA, null, null);

    List<InputRowListPlusRawValues> actualSampledRows = sampleAllRows(inputSourceReader);
    Assert.assertEquals(DeltaTestUtils.EXPECTED_ROWS.size(), actualSampledRows.size());

    for (int idx = 0; idx < DeltaTestUtils.EXPECTED_ROWS.size(); idx++) {
      Map<String, Object> expectedRow = DeltaTestUtils.EXPECTED_ROWS.get(idx);
      InputRowListPlusRawValues actualSampledRow = actualSampledRows.get(idx);
      Assert.assertNull(actualSampledRow.getParseException());

      Map<String, Object> actualSampledRawVals = actualSampledRow.getRawValues();
      Assert.assertNotNull(actualSampledRawVals);
      Assert.assertNotNull(actualSampledRow.getRawValuesList());
      Assert.assertEquals(1, actualSampledRow.getRawValuesList().size());

      for (String key : expectedRow.keySet()) {
        if (DeltaTestUtils.FULL_SCHEMA.getTimestampSpec().getTimestampColumn().equals(key)) {
          final long expectedMillis = (Long) expectedRow.get(key);
          Assert.assertEquals(expectedMillis, actualSampledRawVals.get(key));
        } else {
          Assert.assertEquals(expectedRow.get(key), actualSampledRawVals.get(key));
        }
      }
    }
  }

  @Test
  public void testReadAllDeltaTable() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.FULL_SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.FULL_SCHEMA);
  }

  @Test
  public void testReadAllDeltaTableSubSchema1() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.SCHEMA_1,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.SCHEMA_1);
  }

  @Test
  public void testReadAllDeltaTableWithSubSchema2() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.SCHEMA_2,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.SCHEMA_2);
  }

  @Test
  public void testDeltaLakeWithCreateSplits()
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null);
    final List<InputSplit<DeltaSplit>> splits = deltaInputSource.createSplits(null, null)
                                                                .collect(Collectors.toList());
    Assert.assertEquals(DeltaTestUtils.SPLIT_TO_EXPECTED_ROWS.size(), splits.size());

    for (InputSplit<DeltaSplit> split : splits) {
      final DeltaSplit deltaSplit = split.get();
      final DeltaInputSource deltaInputSourceWithSplit = new DeltaInputSource(
          DeltaTestUtils.DELTA_TABLE_PATH,
          deltaSplit
      );
      List<InputSplit<DeltaSplit>> splitsResult = deltaInputSourceWithSplit.createSplits(null, null)
                                                                           .collect(Collectors.toList());
      Assert.assertEquals(1, splitsResult.size());
      Assert.assertEquals(deltaSplit, splitsResult.get(0).get());
    }
  }

  @Test
  public void testDeltaLakeWithReadSplits() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null);
    final List<InputSplit<DeltaSplit>> splits = deltaInputSource.createSplits(null, null)
                                                                .collect(Collectors.toList());
    Assert.assertEquals(DeltaTestUtils.SPLIT_TO_EXPECTED_ROWS.size(), splits.size());

    for (int idx = 0; idx < splits.size(); idx++) {
      final InputSplit<DeltaSplit> split = splits.get(idx);
      final DeltaSplit deltaSplit = split.get();
      final DeltaInputSource deltaInputSourceWithSplit = new DeltaInputSource(
          DeltaTestUtils.DELTA_TABLE_PATH,
          deltaSplit
      );
      final InputSourceReader inputSourceReader = deltaInputSourceWithSplit.reader(
          DeltaTestUtils.FULL_SCHEMA,
          null,
          null
      );
      final List<InputRow> actualRowsInSplit = readAllRows(inputSourceReader);
      final List<Map<String, Object>> expectedRowsInSplit = DeltaTestUtils.SPLIT_TO_EXPECTED_ROWS.get(idx);
      validateRows(expectedRowsInSplit, actualRowsInSplit, DeltaTestUtils.FULL_SCHEMA);
    }
  }

  @Test
  public void testNullTable()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new DeltaInputSource(null, null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "tablePath cannot be null."
        )
    );
  }

  @Test
  public void testSplitNonExistentTable()
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource("non-existent-table", null);

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
    final DeltaInputSource deltaInputSource = new DeltaInputSource("non-existent-table", null);

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

  private List<InputRowListPlusRawValues> sampleAllRows(InputSourceReader reader) throws IOException
  {
    List<InputRowListPlusRawValues> rows = new ArrayList<>();
    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }

  private List<InputRow> readAllRows(InputSourceReader reader) throws IOException
  {
    final List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }

  private void validateRows(
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
