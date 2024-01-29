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
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeltaInputSourceTest
{
  @Test
  public void testSampleDeltaTable() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtil.DELTA_TABLE_PATH, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(DeltaTestUtil.SCHEMA, null, null);

    List<InputRowListPlusRawValues> actualSampledRows = sampleAllRows(inputSourceReader);
    Assert.assertEquals(DeltaTestUtil.EXPECTED_ROWS.size(), actualSampledRows.size());

    for (int idx = 0; idx < DeltaTestUtil.EXPECTED_ROWS.size(); idx++) {
      Map<String, Object> expectedRow = DeltaTestUtil.EXPECTED_ROWS.get(idx);
      InputRowListPlusRawValues actualSampledRow = actualSampledRows.get(idx);
      Assert.assertNull(actualSampledRow.getParseException());

      Map<String, Object> actualSampledRawVals = actualSampledRow.getRawValues();
      Assert.assertNotNull(actualSampledRawVals);
      Assert.assertNotNull(actualSampledRow.getRawValuesList());
      Assert.assertEquals(1, actualSampledRow.getRawValuesList().size());

      for (String key : expectedRow.keySet()) {
        if (DeltaTestUtil.SCHEMA.getTimestampSpec().getTimestampColumn().equals(key)) {
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
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtil.DELTA_TABLE_PATH, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtil.SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    Assert.assertEquals(DeltaTestUtil.EXPECTED_ROWS.size(), actualReadRows.size());

    validateRows(DeltaTestUtil.EXPECTED_ROWS, actualReadRows);
  }

  @Test
  public void testDeltaLakeWithCreateSplits()
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtil.DELTA_TABLE_PATH, null);
    final List<InputSplit<DeltaSplit>> splits = deltaInputSource.createSplits(null, null)
                                                                .collect(Collectors.toList());
    Assert.assertEquals(DeltaTestUtil.SPLIT_TO_EXPECTED_ROWS.size(), splits.size());

    for (InputSplit<DeltaSplit> split : splits) {
      final DeltaSplit deltaSplit = split.get();
      final DeltaInputSource deltaInputSourceWithSplit = new DeltaInputSource(
          DeltaTestUtil.DELTA_TABLE_PATH,
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
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtil.DELTA_TABLE_PATH, null);
    final List<InputSplit<DeltaSplit>> splits = deltaInputSource.createSplits(null, null)
                                                                .collect(Collectors.toList());
    Assert.assertEquals(DeltaTestUtil.SPLIT_TO_EXPECTED_ROWS.size(), splits.size());

    for (int i = 0; i < splits.size(); i++) {
      final InputSplit<DeltaSplit> split = splits.get(i);
      final DeltaSplit deltaSplit = split.get();
      final DeltaInputSource deltaInputSourceWithSplit = new DeltaInputSource(
          DeltaTestUtil.DELTA_TABLE_PATH,
          deltaSplit
      );
      final InputSourceReader inputSourceReader = deltaInputSourceWithSplit.reader(
          DeltaTestUtil.SCHEMA,
          null,
          null
      );
      final List<InputRow> actualRowsInSplit = readAllRows(inputSourceReader);
      final List<Map<String, Object>> expectedRowsInSplit = DeltaTestUtil.SPLIT_TO_EXPECTED_ROWS.get(i);
      Assert.assertEquals(expectedRowsInSplit.size(), actualRowsInSplit.size());

      validateRows(expectedRowsInSplit, actualRowsInSplit);
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

  private void validateRows(final List<Map<String, Object>> expectedRows, final List<InputRow> actualReadRows)
  {
    for (int idx = 0; idx < expectedRows.size(); idx++) {
      final Map<String, Object> expectedRow = expectedRows.get(idx);
      final InputRow actualInputRow = actualReadRows.get(idx);
      for (String key : expectedRow.keySet()) {
        if (DeltaTestUtil.SCHEMA.getTimestampSpec().getTimestampColumn().equals(key)) {
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
