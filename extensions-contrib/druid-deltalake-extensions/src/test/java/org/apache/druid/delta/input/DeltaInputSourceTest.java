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
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
      Assert.assertEquals(
          expectedRow,
          actualSampledRow.getRawValues()
      );
      Assert.assertNotNull(actualSampledRow.getRawValuesList());
      Assert.assertEquals(expectedRow, actualSampledRow.getRawValuesList().get(0));
    }
  }

  @Test
  public void testReadAllDeltaTable() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtil.DELTA_TABLE_PATH, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(DeltaTestUtil.SCHEMA, null, null);

    List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    Assert.assertEquals(DeltaTestUtil.EXPECTED_ROWS.size(), actualReadRows.size());

    for (int idx = 0; idx < DeltaTestUtil.EXPECTED_ROWS.size(); idx++) {
      Map<String, Object> expectedRow = DeltaTestUtil.EXPECTED_ROWS.get(idx);
      InputRow actualInputRow = actualReadRows.get(idx);
      for (String key : expectedRow.keySet()) {
        if (DeltaTestUtil.SCHEMA.getTimestampSpec().getTimestampColumn().equals(key)) {
          final long expectedMillis = ((Long) expectedRow.get(key) / 1_000_000) * 1000;
          Assert.assertEquals(expectedMillis, actualInputRow.getTimestampFromEpoch());
        } else {
          Assert.assertEquals(expectedRow.get(key), actualInputRow.getDimension(key).get(0));
        }
      }
    }
  }

  @Test
  public void testReadDeltaTableWithNoSplits()
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtil.DELTA_TABLE_PATH, null);
    final Stream<InputSplit<DeltaSplit>> splits = deltaInputSource.createSplits(null, null);
    Assert.assertNotNull(splits);
    Assert.assertEquals(1, splits.count());
  }

  @Test
  public void testReadDeltaLakeWithSplits()
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtil.DELTA_TABLE_PATH, null);
    final List<InputSplit<DeltaSplit>> splits1 = deltaInputSource.createSplits(null, null)
                                                                   .collect(Collectors.toList());
    Assert.assertEquals(1, splits1.size());

    final DeltaInputSource deltaInputSourceWithSplit = new DeltaInputSource(
        DeltaTestUtil.DELTA_TABLE_PATH,
        splits1.get(0).get()
    );
    final List<InputSplit<DeltaSplit>> splits2 = deltaInputSourceWithSplit.createSplits(null, null)
                                                                            .collect(Collectors.toList());
    Assert.assertEquals(1, splits2.size());
    Assert.assertEquals(splits1.get(0).get(), splits2.get(0).get());
  }

  @Test
  public void testNullTablePath()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new DeltaInputSource(null, null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "tablePath cannot be null"
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
}
