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

package org.apache.druid.data.input.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.SplitReader;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CsvReaderTest
{
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "auto", null);
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "name"))
  );

  @Test
  public void testWithoutHeaders() throws IOException
  {
    final ByteSource source = writeData(
        ImmutableList.of(
            "2019-01-01T00:00:10Z,name_1,5",
            "2019-01-01T00:00:20Z,name_2,10",
            "2019-01-01T00:00:30Z,name_3,15"
        )
    );
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of("ts", "name", "score"), null, false, 0);
    assertResult(source, format);
  }

  @Test
  public void testFindColumn() throws IOException
  {
    final ByteSource source = writeData(
        ImmutableList.of(
            "ts,name,score",
            "2019-01-01T00:00:10Z,name_1,5",
            "2019-01-01T00:00:20Z,name_2,10",
            "2019-01-01T00:00:30Z,name_3,15"
        )
    );
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of(), null, true, 0);
    assertResult(source, format);
  }

  @Test
  public void testSkipHeaders() throws IOException
  {
    final ByteSource source = writeData(
        ImmutableList.of(
            "this,is,a,row,to,skip",
            "2019-01-01T00:00:10Z,name_1,5",
            "2019-01-01T00:00:20Z,name_2,10",
            "2019-01-01T00:00:30Z,name_3,15"
        )
    );
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of("ts", "name", "score"), null, false, 1);
    assertResult(source, format);
  }

  @Test
  public void testFindColumnAndSkipHeaders() throws IOException
  {
    final ByteSource source = writeData(
        ImmutableList.of(
            "ts,name,score",
            "this,is,a,row,to,skip",
            "2019-01-01T00:00:10Z,name_1,5",
            "2019-01-01T00:00:20Z,name_2,10",
            "2019-01-01T00:00:30Z,name_3,15"
        )
    );
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of(), null, true, 1);
    assertResult(source, format);
  }

  @Test
  public void testMultiValues() throws IOException
  {
    final ByteSource source = writeData(
        ImmutableList.of(
            "ts,name,score",
            "2019-01-01T00:00:10Z,name_1,5|1",
            "2019-01-01T00:00:20Z,name_2,10|2",
            "2019-01-01T00:00:30Z,name_3,15|3"
        )
    );
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of(), "|", true, 0);
    final SplitReader reader = format.createReader(TIMESTAMP_SPEC, DIMENSIONS_SPEC);
    int numResults = 0;
    try (CloseableIterator<InputRow> iterator = reader.read(source, null)) {
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        Assert.assertEquals(
            DateTimes.of(StringUtils.format("2019-01-01T00:00:%02dZ", (numResults + 1) * 10)),
            row.getTimestamp()
        );
        Assert.assertEquals(
            StringUtils.format("name_%d", numResults + 1),
            Iterables.getOnlyElement(row.getDimension("name"))
        );
        Assert.assertEquals(
            ImmutableList.of(Integer.toString((numResults + 1) * 5), Integer.toString(numResults + 1)),
            row.getDimension("score")
        );
        numResults++;
      }
      Assert.assertEquals(3, numResults);
    }
  }

  private ByteSource writeData(List<String> lines) throws IOException
  {
    final List<byte[]> byteLines = lines.stream()
                                        .map(line -> StringUtils.toUtf8(line + "\n"))
                                        .collect(Collectors.toList());
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(
        byteLines.stream().mapToInt(bytes -> bytes.length).sum()
    );
    for (byte[] bytes : byteLines) {
      outputStream.write(bytes);
    }
    return new ByteSource(outputStream.toByteArray());
  }

  private void assertResult(ByteSource source, CsvInputFormat format) throws IOException
  {
    final SplitReader reader = format.createReader(TIMESTAMP_SPEC, DIMENSIONS_SPEC);
    int numResults = 0;
    try (CloseableIterator<InputRow> iterator = reader.read(source, null)) {
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        Assert.assertEquals(
            DateTimes.of(StringUtils.format("2019-01-01T00:00:%02dZ", (numResults + 1) * 10)),
            row.getTimestamp()
        );
        Assert.assertEquals(
            StringUtils.format("name_%d", numResults + 1),
            Iterables.getOnlyElement(row.getDimension("name"))
        );
        Assert.assertEquals(
            Integer.toString((numResults + 1) * 5),
            Iterables.getOnlyElement(row.getDimension("score"))
        );
        numResults++;
      }
      Assert.assertEquals(3, numResults);
    }
  }
}
