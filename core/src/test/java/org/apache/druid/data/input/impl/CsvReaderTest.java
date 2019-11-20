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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class CsvReaderTest
{
  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      new TimestampSpec("ts", "auto", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "name"))),
      Collections.emptyList()
  );

  @Test
  public void testWithoutHeaders() throws IOException
  {
    final ByteEntity source = writeData(
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
    final ByteEntity source = writeData(
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
    final ByteEntity source = writeData(
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
    final ByteEntity source = writeData(
        ImmutableList.of(
            "this,is,a,row,to,skip",
            "ts,name,score",
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
    final ByteEntity source = writeData(
        ImmutableList.of(
            "ts,name,score",
            "2019-01-01T00:00:10Z,name_1,5|1",
            "2019-01-01T00:00:20Z,name_2,10|2",
            "2019-01-01T00:00:30Z,name_3,15|3"
        )
    );
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of(), "|", true, 0);
    final InputEntityReader reader = format.createReader(INPUT_ROW_SCHEMA, source, null);
    int numResults = 0;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
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

  @Test
  public void testQuotes() throws IOException
  {
    final ByteEntity source = writeData(
        ImmutableList.of(
            "3,\"Lets do some \"\"normal\"\" quotes\",2018-05-05T10:00:00Z",
            "34,\"Lets do some \"\"normal\"\", quotes with comma\",2018-05-06T10:00:00Z",
            "343,\"Lets try \\\"\"it\\\"\" with slash quotes\",2018-05-07T10:00:00Z",
            "545,\"Lets try \\\"\"it\\\"\", with slash quotes and comma\",2018-05-08T10:00:00Z",
            "65,Here I write \\n slash n,2018-05-09T10:00:00Z"
        )
    );
    final List<InputRow> expectedResults = ImmutableList.of(
        new MapBasedInputRow(
            DateTimes.of("2018-05-05T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableMap.of(
                "Value",
                "3",
                "Comment",
                "Lets do some \"normal\" quotes",
                "Timestamp",
                "2018-05-05T10:00:00Z"
            )
        ),
        new MapBasedInputRow(
            DateTimes.of("2018-05-06T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableMap.of(
                "Value",
                "34",
                "Comment",
                "Lets do some \"normal\", quotes with comma",
                "Timestamp",
                "2018-05-06T10:00:00Z"
            )
        ),
        new MapBasedInputRow(
            DateTimes.of("2018-05-07T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableMap.of(
                "Value",
                "343",
                "Comment",
                "Lets try \\\"it\\\" with slash quotes",
                "Timestamp",
                "2018-05-07T10:00:00Z"
            )
        ),
        new MapBasedInputRow(
            DateTimes.of("2018-05-08T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableMap.of(
                "Value",
                "545",
                "Comment",
                "Lets try \\\"it\\\", with slash quotes and comma",
                "Timestamp",
                "2018-05-08T10:00:00Z"
            )
        ),
        new MapBasedInputRow(
            DateTimes.of("2018-05-09T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableMap.of("Value", "65", "Comment", "Here I write \\n slash n", "Timestamp", "2018-05-09T10:00:00Z")
        )
    );
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of("Value", "Comment", "Timestamp"), null, false, 0);
    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("Timestamp", "auto", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("Timestamp"))),
            Collections.emptyList()
        ),
        source,
        null
    );

    try (CloseableIterator<InputRow> iterator = reader.read()) {
      final Iterator<InputRow> expectedRowIterator = expectedResults.iterator();
      while (iterator.hasNext()) {
        Assert.assertTrue(expectedRowIterator.hasNext());
        Assert.assertEquals(expectedRowIterator.next(), iterator.next());
      }
    }
  }

  @Test
  public void testRussianTextMess() throws IOException
  {
    final ByteEntity source = writeData(
        ImmutableList.of(
            "2019-01-01T00:00:10Z,name_1,\"Как говорится: \\\"\"всё течет, всё изменяется\\\"\". Украина как всегда обвиняет Россию в собственных проблемах. #ПровокацияКиева\""
        )
    );
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of("ts", "name", "Comment"), null, false, 0);
    final InputEntityReader reader = format.createReader(INPUT_ROW_SCHEMA, source, null);
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assert.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      Assert.assertEquals(DateTimes.of("2019-01-01T00:00:10Z"), row.getTimestamp());
      Assert.assertEquals("name_1", Iterables.getOnlyElement(row.getDimension("name")));
      Assert.assertEquals(
          "Как говорится: \\\"всё течет, всё изменяется\\\". Украина как всегда обвиняет Россию в собственных проблемах. #ПровокацияКиева",
          Iterables.getOnlyElement(row.getDimension("Comment"))
      );
      Assert.assertFalse(iterator.hasNext());
    }
  }

  private ByteEntity writeData(List<String> lines) throws IOException
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
    return new ByteEntity(outputStream.toByteArray());
  }

  private void assertResult(ByteEntity source, CsvInputFormat format) throws IOException
  {
    final InputEntityReader reader = format.createReader(INPUT_ROW_SCHEMA, source, null);
    int numResults = 0;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
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
