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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class CsvReaderTest
{
  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      new TimestampSpec("ts", "auto", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "name"))),
      ColumnsFilter.all()
  );

  @BeforeClass
  public static void setup()
  {
    NullHandling.initializeForTests();
  }

  @Test
  public void testWithoutHeaders() throws IOException
  {
    final ByteEntity source = writeData(
        ImmutableList.of(
            "2019-01-01T00:00:10Z,name_1,5,0.0",
            "2019-01-01T00:00:20Z,name_2,10,1.0",
            "2019-01-01T00:00:30Z,name_3,15,2.0"
        )
    );
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of("ts", "name", "score"), null, null, false, 0, null);
    assertResult(source, format);
  }

  @Test
  public void testParseNumbers() throws IOException
  {
    final ByteEntity source = writeData(
        ImmutableList.of(
            "2019-01-01T00:00:10Z,name_1,5,0.0",
            "2019-01-01T00:00:20Z,name_2,10,1.0",
            "2019-01-01T00:00:30Z,name_3,15,2.0"
        )
    );
    final CsvInputFormat format = new CsvInputFormat(
        ImmutableList.of("ts", "name", "score", "rating_dbl"),
        null,
        null,
        false,
        0,
        true
    );

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
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of(), null, null, true, 0, null);
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
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of("ts", "name", "score"), null, null, false, 1, null);
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
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of(), null, null, true, 1, null);
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
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of(), "|", null, true, 0, null);
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
    final RowSignature signature =
        RowSignature.builder()
                    .add("Value", null)
                    .add("Comment", null)
                    .add("Timestamp", null)
                    .build();

    final List<InputRow> expectedResults = ImmutableList.of(
        new ListBasedInputRow(
            signature,
            DateTimes.of("2018-05-05T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableList.of(
                "3",
                "Lets do some \"normal\" quotes",
                "2018-05-05T10:00:00Z"
            )
        ),
        new ListBasedInputRow(
            signature,
            DateTimes.of("2018-05-06T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableList.of(
                "34",
                "Lets do some \"normal\", quotes with comma",
                "2018-05-06T10:00:00Z"
            )
        ),
        new ListBasedInputRow(
            signature,
            DateTimes.of("2018-05-07T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableList.of(
                "343",
                "Lets try \\\"it\\\" with slash quotes",
                "2018-05-07T10:00:00Z"
            )
        ),
        new ListBasedInputRow(
            signature,
            DateTimes.of("2018-05-08T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableList.of(
                "545",
                "Lets try \\\"it\\\", with slash quotes and comma",
                "2018-05-08T10:00:00Z"
            )
        ),
        new ListBasedInputRow(
            signature,
            DateTimes.of("2018-05-09T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableList.of(
                "65",
                "Here I write \\n slash n",
                "2018-05-09T10:00:00Z"
            )
        )
    );
    final CsvInputFormat format = new CsvInputFormat(
        ImmutableList.of("Value", "Comment", "Timestamp"),
        null,
        null,
        false,
        0,
        null
    );
    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("Timestamp", "auto", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("Timestamp"))),
            ColumnsFilter.all()
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
  public void testDataWithParseNumbersEnabled() throws IOException
  {
    final ByteEntity source = writeData(
        ImmutableList.of(
            "3,1.0,2,1|2|3,false,2018-05-05T10:00:00Z",
            "34,-2.0,foo,1,true,2018-05-06T10:00:00Z",
            "343,3.0,bar,2|3|4,false,2018-05-07T10:00:00Z",
            "545,-4.0,7,2,true,2018-05-08T10:00:00Z",
            "65,5.0,baz,3|4,false,2018-05-09T10:00:00Z"
        )
    );
    final RowSignature signature =
        RowSignature.builder()
                    .add("Value1", null)
                    .add("Value2", null)
                    .add("Value3", null)
                    .add("Value4", null)
                    .add("Value5", null)
                    .add("Timestamp", null)
                    .build();

    final List<InputRow> expectedResults = ImmutableList.of(
        new ListBasedInputRow(
            signature,
            DateTimes.of("2018-05-05T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableList.of(
                3L,
                1.0,
                2L,
                ImmutableList.of(1L, 2L, 3L),
                "false",
                "2018-05-05T10:00:00Z"
            )
        ),
        new ListBasedInputRow(
            signature,
            DateTimes.of("2018-05-06T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableList.of(
                34L,
                -2.0,
                "foo",
                1L,
                "true",
                "2018-05-06T10:00:00Z"
            )
        ),
        new ListBasedInputRow(
            signature,
            DateTimes.of("2018-05-07T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableList.of(
                343L,
                3.0,
                "bar",
                ImmutableList.of(2L, 3L, 4L),
                "false",
                "2018-05-07T10:00:00Z"
            )
        ),
        new ListBasedInputRow(
            signature,
            DateTimes.of("2018-05-08T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableList.of(
                545L,
                -4.0,
                7L,
                2L,
                "true",
                "2018-05-08T10:00:00Z"
            )
        ),
        new ListBasedInputRow(
            signature,
            DateTimes.of("2018-05-09T10:00:00Z"),
            ImmutableList.of("Timestamp"),
            ImmutableList.of(
                65L,
                5.0,
                "baz",
                ImmutableList.of(3L, 4L),
                "false",
                "2018-05-09T10:00:00Z"
            )
        )
    );
    final CsvInputFormat format = new CsvInputFormat(
        ImmutableList.of("Value1", "Value2", "Value3", "Value4", "Value5", "Timestamp"),
        "|",
        null,
        false,
        0,
        true
    );
    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("Timestamp", "auto", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("Timestamp"))),
            ColumnsFilter.all()
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
    final CsvInputFormat format = new CsvInputFormat(ImmutableList.of("ts", "name", "Comment"), null, null, false, 0, null);
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
        if (format.shouldTryParseNumbers()) {
          Assert.assertEquals(((numResults + 1) * 5L), row.getRaw("score"));
          if (format.getColumns().contains("rating_dbl")) {
            Assert.assertEquals(numResults * 1.0, row.getRaw("rating_dbl"));
          }
        } else {
          Assert.assertEquals(Integer.toString((numResults + 1) * 5), row.getRaw("score"));
          if (format.getColumns().contains("rating_dbl")) {
            Assert.assertEquals(Double.toString(numResults * 1.0), row.getRaw("rating_dbl"));
          }
        }
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
