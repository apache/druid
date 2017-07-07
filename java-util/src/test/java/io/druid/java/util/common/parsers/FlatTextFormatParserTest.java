/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.parsers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.parsers.AbstractFlatTextFormatParser.FlatTextFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class FlatTextFormatParserTest
{
  @Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{FlatTextFormat.CSV},
        new Object[]{FlatTextFormat.DELIMITED}
    );
  }

  private static final FlatTextFormatParserFactory parserFactory = new FlatTextFormatParserFactory();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final FlatTextFormat format;

  public FlatTextFormatParserTest(FlatTextFormat format)
  {
    this.format = format;
  }

  @Test
  public void testValidHeader()
  {
    final String header = concat(format, "time", "value1", "value2");
    final Parser<String, Object> parser = parserFactory.get(format, header);
    Assert.assertEquals(ImmutableList.of("time", "value1", "value2"), parser.getFieldNames());
  }

  @Test
  public void testDuplicatedColumnName()
  {
    final String header = concat(format, "time", "value1", "value2", "value2");

    expectedException.expect(ParseException.class);
    expectedException.expectMessage(StringUtils.format("Unable to parse header [%s]", header));

    parserFactory.get(format, header);
  }

  @Test
  public void testWithHeader()
  {
    final String header = concat(format, "time", "value1", "value2");
    final Parser<String, Object> parser = parserFactory.get(format, header);
    final String body = concat(format, "hello", "world", "foo");
    final Map<String, Object> jsonMap = parser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap
    );
  }

  @Test
  public void testWithoutHeader()
  {
    final Parser<String, Object> parser = parserFactory.get(format);
    final String body = concat(format, "hello", "world", "foo");
    final Map<String, Object> jsonMap = parser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("column_1", "hello", "column_2", "world", "column_3", "foo"),
        jsonMap
    );
  }

  @Test
  public void testWithSkipHeaderRows()
  {
    final int skipHeaderRows = 2;
    final Parser<String, Object> parser = parserFactory.get(format, false, skipHeaderRows);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "header", "line", "1"),
        concat(format, "header", "line", "2"),
        concat(format, "hello", "world", "foo")
    };
    int index;
    for (index = 0; index < skipHeaderRows; index++) {
      Assert.assertNull(parser.parse(body[index]));
    }
    final Map<String, Object> jsonMap = parser.parse(body[index]);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("column_1", "hello", "column_2", "world", "column_3", "foo"),
        jsonMap
    );
  }

  @Test
  public void testWithHeaderRow()
  {
    final Parser<String, Object> parser = parserFactory.get(format, true, 0);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "time", "value1", "value2"),
        concat(format, "hello", "world", "foo")
    };
    Assert.assertNull(parser.parse(body[0]));
    final Map<String, Object> jsonMap = parser.parse(body[1]);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap
    );
  }

  @Test
  public void testWithHeaderRowOfEmptyColumns()
  {
    final Parser<String, Object> parser = parserFactory.get(format, true, 0);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "time", "", "value2", ""),
        concat(format, "hello", "world", "foo", "bar")
    };
    Assert.assertNull(parser.parse(body[0]));
    final Map<String, Object> jsonMap = parser.parse(body[1]);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("time", "hello", "column_2", "world", "value2", "foo", "column_4", "bar"),
        jsonMap
    );
  }

  @Test
  public void testWithDifferentHeaderRows()
  {
    final Parser<String, Object> parser = parserFactory.get(format, true, 0);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "time", "value1", "value2"),
        concat(format, "hello", "world", "foo")
    };
    Assert.assertNull(parser.parse(body[0]));
    Map<String, Object> jsonMap = parser.parse(body[1]);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap
    );

    parser.startFileFromBeginning();
    final String[] body2 = new String[]{
        concat(format, "time", "value1", "value2", "value3"),
        concat(format, "hello", "world", "foo", "bar")
    };
    Assert.assertNull(parser.parse(body2[0]));
    jsonMap = parser.parse(body2[1]);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo", "value3", "bar"),
        jsonMap
    );
  }

  @Test
  public void testWithoutStartFileFromBeginning()
  {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage(
        "hasHeaderRow or maxSkipHeaderRows is not supported. Please check the indexTask supports these options."
    );

    final int skipHeaderRows = 2;
    final Parser<String, Object> parser = parserFactory.get(format, false, skipHeaderRows);
    final String[] body = new String[]{
        concat(format, "header", "line", "1"),
        concat(format, "header", "line", "2"),
        concat(format, "hello", "world", "foo")
    };
    parser.parse(body[0]);
  }

  private static class FlatTextFormatParserFactory
  {
    public Parser<String, Object> get(FlatTextFormat format)
    {
      return get(format, false, 0);
    }

    public Parser<String, Object> get(FlatTextFormat format, boolean hasHeaderRow, int maxSkipHeaderRows)
    {
      switch (format) {
        case CSV:
          return new CSVParser(null, hasHeaderRow, maxSkipHeaderRows);
        case DELIMITED:
          return new DelimitedParser("\t", null, hasHeaderRow, maxSkipHeaderRows);
        default:
          throw new IAE("Unknown format[%s]", format);
      }
    }

    public Parser<String, Object> get(FlatTextFormat format, String header)
    {
      switch (format) {
        case CSV:
          return new CSVParser(null, header);
        case DELIMITED:
          return new DelimitedParser("\t", null, header);
        default:
          throw new IAE("Unknown format[%s]", format);
      }
    }
  }

  private static String concat(FlatTextFormat format, String ... values)
  {
    return Arrays.stream(values).collect(Collectors.joining(format.getDefaultDelimiter()));
  }
}
