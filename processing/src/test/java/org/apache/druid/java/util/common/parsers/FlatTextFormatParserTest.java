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

package org.apache.druid.java.util.common.parsers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.AbstractFlatTextFormatParser.FlatTextFormat;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ParameterizedClass
@MethodSource("constructorFeeder")
public class FlatTextFormatParserTest extends InitializedNullHandlingTest
{
  public static Stream<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{FlatTextFormat.CSV},
        new Object[]{FlatTextFormat.DELIMITED}
    ).stream();
  }

  private static final FlatTextFormatParserFactory PARSER_FACTORY = new FlatTextFormatParserFactory();

  @Parameter(0)
  public FlatTextFormat format;

  @Test
  public void testValidHeader()
  {
    final String header = concat(format, "time", "value1", "value2");
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, header);
    Assertions.assertEquals(ImmutableList.of("time", "value1", "value2"), parser.getFieldNames());
  }

  @Test
  public void testDuplicatedColumnName()
  {
    final String header = concat(format, "time", "value1", "value2", "value2");

    Throwable t = Assertions.assertThrows(
        ParseException.class,
        () -> PARSER_FACTORY.get(format, header)
    );
    Assertions.assertEquals(StringUtils.format("Unable to parse header [%s]", header), t.getMessage());
  }

  @Test
  public void testWithHeader()
  {
    final String header = concat(format, "time", "value1", "value2");
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, header);
    final String body = concat(format, "hello", "world", "foo");
    final Map<String, Object> jsonMap = parser.parseToMap(body);
    Assertions.assertEquals(
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap,
        "jsonMap"
    );
  }

  @Test
  public void testWithoutHeader()
  {
    final Parser<String, Object> parser = PARSER_FACTORY.get(format);
    final String body = concat(format, "hello", "world", "foo");
    final Map<String, Object> jsonMap = parser.parseToMap(body);
    Assertions.assertEquals(
        ImmutableMap.of("column_1", "hello", "column_2", "world", "column_3", "foo"),
        jsonMap,
        "jsonMap"
    );
  }

  @Test
  public void testWithSkipHeaderRows()
  {
    final int skipHeaderRows = 2;
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, false, skipHeaderRows, false);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "header", "line", "1"),
        concat(format, "header", "line", "2"),
        concat(format, "hello", "world", "foo")
    };
    int index;
    for (index = 0; index < skipHeaderRows; index++) {
      Assertions.assertNull(parser.parseToMap(body[index]));
    }
    final Map<String, Object> jsonMap = parser.parseToMap(body[index]);
    Assertions.assertEquals(
        ImmutableMap.of("column_1", "hello", "column_2", "world", "column_3", "foo"),
        jsonMap,
        "jsonMap"
    );
  }

  @Test
  public void testWithHeaderRow()
  {
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, true, 0, false);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "time", "value1", "value2"),
        concat(format, "hello", "world", "foo")
    };
    Assertions.assertNull(parser.parseToMap(body[0]));
    final Map<String, Object> jsonMap = parser.parseToMap(body[1]);
    Assertions.assertEquals(
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap,
        "jsonMap"
    );
  }

  @Test
  public void testWithHeaderRowOfEmptyColumns()
  {
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, true, 0, false);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "time", "", "value2", ""),
        concat(format, "hello", "world", "foo", "bar")
    };
    Assertions.assertNull(parser.parseToMap(body[0]));
    final Map<String, Object> jsonMap = parser.parseToMap(body[1]);
    Assertions.assertEquals(
        ImmutableMap.of("time", "hello", "column_2", "world", "value2", "foo", "column_4", "bar"),
        jsonMap,
        "jsonMap"
    );
  }

  @Test
  public void testWithDifferentHeaderRows()
  {
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, true, 0, false);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "time", "value1", "value2"),
        concat(format, "hello", "world", "foo")
    };
    Assertions.assertNull(parser.parseToMap(body[0]));
    Map<String, Object> jsonMap = parser.parseToMap(body[1]);
    Assertions.assertEquals(
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap,
        "jsonMap"
    );

    parser.startFileFromBeginning();
    final String[] body2 = new String[]{
        concat(format, "time", "value1", "value2", "value3"),
        concat(format, "hello", "world", "foo", "bar")
    };
    Assertions.assertNull(parser.parseToMap(body2[0]));
    jsonMap = parser.parseToMap(body2[1]);
    Assertions.assertEquals(
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo", "value3", "bar"),
        jsonMap,
        "jsonMap"
    );
  }

  @Test
  public void testWithoutStartFileFromBeginning()
  {
    final int skipHeaderRows = 2;
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, false, skipHeaderRows, false);
    final String[] body = new String[]{
        concat(format, "header", "line", "1"),
        concat(format, "header", "line", "2"),
        concat(format, "hello", "world", "foo")
    };
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> parser.parseToMap(body[0])
    );
  }

  @Test
  public void testWithNullValues()
  {
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, true, 0, false);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "time", "value1", "value2"),
        concat(format, "hello", "world", "")
    };
    Assertions.assertNull(parser.parseToMap(body[0]));
    final Map<String, Object> jsonMap = parser.parseToMap(body[1]);
    Assertions.assertNull(jsonMap.get("value2"));
  }

  private static class FlatTextFormatParserFactory
  {
    public Parser<String, Object> get(FlatTextFormat format)
    {
      return get(format, false, 0, false);
    }

    public Parser<String, Object> get(FlatTextFormat format, boolean hasHeaderRow, int maxSkipHeaderRows, boolean tryParseNumbers)
    {
      switch (format) {
        case CSV:
          return new CSVParser(null, hasHeaderRow, maxSkipHeaderRows, tryParseNumbers);
        case DELIMITED:
          return new DelimitedParser("\t", null, hasHeaderRow, maxSkipHeaderRows, tryParseNumbers);
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

  private static String concat(FlatTextFormat format, String... values)
  {
    return Arrays.stream(values).collect(Collectors.joining(format.getDefaultDelimiter()));
  }
}
