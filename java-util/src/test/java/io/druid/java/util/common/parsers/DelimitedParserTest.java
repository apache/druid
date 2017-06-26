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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class DelimitedParserTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testValidHeader()
  {
    String tsv = "time\tvalue1\tvalue2";
    final Parser<String, Object> delimitedParser;
    boolean parseable = true;
    try {
      delimitedParser = new DelimitedParser(Optional.of("\t"), Optional.<String>absent(), tsv);
    }
    catch (Exception e) {
      parseable = false;
    }
    finally {
      Assert.assertTrue(parseable);
    }
  }

  @Test
  public void testInvalidHeader()
  {
    String tsv = "time\tvalue1\tvalue2\tvalue2";
    final Parser<String, Object> delimitedParser;
    boolean parseable = true;
    try {
      delimitedParser = new DelimitedParser(Optional.of("\t"), Optional.<String>absent(), tsv);
    }
    catch (Exception e) {
      parseable = false;
    }
    finally {
      Assert.assertFalse(parseable);
    }
  }

  @Test
  public void testTSVParserWithHeader()
  {
    String header = "time\tvalue1\tvalue2";
    final Parser<String, Object> delimitedParser = new DelimitedParser(
        Optional.of("\t"),
        Optional.<String>absent(),
        header
    );
    String body = "hello\tworld\tfoo";
    final Map<String, Object> jsonMap = delimitedParser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap
    );
  }

  @Test
  public void testTSVParserWithoutHeader()
  {
    final Parser<String, Object> delimitedParser = new DelimitedParser(
        Optional.of("\t"),
        Optional.<String>absent(),
        false,
        0
    );
    String body = "hello\tworld\tfoo";
    final Map<String, Object> jsonMap = delimitedParser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("column_1", "hello", "column_2", "world", "column_3", "foo"),
        jsonMap
    );
  }

  @Test
  public void testTSVParserWithSkipHeaderRows()
  {
    final int skipHeaderRows = 2;
    final Parser<String, Object> delimitedParser = new DelimitedParser(
        Optional.of("\t"),
        Optional.absent(),
        false,
        skipHeaderRows
    );
    delimitedParser.startFileFromBeginning();
    final String[] body = new String[] {
        "header\tline\t1",
        "header\tline\t2",
        "hello\tworld\tfoo"
    };
    int index;
    for (index = 0; index < skipHeaderRows; index++) {
      Assert.assertNull(delimitedParser.parse(body[index]));
    }
    final Map<String, Object> jsonMap = delimitedParser.parse(body[index]);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("column_1", "hello", "column_2", "world", "column_3", "foo"),
        jsonMap
    );
  }

  @Test
  public void testTSVParserWithHeaderRow()
  {
    final Parser<String, Object> parser = new DelimitedParser(
        Optional.of("\t"),
        Optional.absent(),
        true,
        0
    );
    parser.startFileFromBeginning();
    final String[] body = new String[] {
        "time\tvalue1\tvalue2",
        "hello\tworld\tfoo"
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
  public void testTSVParserWithDifferentHeaderRows()
  {
    final Parser<String, Object> csvParser = new DelimitedParser(
        Optional.of("\t"),
        Optional.absent(),
        true,
        0
    );
    csvParser.startFileFromBeginning();
    final String[] body = new String[] {
        "time\tvalue1\tvalue2",
        "hello\tworld\tfoo"
    };
    Assert.assertNull(csvParser.parse(body[0]));
    Map<String, Object> jsonMap = csvParser.parse(body[1]);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap
    );

    csvParser.startFileFromBeginning();
    final String[] body2 = new String[] {
        "time\tvalue1\tvalue2\tvalue3",
        "hello\tworld\tfoo\tbar"
    };
    Assert.assertNull(csvParser.parse(body2[0]));
    jsonMap = csvParser.parse(body2[1]);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo", "value3", "bar"),
        jsonMap
    );
  }

  @Test
  public void testTSVParserWithoutStartFileFromBeginning()
  {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage(
        "hasHeaderRow or maxSkipHeaderRows is not supported. Please check the indexTask supports these options."
    );

    final int skipHeaderRows = 2;
    final Parser<String, Object> delimitedParser = new DelimitedParser(
        Optional.of("\t"),
        Optional.absent(),
        false,
        skipHeaderRows
    );
    final String[] body = new String[] {
        "header\tline\t1",
        "header\tline\t2",
        "hello\tworld\tfoo"
    };
    delimitedParser.parse(body[0]);
  }
}
