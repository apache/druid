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

public class CSVParserTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testValidHeader()
  {
    String csv = "time,value1,value2";
    final Parser<String, Object> csvParser;
    boolean parseable = true;
    try {
      csvParser = new CSVParser(Optional.<String>fromNullable(null), csv);
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
    String csv = "time,value1,value2,value2";
    final Parser<String, Object> csvParser;
    boolean parseable = true;
    try {
      csvParser = new CSVParser(Optional.<String>fromNullable(null), csv);
    }
    catch (Exception e) {
      parseable = false;
    }
    finally {
      Assert.assertFalse(parseable);
    }
  }

  @Test
  public void testCSVParserWithHeader()
  {
    String header = "time,value1,value2";
    final Parser<String, Object> csvParser = new CSVParser(Optional.<String>fromNullable(null), header);
    String body = "hello,world,foo";
    final Map<String, Object> jsonMap = csvParser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap
    );
  }

  @Test
  public void testCSVParserWithoutHeader()
  {
    final Parser<String, Object> csvParser = new CSVParser(Optional.<String>fromNullable(null), false, 0);
    String body = "hello,world,foo";
    final Map<String, Object> jsonMap = csvParser.parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("column_1", "hello", "column_2", "world", "column_3", "foo"),
        jsonMap
    );
  }

  @Test
  public void testCSVParserWithSkipHeaderRows()
  {
    final int skipHeaderRows = 2;
    final Parser<String, Object> csvParser = new CSVParser(
        Optional.absent(),
        false,
        skipHeaderRows
    );
    csvParser.startFileFromBeginning();
    final String[] body = new String[] {
        "header,line,1",
        "header,line,2",
        "hello,world,foo"
    };
    int index;
    for (index = 0; index < skipHeaderRows; index++) {
      Assert.assertNull(csvParser.parse(body[index]));
    }
    final Map<String, Object> jsonMap = csvParser.parse(body[index]);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("column_1", "hello", "column_2", "world", "column_3", "foo"),
        jsonMap
    );
  }

  @Test
  public void testCSVParserWithHeaderRow()
  {
    final Parser<String, Object> csvParser = new CSVParser(
        Optional.absent(),
        true,
        0
    );
    csvParser.startFileFromBeginning();
    final String[] body = new String[] {
        "time,value1,value2",
        "hello,world,foo"
    };
    Assert.assertNull(csvParser.parse(body[0]));
    final Map<String, Object> jsonMap = csvParser.parse(body[1]);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap
    );
  }

  @Test
  public void testCSVParserWithDifferentHeaderRows()
  {
    final Parser<String, Object> csvParser = new CSVParser(
        Optional.absent(),
        true,
        0
    );
    csvParser.startFileFromBeginning();
    final String[] body = new String[] {
        "time,value1,value2",
        "hello,world,foo"
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
        "time,value1,value2,value3",
        "hello,world,foo,bar"
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
  public void testCSVParserWithoutStartFileFromBeginning()
  {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage(
        "hasHeaderRow or maxSkipHeaderRows is not supported. Please check the indexTask supports these options."
    );

    final int skipHeaderRows = 2;
    final Parser<String, Object> csvParser = new CSVParser(
        Optional.absent(),
        false,
        skipHeaderRows
    );
    final String[] body = new String[] {
        "header,line,1",
        "header,line,2",
        "hello,world,foo"
    };
    csvParser.parse(body[0]);
  }
}
