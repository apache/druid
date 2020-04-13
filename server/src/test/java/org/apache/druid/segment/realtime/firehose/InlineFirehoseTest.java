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

package org.apache.druid.segment.realtime.firehose;

import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

@SuppressWarnings("ConstantConditions")
public class InlineFirehoseTest
{
  private static final String DIMENSION_0 = "timestamp";
  private static final String DIMENSION_1 = "value";
  private static final List<String> DIMENSIONS = Arrays.asList(DIMENSION_0, DIMENSION_1);
  private static final String DELIMITER = ",";
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  private static final StringInputRowParser PARSER = new StringInputRowParser(
      new CSVParseSpec(
          new TimestampSpec(
              DIMENSION_0,
              "auto",
              null
          ),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(DIMENSIONS),
              Collections.emptyList(),
              Collections.emptyList()
          ),
          DELIMITER,
          DIMENSIONS,
          false,
          0
      ),
      CHARSET.name()
  );
  private static final String EMPTY = "";
  private static final String TIMESTAMP_0 = "0";
  private static final String VALUE_0 = "a";
  private static final String NOT_EMPTY = TIMESTAMP_0 + DELIMITER + VALUE_0;
  private static final String PARSEABLE = NOT_EMPTY;
  private static final String NOT_PARSEABLE = VALUE_0 + DELIMITER + TIMESTAMP_0;
  private static final String TIMESTAMP_1 = "1";
  private static final String VALUE_1 = "b";
  private static final String LINE_0 = TIMESTAMP_0 + DELIMITER + VALUE_0;
  private static final String LINE_1 = TIMESTAMP_1 + DELIMITER + VALUE_1;
  private static final String MULTILINE = LINE_0 + "\n" + LINE_1;

  @Test
  public void testHasMoreEmpty()
  {
    InlineFirehose target = create(EMPTY);
    Assert.assertFalse(target.hasMore());
  }

  @Test
  public void testHasMoreNotEmpty()
  {
    InlineFirehose target = create(NOT_EMPTY);
    Assert.assertTrue(target.hasMore());
  }

  @Test(expected = NoSuchElementException.class)
  public void testNextRowEmpty()
  {
    InlineFirehose target = create(EMPTY);
    target.nextRow();
  }

  @Test
  public void testNextRowNotEmpty()
  {
    InlineFirehose target = create(NOT_EMPTY);
    InputRow row = target.nextRow();
    assertRowValue(VALUE_0, row);
  }

  @Test(expected = NoSuchElementException.class)
  public void testNextRowWithRawEmpty()
  {
    InlineFirehose target = create(EMPTY);
    target.nextRowWithRaw();
  }

  @Test
  public void testNextRowWithRawParseable()
  {
    final String data = PARSEABLE;
    InlineFirehose target = create(data);
    InputRowListPlusRawValues rowPlusRaw = target.nextRowWithRaw();

    InputRow row = Iterables.getOnlyElement(rowPlusRaw.getInputRows());
    assertRowValue(VALUE_0, row);

    Map<String, Object> raw = rowPlusRaw.getRawValues();
    Map<String, Object> expected = new HashMap<>();
    expected.put("timestamp", TIMESTAMP_0);
    expected.put("value", VALUE_0);
    Assert.assertEquals(expected, raw);

    Assert.assertNull(rowPlusRaw.getParseException());
  }

  @Test
  public void testNextRowWithRawNotParseable()
  {
    final String data = NOT_PARSEABLE;
    InlineFirehose target = create(data);
    InputRowListPlusRawValues rowPlusRaw = target.nextRowWithRaw();

    Assert.assertNull(rowPlusRaw.getInputRows());

    Map<String, Object> raw = rowPlusRaw.getRawValues();
    Map<String, Object> expected = new HashMap<>();
    expected.put("timestamp", VALUE_0);
    expected.put("value", TIMESTAMP_0);
    Assert.assertEquals(expected, raw);


    Assert.assertNotNull(rowPlusRaw.getParseException());
  }

  @Test
  public void testCloseOpen() throws IOException
  {
    InlineFirehose target = create(NOT_EMPTY);
    target.close();
    try {
      target.nextRow();
      Assert.fail("Should not be able to read from closed firehose");
    }
    catch (NoSuchElementException ignored) {
    }
  }

  @Test
  public void testCloseNotOpen()
  {
    InlineFirehose target = create(NOT_EMPTY);
    try {
      target.close();
    }
    catch (IOException e) {
      Assert.fail("Should be able to close an opened firehose");
    }
    try {
      target.close();
    }
    catch (IOException e) {
      Assert.fail("Should be able to close a closed firehose");
    }
  }

  @Test
  public void testMultiline()
  {
    InlineFirehose target = create(MULTILINE);

    // First line
    Assert.assertTrue(target.hasMore());
    InputRow row0 = target.nextRow();
    assertRowValue(VALUE_0, row0);

    // Second line
    InputRowListPlusRawValues rowPlusRaw = target.nextRowWithRaw();
    assertRowValue(VALUE_1, Iterables.getOnlyElement(rowPlusRaw.getInputRows()));
    Map<String, Object> raw = rowPlusRaw.getRawValues();
    Map<String, Object> expected = new HashMap<>();
    expected.put("timestamp", TIMESTAMP_1);
    expected.put("value", VALUE_1);
    Assert.assertEquals(expected, raw);
    Assert.assertNull(rowPlusRaw.getParseException());

    Assert.assertFalse(target.hasMore());
  }

  private static InlineFirehose create(String data)
  {
    try {
      return new InlineFirehose(data, PARSER);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void assertRowValue(String expected, InputRow row)
  {
    Assert.assertNotNull(row);
    List<String> values = row.getDimension(DIMENSION_1);
    Assert.assertNotNull(values);
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(expected, values.get(0));
  }
}

