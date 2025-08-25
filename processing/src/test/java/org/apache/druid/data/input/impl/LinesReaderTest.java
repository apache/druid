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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LinesReaderTest extends InitializedNullHandlingTest
{
  @Test
  public void testSimpleLineParsing() throws IOException
  {
    final String input = "line1\nline2\nline3";
    final InputEntityReader reader = createReader(input, Collections.emptyList());

    final List<InputRow> inputRows = readAllRows(reader);

    Assert.assertEquals(3, inputRows.size());
    Assert.assertEquals("line1", inputRows.get(0).getRaw("line"));
    Assert.assertEquals("line2", inputRows.get(1).getRaw("line"));
    Assert.assertEquals("line3", inputRows.get(2).getRaw("line"));
  }

  @Test
  public void testEmptyLines() throws IOException
  {
    final String input = "line1\n\n line3\n";
    final InputEntityReader reader = createReader(input, Collections.emptyList());

    final List<InputRow> inputRows = readAllRows(reader);

    Assert.assertEquals(3, inputRows.size());
    Assert.assertEquals("line1", inputRows.get(0).getRaw("line"));
    Assert.assertEquals("", inputRows.get(1).getRaw("line"));
    Assert.assertEquals(" line3", inputRows.get(2).getRaw("line"));
  }

  @Test
  public void testSingleLine() throws IOException
  {
    final String input = "single line without newline";
    final InputEntityReader reader = createReader(input, Collections.emptyList());

    final List<InputRow> inputRows = readAllRows(reader);

    Assert.assertEquals(1, inputRows.size());
    Assert.assertEquals("single line without newline", inputRows.get(0).getRaw("line"));
  }

  @Test
  public void testToMap()
  {
    final String input = "test line";
    final LinesReader reader = (LinesReader) createReader(input, Collections.emptyList());

    final List<Map<String, Object>> maps = reader.toMap("test line");

    Assert.assertEquals(1, maps.size());
    Assert.assertEquals(ImmutableMap.of("line", "test line"), maps.get(0));
  }

  private InputEntityReader createReader(
      String input,
      List<String> columns
  )
  {
    final InputRowSchema inputRowSchema = new InputRowSchema(
        new TimestampSpec("__time", "auto", DateTimes.of("2000-01-01T00:00:00.000Z")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(columns)),
        ColumnsFilter.all()
    );

    final ByteEntity entity = new ByteEntity(StringUtils.toUtf8(input));

    return new LinesReader(inputRowSchema, entity);
  }

  private List<InputRow> readAllRows(InputEntityReader reader) throws IOException
  {
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      return Lists.newArrayList(iterator);
    }
  }
}
