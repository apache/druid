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

package org.apache.druid.indexing.seekablestream;

import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class StreamChunkParserTest
{
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec(null, null, null);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testWithParserAndNullInputformatParseProperly() throws IOException
  {
    final InputRowParser<ByteBuffer> parser = new StringInputRowParser(
        new NotConvertibleToInputFormatParseSpec(),
        StringUtils.UTF8_STRING
    );
    final StreamChunkParser chunkParser = new StreamChunkParser(
        parser,
        // Set nulls for all parameters below since inputFormat will be never used.
        null,
        null,
        null,
        null
    );
    final String json = "{\"timestamp\": \"2020-01-01\", \"dim\": \"val\", \"met\": \"val2\"}";
    List<InputRow> parsedRows = chunkParser.parse(Collections.singletonList(json.getBytes(StringUtils.UTF8_STRING)));
    Assert.assertEquals(1, parsedRows.size());
    InputRow row = parsedRows.get(0);
    Assert.assertEquals(DateTimes.of("2020-01-01"), row.getTimestamp());
    Assert.assertEquals("val", Iterables.getOnlyElement(row.getDimension("dim")));
    Assert.assertEquals("val2", Iterables.getOnlyElement(row.getDimension("met")));
  }

  @Test
  public void testWithNullParserAndInputformatParseProperly() throws IOException
  {
    final JsonInputFormat inputFormat = new JsonInputFormat(JSONPathSpec.DEFAULT, Collections.emptyMap());
    final StreamChunkParser chunkParser = new StreamChunkParser(
        null,
        inputFormat,
        new InputRowSchema(TIMESTAMP_SPEC, DimensionsSpec.EMPTY, Collections.emptyList()),
        TransformSpec.NONE,
        temporaryFolder.newFolder()
    );
    final String json = "{\"timestamp\": \"2020-01-01\", \"dim\": \"val\", \"met\": \"val2\"}";
    List<InputRow> parsedRows = chunkParser.parse(Collections.singletonList(json.getBytes(StringUtils.UTF8_STRING)));
    Assert.assertEquals(1, parsedRows.size());
    InputRow row = parsedRows.get(0);
    Assert.assertEquals(DateTimes.of("2020-01-01"), row.getTimestamp());
    Assert.assertEquals("val", Iterables.getOnlyElement(row.getDimension("dim")));
    Assert.assertEquals("val2", Iterables.getOnlyElement(row.getDimension("met")));
  }

  @Test
  public void testWithNullParserAndNullInputformatFailToCreateParser()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Either parser or inputFormat shouldn't be set");
    final StreamChunkParser chunkParser = new StreamChunkParser(
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testBothParserAndInputFormatParseProperlyUsingParser() throws IOException
  {
    final TrackingStringInputRowParser parser = new TrackingStringInputRowParser(
        new NotConvertibleToInputFormatParseSpec(),
        StringUtils.UTF8_STRING
    );
    final JsonInputFormat inputFormat = new JsonInputFormat(JSONPathSpec.DEFAULT, Collections.emptyMap());
    final StreamChunkParser chunkParser = new StreamChunkParser(
        parser,
        inputFormat,
        new InputRowSchema(TIMESTAMP_SPEC, DimensionsSpec.EMPTY, Collections.emptyList()),
        TransformSpec.NONE,
        temporaryFolder.newFolder()
    );
    final String json = "{\"timestamp\": \"2020-01-01\", \"dim\": \"val\", \"met\": \"val2\"}";
    List<InputRow> parsedRows = chunkParser.parse(Collections.singletonList(json.getBytes(StringUtils.UTF8_STRING)));
    Assert.assertEquals(1, parsedRows.size());
    InputRow row = parsedRows.get(0);
    Assert.assertEquals(DateTimes.of("2020-01-01"), row.getTimestamp());
    Assert.assertEquals("val", Iterables.getOnlyElement(row.getDimension("dim")));
    Assert.assertEquals("val2", Iterables.getOnlyElement(row.getDimension("met")));
    Assert.assertTrue(parser.used);
  }

  private static class NotConvertibleToInputFormatParseSpec extends JSONParseSpec
  {
    private NotConvertibleToInputFormatParseSpec()
    {
      super(
          TIMESTAMP_SPEC,
          DimensionsSpec.EMPTY,
          JSONPathSpec.DEFAULT,
          Collections.emptyMap()
      );
    }

    @Override
    public InputFormat toInputFormat()
    {
      return null;
    }
  }

  private static class TrackingStringInputRowParser extends StringInputRowParser
  {
    private boolean used;

    private TrackingStringInputRowParser(ParseSpec parseSpec, String encoding)
    {
      super(parseSpec, encoding);
    }

    @Override
    public List<InputRow> parseBatch(ByteBuffer input)
    {
      used = true;
      return super.parseBatch(input);
    }

    @Override
    public InputRow parse(String input)
    {
      used = true;
      return super.parse(input);
    }
  }
}
