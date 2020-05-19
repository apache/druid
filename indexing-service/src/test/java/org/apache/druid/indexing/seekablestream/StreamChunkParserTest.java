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
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
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

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
        new JSONParseSpec(
            TIMESTAMP_SPEC,
            DimensionsSpec.EMPTY,
            JSONPathSpec.DEFAULT,
            Collections.emptyMap(),
            false
        ),
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
    parseAndAssertResult(chunkParser);
  }

  @Test
  public void testWithNullParserAndInputformatParseProperly() throws IOException
  {
    final JsonInputFormat inputFormat = new JsonInputFormat(JSONPathSpec.DEFAULT, Collections.emptyMap(), null);
    final StreamChunkParser chunkParser = new StreamChunkParser(
        null,
        inputFormat,
        new InputRowSchema(TIMESTAMP_SPEC, DimensionsSpec.EMPTY, Collections.emptyList()),
        TransformSpec.NONE,
        temporaryFolder.newFolder()
    );
    parseAndAssertResult(chunkParser);
  }

  @Test
  public void testWithNullParserAndNullInputformatFailToCreateParser()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Either parser or inputFormat should be set");
    final StreamChunkParser chunkParser = new StreamChunkParser(
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testBothParserAndInputFormatParseProperlyUsingInputFormat() throws IOException
  {
    final InputRowParser<ByteBuffer> parser = new StringInputRowParser(
        new JSONParseSpec(
            TIMESTAMP_SPEC,
            DimensionsSpec.EMPTY,
            JSONPathSpec.DEFAULT,
            Collections.emptyMap(),
            false
        ),
        StringUtils.UTF8_STRING
    );
    final TrackingJsonInputFormat inputFormat = new TrackingJsonInputFormat(
        JSONPathSpec.DEFAULT,
        Collections.emptyMap()
    );
    final StreamChunkParser chunkParser = new StreamChunkParser(
        parser,
        inputFormat,
        new InputRowSchema(TIMESTAMP_SPEC, DimensionsSpec.EMPTY, Collections.emptyList()),
        TransformSpec.NONE,
        temporaryFolder.newFolder()
    );
    parseAndAssertResult(chunkParser);
    Assert.assertTrue(inputFormat.used);
  }

  private void parseAndAssertResult(StreamChunkParser chunkParser) throws IOException
  {
    final String json = "{\"timestamp\": \"2020-01-01\", \"dim\": \"val\", \"met\": \"val2\"}";
    List<InputRow> parsedRows = chunkParser.parse(Collections.singletonList(json.getBytes(StringUtils.UTF8_STRING)));
    Assert.assertEquals(1, parsedRows.size());
    InputRow row = parsedRows.get(0);
    Assert.assertEquals(DateTimes.of("2020-01-01"), row.getTimestamp());
    Assert.assertEquals("val", Iterables.getOnlyElement(row.getDimension("dim")));
    Assert.assertEquals("val2", Iterables.getOnlyElement(row.getDimension("met")));
  }

  private static class TrackingJsonInputFormat extends JsonInputFormat
  {
    private boolean used;

    private TrackingJsonInputFormat(@Nullable JSONPathSpec flattenSpec, @Nullable Map<String, Boolean> featureSpec)
    {
      super(flattenSpec, featureSpec, null);
    }

    @Override
    public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
    {
      used = true;
      return super.createReader(inputRowSchema, source, temporaryDirectory);
    }
  }
}
