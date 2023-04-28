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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.transform.TransformSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class StreamChunkParserTest
{
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec(null, null, null);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
  private final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
      rowIngestionMeters,
      false,
      0,
      0
  );

  @Mock
  private SettableByteEntityReader mockedByteEntityReader;

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
    final StreamChunkParser<ByteEntity> chunkParser = new StreamChunkParser<>(
        parser,
        // Set nulls for all parameters below since inputFormat will never be used.
        null,
        null,
        null,
        null,
        row -> true,
        rowIngestionMeters,
        parseExceptionHandler
    );
    parseAndAssertResult(chunkParser);
  }

  @Test
  public void testWithNullParserAndInputformatParseProperly() throws IOException
  {
    final JsonInputFormat inputFormat = new JsonInputFormat(JSONPathSpec.DEFAULT, Collections.emptyMap(), null, null, null);
    final StreamChunkParser<ByteEntity> chunkParser = new StreamChunkParser<>(
        null,
        inputFormat,
        new InputRowSchema(TIMESTAMP_SPEC, DimensionsSpec.EMPTY, ColumnsFilter.all()),
        TransformSpec.NONE,
        temporaryFolder.newFolder(),
        row -> true,
        rowIngestionMeters,
        parseExceptionHandler
    );
    parseAndAssertResult(chunkParser);
  }

  @Test
  public void testWithNullParserAndNullInputformatFailToCreateParser()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Either parser or inputFormat should be set");
    final StreamChunkParser<ByteEntity> chunkParser = new StreamChunkParser<>(
        null,
        null,
        null,
        null,
        null,
        row -> true,
        rowIngestionMeters,
        parseExceptionHandler
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
    final StreamChunkParser<ByteEntity> chunkParser = new StreamChunkParser<>(
        parser,
        inputFormat,
        new InputRowSchema(TIMESTAMP_SPEC, DimensionsSpec.EMPTY, ColumnsFilter.all()),
        TransformSpec.NONE,
        temporaryFolder.newFolder(),
        row -> true,
        rowIngestionMeters,
        parseExceptionHandler
    );
    parseAndAssertResult(chunkParser);
    Assert.assertTrue(inputFormat.props.used);
  }

  @Test
  public void parseEmptyNotEndOfShard() throws IOException
  {
    final TrackingJsonInputFormat inputFormat = new TrackingJsonInputFormat(
        JSONPathSpec.DEFAULT,
        Collections.emptyMap()
    );
    final StreamChunkParser<ByteEntity> chunkParser = new StreamChunkParser<>(
        null,
        inputFormat,
        new InputRowSchema(TIMESTAMP_SPEC, DimensionsSpec.EMPTY, ColumnsFilter.all()),
        TransformSpec.NONE,
        temporaryFolder.newFolder(),
        row -> true,
        rowIngestionMeters,
        parseExceptionHandler
    );
    List<InputRow> parsedRows = chunkParser.parse(ImmutableList.of(), false);
    Assert.assertEquals(0, parsedRows.size());
    Assert.assertEquals(0, rowIngestionMeters.getUnparseable());
    Assert.assertEquals(1, rowIngestionMeters.getThrownAway());
  }

  @Test
  public void parseEmptyEndOfShard() throws IOException
  {
    final TrackingJsonInputFormat inputFormat = new TrackingJsonInputFormat(
        JSONPathSpec.DEFAULT,
        Collections.emptyMap()
    );
    final StreamChunkParser<ByteEntity> chunkParser = new StreamChunkParser<>(
        null,
        inputFormat,
        new InputRowSchema(TIMESTAMP_SPEC, DimensionsSpec.EMPTY, ColumnsFilter.all()),
        TransformSpec.NONE,
        temporaryFolder.newFolder(),
        row -> true,
        rowIngestionMeters,
        parseExceptionHandler
    );
    List<InputRow> parsedRows = chunkParser.parse(ImmutableList.of(), true);
    Assert.assertEquals(0, parsedRows.size());
    Assert.assertEquals(0, rowIngestionMeters.getUnparseable());
    Assert.assertEquals(0, rowIngestionMeters.getThrownAway());
  }

  @Test
  public void testParseMalformedDataWithAllowedParseExceptions_thenNoException() throws IOException
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

    final int maxAllowedParseExceptions = 1;
    final StreamChunkParser<ByteEntity> chunkParser = new StreamChunkParser<>(
        parser,
        mockedByteEntityReader,
        row -> true,
        rowIngestionMeters,
        new ParseExceptionHandler(
            rowIngestionMeters,
            false,
            maxAllowedParseExceptions,
            0
        )
    );
    Mockito.when(mockedByteEntityReader.read()).thenThrow(new ParseException(null, "error parsing malformed data"));
    final String json = "malformedJson";

    List<InputRow> parsedRows = chunkParser.parse(
        Collections.singletonList(
            new ByteEntity(json.getBytes(StringUtils.UTF8_STRING))), false
    );
    // no exception and no parsed rows
    Assert.assertEquals(0, parsedRows.size());
    Assert.assertEquals(maxAllowedParseExceptions, rowIngestionMeters.getUnparseable());
  }

  @Test
  public void testParseMalformedDataException() throws IOException
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

    final StreamChunkParser<ByteEntity> chunkParser = new StreamChunkParser<>(
        parser,
        mockedByteEntityReader,
        row -> true,
        rowIngestionMeters,
        parseExceptionHandler
    );

    Mockito.when(mockedByteEntityReader.read()).thenThrow(new ParseException(null, "error parsing malformed data"));
    final String json = "malformedJson";
    List<ByteEntity> byteEntities = Arrays.asList(
        new ByteEntity(json.getBytes(StringUtils.UTF8_STRING)),
        new ByteEntity(json.getBytes(StringUtils.UTF8_STRING)),
        new ByteEntity(json.getBytes(StringUtils.UTF8_STRING)),
        new ByteEntity(json.getBytes(StringUtils.UTF8_STRING)),
        new ByteEntity(json.getBytes(StringUtils.UTF8_STRING))
    );
    Assert.assertThrows(
        "Max parse exceptions[0] exceeded",
        RE.class,
        () -> chunkParser.parse(byteEntities, false)
    );
    Assert.assertEquals(1, rowIngestionMeters.getUnparseable()); // should barf on the first unparseable row
  }

  @Test
  public void testParseMalformedDataWithUnlimitedAllowedParseExceptions_thenNoException() throws IOException
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

    final StreamChunkParser<ByteEntity> chunkParser = new StreamChunkParser<>(
        parser,
        mockedByteEntityReader,
        row -> true,
        rowIngestionMeters,
        new ParseExceptionHandler(
            rowIngestionMeters,
            false,
            Integer.MAX_VALUE,
            0
        )
    );

    Mockito.when(mockedByteEntityReader.read()).thenThrow(new ParseException(null, "error parsing malformed data"));
    final String json = "malformedJson";

    List<ByteEntity> byteEntities = Arrays.asList(
        new ByteEntity(json.getBytes(StringUtils.UTF8_STRING)),
        new ByteEntity(json.getBytes(StringUtils.UTF8_STRING)),
        new ByteEntity(json.getBytes(StringUtils.UTF8_STRING)),
        new ByteEntity(json.getBytes(StringUtils.UTF8_STRING)),
        new ByteEntity(json.getBytes(StringUtils.UTF8_STRING))
    );

    List<InputRow> parsedRows = chunkParser.parse(byteEntities, false);
    // no exception since we've unlimited threhold for parse exceptions
    Assert.assertEquals(0, parsedRows.size());
    Assert.assertEquals(byteEntities.size(), rowIngestionMeters.getUnparseable());
  }

  @Test
  public void testWithNullParserAndNullByteEntityReaderFailToInstantiate()
  {
    Assert.assertThrows(
        "Either parser or byteEntityReader should be set",
        IAE.class,
        () -> new StreamChunkParser<>(
            null,
            null,
            row -> true,
            rowIngestionMeters,
            parseExceptionHandler
        )
    );
  }

  private void parseAndAssertResult(StreamChunkParser<ByteEntity> chunkParser) throws IOException
  {
    final String json = "{\"timestamp\": \"2020-01-01\", \"dim\": \"val\", \"met\": \"val2\"}";
    List<InputRow> parsedRows = chunkParser.parse(Collections.singletonList(new ByteEntity(json.getBytes(StringUtils.UTF8_STRING))), false);
    Assert.assertEquals(1, parsedRows.size());
    InputRow row = parsedRows.get(0);
    Assert.assertEquals(DateTimes.of("2020-01-01"), row.getTimestamp());
    Assert.assertEquals("val", Iterables.getOnlyElement(row.getDimension("dim")));
    Assert.assertEquals("val2", Iterables.getOnlyElement(row.getDimension("met")));
    Assert.assertEquals(0, rowIngestionMeters.getUnparseable());
  }

  private static class TrackingJsonInputFormat extends JsonInputFormat
  {
    static class Props
    {
      private boolean used;
    }
    Props props;

    private TrackingJsonInputFormat(@Nullable JSONPathSpec flattenSpec,
                                    @Nullable Map<String, Boolean> featureSpec)
    {
      super(flattenSpec, featureSpec, null, null, null);
      props = new Props();
    }

    private TrackingJsonInputFormat(@Nullable JSONPathSpec flattenSpec,
                                    @Nullable Map<String, Boolean> featureSpec,
                                    boolean lineSplittable,
                                    Props props)
    {
      super(flattenSpec, featureSpec, null, lineSplittable, null, null);
      this.props = props;
    }

    @Override
    public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
    {
      props.used = true;
      return super.createReader(inputRowSchema, source, temporaryDirectory);
    }

    @Override
    public JsonInputFormat withLineSplittable(boolean lineSplittable)
    {
      return new TrackingJsonInputFormat(this.getFlattenSpec(),
                                         this.getFeatureSpec(),
                                         lineSplittable,
                                         //pass `props` to new object as reference,
                                         //so any changes on this property of the new object can also be seen from original object
                                         this.props);
    }
  }
}
