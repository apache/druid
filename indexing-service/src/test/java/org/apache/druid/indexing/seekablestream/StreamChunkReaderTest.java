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
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.task.InputRowFilter;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.incremental.InputRowFilterResult;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StreamChunkReaderTest
{
  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolderExtension = TemporaryFolderExtension.perTest();
  private final File temporaryFolder = temporaryFolderExtension.getRoot();

  private final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
  private final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
      rowIngestionMeters,
      false,
      0,
      0
  );

  @Mock
  private SettableByteEntityReader mockedByteEntityReader;
  private AutoCloseable mocks;

  @BeforeEach
  public void setup()
  {
    mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testInputformatParseProperly() throws IOException
  {
    final JsonInputFormat inputFormat = new JsonInputFormat(JSONPathSpec.DEFAULT, Collections.emptyMap(), null, null, null);
    final StreamChunkReader<ByteEntity> chunkParser = new StreamChunkReader<>(
        inputFormat,
        new InputRowSchema(TimestampSpec.DEFAULT, DimensionsSpec.EMPTY, ColumnsFilter.all()),
        TransformSpec.NONE,
        FileUtils.createTempDirInLocation(temporaryFolder.toPath(), null),
        InputRowFilter.allowAll(),
        rowIngestionMeters,
        parseExceptionHandler
    );
    parseAndAssertResult(chunkParser);
  }

  @Test
  public void testWithNullParserAndNullInputformatFailToCreateParser()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new StreamChunkReader<>(
            null,
            null,
            null,
            null,
            InputRowFilter.allowAll(),
            rowIngestionMeters,
            parseExceptionHandler
        )
    );
    Assertions.assertEquals("inputFormat must not be null", t.getMessage());
  }


  @Test
  public void parseEmptyNotEndOfShard() throws IOException
  {
    final TrackingJsonInputFormat inputFormat = new TrackingJsonInputFormat(
        JSONPathSpec.DEFAULT,
        Collections.emptyMap()
    );
    final StreamChunkReader<ByteEntity> chunkParser = new StreamChunkReader<>(
        inputFormat,
        new InputRowSchema(TimestampSpec.DEFAULT, DimensionsSpec.EMPTY, ColumnsFilter.all()),
        TransformSpec.NONE,
        FileUtils.createTempDirInLocation(temporaryFolder.toPath(), null),
        InputRowFilter.allowAll(),
        rowIngestionMeters,
        parseExceptionHandler
    );
    List<InputRow> parsedRows = chunkParser.parse(ImmutableList.of(), false);
    Assertions.assertEquals(0, parsedRows.size());
    Assertions.assertEquals(0, rowIngestionMeters.getUnparseable());
    Assertions.assertEquals(1, rowIngestionMeters.getThrownAway());
  }

  @Test
  public void parseEmptyEndOfShard() throws IOException
  {
    final TrackingJsonInputFormat inputFormat = new TrackingJsonInputFormat(
        JSONPathSpec.DEFAULT,
        Collections.emptyMap()
    );
    final StreamChunkReader<ByteEntity> chunkParser = new StreamChunkReader<>(
        inputFormat,
        new InputRowSchema(TimestampSpec.DEFAULT, DimensionsSpec.EMPTY, ColumnsFilter.all()),
        TransformSpec.NONE,
        FileUtils.createTempDirInLocation(temporaryFolder.toPath(), null),
        InputRowFilter.allowAll(),
        rowIngestionMeters,
        parseExceptionHandler
    );
    List<InputRow> parsedRows = chunkParser.parse(ImmutableList.of(), true);
    Assertions.assertEquals(0, parsedRows.size());
    Assertions.assertEquals(0, rowIngestionMeters.getUnparseable());
    Assertions.assertEquals(0, rowIngestionMeters.getThrownAway());
  }

  @Test
  public void testTransformSpecFilterIncrementsCustomFilterReason() throws IOException
  {
    final JsonInputFormat inputFormat = new JsonInputFormat(
        JSONPathSpec.DEFAULT,
        Collections.emptyMap(),
        null,
        null,
        null
    );
    final TransformSpec transformSpec = new TransformSpec(
        new AndDimFilter(
            new SelectorDimFilter("column_a", "y", null),
            new NotDimFilter(new SelectorDimFilter("column_b", "other", null))
        ),
        null
    );
    final StreamChunkReader<ByteEntity> chunkParser = new StreamChunkReader<>(
        inputFormat,
        new InputRowSchema(TimestampSpec.DEFAULT, DimensionsSpec.EMPTY, ColumnsFilter.all()),
        transformSpec,
        FileUtils.createTempDirInLocation(temporaryFolder.toPath(), null),
        InputRowFilter.allowAll(),
        rowIngestionMeters,
        parseExceptionHandler
    );
    final List<InputRow> parsedRows = chunkParser.parse(
        Arrays.asList(
            new ByteEntity(
                "{\"timestamp\": \"2020-01-01\", \"column_a\": \"y\", \"column_b\": \"other\"}"
                    .getBytes(StringUtils.UTF8_STRING)
            ),
            new ByteEntity(
                "{\"timestamp\": \"2020-01-01\", \"column_a\": \"y\", \"column_b\": \"title1\"}"
                    .getBytes(StringUtils.UTF8_STRING)
            )
        ),
        false
    );

    Assertions.assertEquals(1, parsedRows.size());
    Assertions.assertEquals("title1", Iterables.getOnlyElement(parsedRows.get(0).getDimension("column_b")));
    Assertions.assertEquals(1, rowIngestionMeters.getThrownAway());

    final Map<String, Long> thrownAwayByReason = rowIngestionMeters.getThrownAwayByReason();
    Assertions.assertEquals(Long.valueOf(1), thrownAwayByReason.get(InputRowFilterResult.CUSTOM_FILTER.getReason()));
    Assertions.assertFalse(thrownAwayByReason.containsKey(InputRowFilterResult.NULL_OR_EMPTY_RECORD.getReason()));
  }

  @Test
  public void testTransformSpecFilterPreservesOtherRejectionReasons() throws IOException
  {
    final JsonInputFormat inputFormat = new JsonInputFormat(
        JSONPathSpec.DEFAULT,
        Collections.emptyMap(),
        null,
        null,
        null
    );
    final TransformSpec transformSpec = new TransformSpec(
        new AndDimFilter(
            new SelectorDimFilter("column_a", "y", null),
            new NotDimFilter(new SelectorDimFilter("column_b", "other", null))
        ),
        null
    );
    final InputRowFilter rowFilter = row -> {
      if (row == null) {
        return InputRowFilterResult.NULL_OR_EMPTY_RECORD;
      } else if ("late".equals(row.getRaw("column_b"))) {
        return InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME;
      } else if ("early".equals(row.getRaw("column_b"))) {
        return InputRowFilterResult.AFTER_MAX_MESSAGE_TIME;
      }
      return InputRowFilterResult.ACCEPTED;
    };
    final StreamChunkReader<ByteEntity> chunkParser = new StreamChunkReader<>(
        inputFormat,
        new InputRowSchema(TimestampSpec.DEFAULT, DimensionsSpec.EMPTY, ColumnsFilter.all()),
        transformSpec,
        FileUtils.createTempDirInLocation(temporaryFolder.toPath(), null),
        rowFilter,
        rowIngestionMeters,
        parseExceptionHandler
    );

    chunkParser.parse(ImmutableList.of(), false);
    final List<InputRow> parsedRows = chunkParser.parse(
        Arrays.asList(
            new ByteEntity(
                "{\"timestamp\": \"2020-01-01\", \"column_a\": \"y\", \"column_b\": \"other\"}"
                    .getBytes(StringUtils.UTF8_STRING)
            ),
            new ByteEntity(
                "{\"timestamp\": \"2020-01-01\", \"column_a\": \"y\", \"column_b\": \"late\"}"
                    .getBytes(StringUtils.UTF8_STRING)
            ),
            new ByteEntity(
                "{\"timestamp\": \"2020-01-01\", \"column_a\": \"y\", \"column_b\": \"early\"}"
                    .getBytes(StringUtils.UTF8_STRING)
            ),
            new ByteEntity(
                "{\"timestamp\": \"2020-01-01\", \"column_a\": \"y\", \"column_b\": \"title1\"}"
                    .getBytes(StringUtils.UTF8_STRING)
            )
        ),
        false
    );

    Assertions.assertEquals(1, parsedRows.size());
    Assertions.assertEquals("title1", Iterables.getOnlyElement(parsedRows.get(0).getDimension("column_b")));
    Assertions.assertEquals(4, rowIngestionMeters.getThrownAway());

    final Map<String, Long> thrownAwayByReason = rowIngestionMeters.getThrownAwayByReason();
    Assertions.assertEquals(Long.valueOf(1), thrownAwayByReason.get(InputRowFilterResult.NULL_OR_EMPTY_RECORD.getReason()));
    Assertions.assertEquals(Long.valueOf(1), thrownAwayByReason.get(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME.getReason()));
    Assertions.assertEquals(Long.valueOf(1), thrownAwayByReason.get(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME.getReason()));
    Assertions.assertEquals(Long.valueOf(1), thrownAwayByReason.get(InputRowFilterResult.CUSTOM_FILTER.getReason()));
    Assertions.assertFalse(thrownAwayByReason.containsKey(InputRowFilterResult.UNKNOWN.getReason()));
  }

  @Test
  public void testParseMalformedDataWithAllowedParseExceptions_thenNoException() throws IOException
  {
    final int maxAllowedParseExceptions = 1;
    final StreamChunkReader<ByteEntity> chunkParser = new StreamChunkReader<>(
        mockedByteEntityReader,
        InputRowFilter.allowAll(),
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
    Assertions.assertEquals(0, parsedRows.size());
    Assertions.assertEquals(maxAllowedParseExceptions, rowIngestionMeters.getUnparseable());
  }

  @Test
  public void testParseMalformedDataException() throws IOException
  {
    final StreamChunkReader<ByteEntity> chunkParser = new StreamChunkReader<>(
        mockedByteEntityReader,
        InputRowFilter.allowAll(),
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
    Assertions.assertThrows(
        RE.class,
        () -> chunkParser.parse(byteEntities, false),
        "Max parse exceptions[0] exceeded"
    );
    Assertions.assertEquals(1, rowIngestionMeters.getUnparseable()); // should barf on the first unparseable row
  }

  @Test
  public void testParseMalformedDataWithUnlimitedAllowedParseExceptions_thenNoException() throws IOException
  {
    final StreamChunkReader<ByteEntity> chunkParser = new StreamChunkReader<>(
        mockedByteEntityReader,
        InputRowFilter.allowAll(),
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
    Assertions.assertEquals(0, parsedRows.size());
    Assertions.assertEquals(byteEntities.size(), rowIngestionMeters.getUnparseable());
  }

  @Test
  public void testWithNullParserAndNullByteEntityReaderFailToInstantiate()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new StreamChunkReader<>(
            null,
            InputRowFilter.allowAll(),
            rowIngestionMeters,
            parseExceptionHandler
        )
    );
    Assertions.assertEquals("byteEntityReader must not be null", t.getMessage());
  }

  private void parseAndAssertResult(StreamChunkReader<ByteEntity> chunkParser) throws IOException
  {
    final String json = "{\"timestamp\": \"2020-01-01\", \"dim\": \"val\", \"met\": \"val2\"}";
    List<InputRow> parsedRows = chunkParser.parse(Collections.singletonList(new ByteEntity(json.getBytes(StringUtils.UTF8_STRING))), false);
    Assertions.assertEquals(1, parsedRows.size());
    InputRow row = parsedRows.get(0);
    Assertions.assertEquals(DateTimes.of("2020-01-01"), row.getTimestamp());
    Assertions.assertEquals("val", Iterables.getOnlyElement(row.getDimension("dim")));
    Assertions.assertEquals("val2", Iterables.getOnlyElement(row.getDimension("met")));
    Assertions.assertEquals(0, rowIngestionMeters.getUnparseable());
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
