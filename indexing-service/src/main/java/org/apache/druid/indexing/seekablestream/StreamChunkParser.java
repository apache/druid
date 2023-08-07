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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.FluentIterable;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.transform.TransformSpec;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Abstraction for parsing stream data which internally uses {@link org.apache.druid.data.input.InputEntityReader}
 * or {@link InputRowParser}. This class will be useful until we remove the deprecated {@link InputRowParser}.
 */
class StreamChunkParser<RecordType extends ByteEntity>
{
  @Nullable
  private final InputRowParser<ByteBuffer> parser;
  @Nullable
  private final SettableByteEntityReader<RecordType> byteEntityReader;
  private final Predicate<InputRow> rowFilter;

  /**
   * Either parser or inputFormat shouldn't be null.
   */
  StreamChunkParser(
      @Nullable InputRowParser<ByteBuffer> parser,
      @Nullable InputFormat inputFormat,
      InputRowSchema inputRowSchema,
      TransformSpec transformSpec,
      File indexingTmpDir,
      Predicate<InputRow> rowFilter
  )
  {
    if (parser == null && inputFormat == null) {
      throw new IAE("Either parser or inputFormat should be set");
    }
    // parser is already decorated with transformSpec in DataSchema
    this.parser = parser;
    if (inputFormat != null) {
      this.byteEntityReader = new SettableByteEntityReader<>(
          inputFormat,
          inputRowSchema,
          transformSpec,
          indexingTmpDir
      );
    } else {
      this.byteEntityReader = null;
    }
    this.rowFilter = rowFilter;
  }

  @VisibleForTesting
  StreamChunkParser(
      @Nullable InputRowParser<ByteBuffer> parser,
      @Nullable SettableByteEntityReader<RecordType> byteEntityReader,
      Predicate<InputRow> rowFilter
  )
  {
    if (parser == null && byteEntityReader == null) {
      throw new IAE("Either parser or byteEntityReader should be set");
    }
    this.parser = parser;
    this.byteEntityReader = byteEntityReader;
    this.rowFilter = rowFilter;
  }

  List<ParseResult> parse(@Nullable List<RecordType> streamChunk, boolean isEndOfShard) throws IOException
  {
    if (streamChunk == null || streamChunk.isEmpty()) {
      // We do not count end of shard record as thrown away event since this is a record created by Druid
      // Note that this only applies to Kinesis
      if (isEndOfShard) {
        return Collections.singletonList(ParseResult.forEndOfShard());
      } else {
        return Collections.singletonList(ParseResult.forEmptyChunk());
      }
    } else {
      if (byteEntityReader != null) {
        return parseWithInputFormat(byteEntityReader, streamChunk);
      } else {
        return parseWithParser(parser, streamChunk);
      }
    }
  }

  private List<ParseResult> parseWithParser(InputRowParser<ByteBuffer> parser, List<? extends ByteEntity> valueBytes)
  {
    final FluentIterable<InputRow> iterable = FluentIterable
        .from(valueBytes)
        .transform(ByteEntity::getBuffer)
        .transform(this::incrementProcessedBytes)
        .transformAndConcat(bytes -> {
          // TODO push down synchronized{} so that each implementation can decide thread safety
          //      for the first pass, we do it here, to guarantee it is safe
          synchronized (parser) {
            return parser.parseBatch(bytes);
          }
        });
    return rowsToParseResult(iterable.iterator());
  }

  /**
   * Increments the processed bytes with the number of bytes remaining in the
   * given buffer. This method must be called before reading the buffer.
   */
  private ByteBuffer incrementProcessedBytes(final ByteBuffer recordByteBuffer)
  {
    // TODO re-enable this, somehow. We need to make the increment data available
    //      to the processor of the ParseResult. Should we return a Pair? or is it
    //      a part of the ParseResult itself? How to deal with multiple parsed
    //      results per bytebuffer?
    // rowIngestionMeters.incrementProcessedBytes(recordByteBuffer.remaining());
    return recordByteBuffer;
  }

  private List<ParseResult> parseWithInputFormat(
      SettableByteEntityReader byteEntityReader,
      List<? extends ByteEntity> valueBytess
  ) throws IOException
  {
    final List<ParseResult> allParseResults = new ArrayList<>();
    for (ByteEntity valueBytes : valueBytess) {
      incrementProcessedBytes(valueBytes.getBuffer());
      byteEntityReader.setEntity(valueBytes);
      try (CloseableIterator<InputRow> rows = byteEntityReader.read()) {
        allParseResults.addAll(rowsToParseResult(rows));
      }
      catch (ParseException pe) {
        allParseResults.add(ParseResult.fromParseException(pe));
      }
    }
    return allParseResults;
  }

  List<ParseResult> rowsToParseResult(Iterator<InputRow> rows)
  {
    final List<ParseResult> parseResults = new ArrayList<>();
    while (true) {
      try {
        if (!rows.hasNext()) {
          break;
        }

        InputRow nextRow = rows.next();
        boolean filterResult = rowFilter.test(nextRow);
        parseResults.add(ParseResult.fromFilterResult(nextRow, filterResult));
      }
      catch (ParseException ex) {
        parseResults.add(ParseResult.fromParseException(ex));
      }
    }
    return parseResults;
  }


  public enum ParseResultCode
  {
    UNPROCESSED,
    PROCESSED,
    PROCESSED_WITH_ERROR,
    UNPARSEABLE,
    THROWN_AWAY,
    END_OF_SHARD,
  }

  public static class ParseResult
  {
    final InputRow output;
    final ParseException parseException;
    final ParseResultCode resultCode;

    public static ParseResult forEndOfShard()
    {
      return new ParseResult(ParseResultCode.END_OF_SHARD, null, null);
    }

    public static ParseResult forEmptyChunk()
    {
      return new ParseResult(ParseResultCode.THROWN_AWAY, null, null);
    }

    public ParseResult mapResult(UnaryOperator<InputRow> mapFn)
    {
      return new ParseResult(
              resultCode,
              mapFn.apply(output),
              parseException);
    }

    public static ParseResult fromFilterResult(InputRow row, boolean valid)
    {
      return new ParseResult(
              valid ? ParseResultCode.PROCESSED : ParseResultCode.THROWN_AWAY,
              valid ? row : null,
              null);
    }

    public static ParseResult fromParseException(ParseException ex)
    {
      return new ParseResult(
              ex.isFromPartiallyValidRow()
                      ? ParseResultCode.PROCESSED_WITH_ERROR
                      : ParseResultCode.UNPARSEABLE,
              null,
              ex);
    }

    ParseResult(ParseResultCode rc, InputRow output, ParseException ex)
    {
      this.resultCode = rc;
      this.output = output;
      this.parseException = ex;
    }

    /**
     * Performs application of meter changes and exception handler, and fetches the row.
     *
     * @param rowIngestionMeters
     * @param parseExceptionHandler
     * @return the parsed row, or null if row was thrown away at filtering
     */
    @Nullable
    InputRow getInputRowAndApplyHandlers(
            RowIngestionMeters rowIngestionMeters,
            ParseExceptionHandler parseExceptionHandler
    )
    {
      if (resultCode == ParseResultCode.THROWN_AWAY) {
        // do not record other metrics
        // if it was an errored row, then the parseExceptionHandler will increment the counter
        // and perhaps even throw itself
        rowIngestionMeters.incrementThrownAway();
      }

      parseExceptionHandler.handle(parseException);

      return output;
    }

    /**
     * Fetches the row, without updating any meters or handling exceptions.
     *
     * @return the row, or null if row was thrown away at filtering
     */
    @Nullable
    public InputRow getRowRaw()
    {
      return output;
    }

    @Nullable
    public ParseException getParseException()
    {
      return parseException;
    }

    @Override
    public String toString()
    {
      return "ParseResult=[" +
              "resultCode=" + resultCode + ", " +
              "output=" + output + ", " +
              "parseException=" + parseException +
              "]";
    }
  }
}
