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

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexing.common.task.FilteringCloseableInputRowIterator;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.transform.TransformSpec;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * Abstraction for parsing stream data which internally uses {@link org.apache.druid.data.input.InputEntityReader}
 * or {@link InputRowParser}. This class will be useful untill we remove the deprecated InputRowParser.
 */
class StreamChunkParser<RecordType extends ByteEntity>
{
  @Nullable
  private final InputRowParser<ByteBuffer> parser;
  @Nullable
  private final SettableByteEntityReader byteEntityReader;
  private final Predicate<InputRow> rowFilter;
  private final RowIngestionMeters rowIngestionMeters;
  private final ParseExceptionHandler parseExceptionHandler;

  /**
   * Either parser or inputFormat shouldn't be null.
   */
  StreamChunkParser(
      @Nullable InputRowParser<ByteBuffer> parser,
      @Nullable InputFormat inputFormat,
      InputRowSchema inputRowSchema,
      TransformSpec transformSpec,
      File indexingTmpDir,
      Predicate<InputRow> rowFilter,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler
  )
  {
    if (parser == null && inputFormat == null) {
      throw new IAE("Either parser or inputFormat should be set");
    }
    // parser is already decorated with transformSpec in DataSchema
    this.parser = parser;
    if (inputFormat != null) {
      this.byteEntityReader = new SettableByteEntityReader(
          inputFormat,
          inputRowSchema,
          transformSpec,
          indexingTmpDir
      );
    } else {
      this.byteEntityReader = null;
    }
    this.rowFilter = rowFilter;
    this.rowIngestionMeters = rowIngestionMeters;
    this.parseExceptionHandler = parseExceptionHandler;
  }

  List<InputRow> parse(@Nullable List<RecordType> streamChunk) throws IOException
  {
    if (streamChunk == null || streamChunk.isEmpty()) {
      rowIngestionMeters.incrementThrownAway();
      return Collections.emptyList();
    } else {
      if (byteEntityReader != null) {
        return parseWithInputFormat(byteEntityReader, streamChunk);
      } else {
        return parseWithParser(parser, streamChunk);
      }
    }
  }

  private List<InputRow> parseWithParser(InputRowParser<ByteBuffer> parser, List<? extends ByteEntity> valueBytess)
  {
    final FluentIterable<InputRow> iterable = FluentIterable
        .from(valueBytess)
        .transformAndConcat(bytes -> parser.parseBatch(bytes.getBuffer()));

    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        CloseableIterators.withEmptyBaggage(iterable.iterator()),
        rowFilter,
        rowIngestionMeters,
        parseExceptionHandler
    );
    return Lists.newArrayList(rowIterator);
  }

  private List<InputRow> parseWithInputFormat(
      SettableByteEntityReader byteEntityReader,
      List<? extends ByteEntity> valueBytess
  ) throws IOException
  {
    final List<InputRow> rows = new ArrayList<>();
    for (ByteEntity valueBytes : valueBytess) {
      byteEntityReader.setEntity(valueBytes);
      try (FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
          byteEntityReader.read(),
          rowFilter,
          rowIngestionMeters,
          parseExceptionHandler
      )) {
        rowIterator.forEachRemaining(rows::add);
      }
    }
    return rows;
  }
}
