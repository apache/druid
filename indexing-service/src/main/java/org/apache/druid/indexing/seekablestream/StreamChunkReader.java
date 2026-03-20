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
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.task.FilteringCloseableInputRowIterator;
import org.apache.druid.indexing.common.task.InputRowFilter;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.incremental.InputRowFilterResult;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.transform.TransformSpec;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Abstraction for parsing stream data which internally uses {@link org.apache.druid.data.input.InputEntityReader}
 * or {@link InputRowParser}. This class will be useful until we remove the deprecated {@link InputRowParser}.
 */
class StreamChunkReader<RecordType extends ByteEntity>
{
  private final SettableByteEntityReader<RecordType> byteEntityReader;
  private final InputRowFilter rowFilter;
  private final RowIngestionMeters rowIngestionMeters;
  private final ParseExceptionHandler parseExceptionHandler;

  /**
   * Either parser or inputFormat shouldn't be null.
   */
  StreamChunkReader(
      InputFormat inputFormat,
      InputRowSchema inputRowSchema,
      TransformSpec transformSpec,
      File indexingTmpDir,
      InputRowFilter rowFilter,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler
  )
  {
    InvalidInput.notNull(inputFormat, "inputFormat");
    this.byteEntityReader = new SettableByteEntityReader<>(
        inputFormat,
        inputRowSchema,
        transformSpec,
        indexingTmpDir
    );
    this.rowFilter = rowFilter;
    this.rowIngestionMeters = rowIngestionMeters;
    this.parseExceptionHandler = parseExceptionHandler;
  }

  @VisibleForTesting
  StreamChunkReader(
      SettableByteEntityReader<RecordType> byteEntityReader,
      InputRowFilter rowFilter,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler
  )
  {
    this.byteEntityReader = InvalidInput.notNull(byteEntityReader, "byteEntityReader");
    this.rowFilter = rowFilter;
    this.rowIngestionMeters = rowIngestionMeters;
    this.parseExceptionHandler = parseExceptionHandler;
  }

  List<InputRow> parse(@Nullable List<RecordType> streamChunk, boolean isEndOfShard) throws IOException
  {
    if (streamChunk == null || streamChunk.isEmpty()) {
      if (!isEndOfShard) {
        // We do not count end of shard record as thrown away event since this is a record created by Druid
        // Note that this only applies to Kinesis
        rowIngestionMeters.incrementThrownAway(InputRowFilterResult.NULL_OR_EMPTY_RECORD);
      }
      return Collections.emptyList();
    } else {
      final List<InputRow> rows = new ArrayList<>();
      for (RecordType valueBytes : streamChunk) {
        rowIngestionMeters.incrementProcessedBytes(valueBytes.getBuffer().remaining());
        byteEntityReader.setEntity(valueBytes);
        try (FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
            byteEntityReader.read(),
            rowFilter,
            rowIngestionMeters,
            parseExceptionHandler
        )) {
          rowIterator.forEachRemaining(rows::add);
        }
        catch (ParseException pe) {
          parseExceptionHandler.handle(pe);
        }
      }
      return rows;
    }
  }
}
