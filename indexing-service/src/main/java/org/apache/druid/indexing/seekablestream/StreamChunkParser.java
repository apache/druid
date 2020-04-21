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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstraction for parsing stream data which internally uses {@link org.apache.druid.data.input.InputEntityReader}
 * or {@link InputRowParser}. This class will be useful untill we remove the deprecated InputRowParser.
 */
class StreamChunkParser
{
  @Nullable
  private final InputRowParser<ByteBuffer> parser;
  private final SettableByteEntityReader byteEntityReader;

  StreamChunkParser(@Nullable InputRowParser<ByteBuffer> parser, SettableByteEntityReader byteEntityReader)
  {
    this.parser = parser;
    this.byteEntityReader = byteEntityReader;
  }

  List<InputRow> parse(List<byte[]> streamChunk) throws IOException
  {
    if (parser != null) {
      return parseWithParser(parser, streamChunk);
    } else {
      return parseWithInputFormat(byteEntityReader, streamChunk);
    }
  }

  private static List<InputRow> parseWithParser(InputRowParser<ByteBuffer> parser, List<byte[]> valueBytess)
  {
    final List<InputRow> rows = new ArrayList<>();
    for (byte[] valueBytes : valueBytess) {
      rows.addAll(parser.parseBatch(ByteBuffer.wrap(valueBytes)));
    }
    return rows;
  }

  private static List<InputRow> parseWithInputFormat(
      SettableByteEntityReader byteEntityReader,
      List<byte[]> valueBytess
  ) throws IOException
  {
    final List<InputRow> rows = new ArrayList<>();
    for (byte[] valueBytes : valueBytess) {
      byteEntityReader.setEntity(new ByteEntity(valueBytes));
      try (CloseableIterator<InputRow> rowIterator = byteEntityReader.read()) {
        rowIterator.forEachRemaining(rows::add);
      }
    }
    return rows;
  }
}
