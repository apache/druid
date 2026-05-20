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

package org.apache.druid.query.rowsandcols.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.frame.Frame;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.WireTransferable;
import org.apache.druid.segment.column.RowSignature;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Deserializer for {@link RowsAndColumns} following the contract set out in {@link WireTransferable}.
 *
 * There is some legacy code in {@link #deserializeLegacyFrame(JsonParser)} that handles deserializing
 * {@link FrameRowsAndColumns} specifically as a signature object followed by frame bytes. This code is
 * for compatibility with the format written by {@link RowsAndColumnsSerializer} when
 * {@link WireTransferableContext#useLegacyFrameSerialization()} is true.
 */
public class RowsAndColumnsDeserializer extends StdDeserializer<RowsAndColumns>
{
  private final WireTransferableContext wtContext;

  public RowsAndColumnsDeserializer(WireTransferableContext wtContext)
  {
    super(RowsAndColumns.class);
    this.wtContext = wtContext;
  }

  @Override
  public RowsAndColumns deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException
  {
    if (jsonParser.currentToken().isScalarValue()) {
      final byte[] bytes = jsonParser.readValueAs(byte[].class);
      return wtContext.deserialize(ByteBufferUtils.wrapLE(bytes));
    } else {
      return deserializeLegacyFrame(jsonParser);
    }
  }

  /**
   * Deserializes the format written by {@link RowsAndColumnsSerializer} when
   * {@link WireTransferableContext#useLegacyFrameSerialization()} is true.
   */
  private RowsAndColumns deserializeLegacyFrame(JsonParser jsonParser) throws IOException
  {
    final RowSignature sig;
    try {
      sig = jsonParser.readValueAs(RowSignature.class);
    }
    catch (MismatchedInputException e) {
      throw jsonParser.readValueAs(ErrorResponse.class).getUnderlyingException();
    }
    jsonParser.nextValue();

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    jsonParser.readBinaryValue(baos);
    final Frame frame = Frame.wrap(baos.toByteArray());
    if (frame.type().isColumnar()) {
      return new ColumnBasedFrameRowsAndColumns(frame, sig);
    } else {
      return new RowBasedFrameRowsAndColumns(frame, sig);
    }
  }
}
