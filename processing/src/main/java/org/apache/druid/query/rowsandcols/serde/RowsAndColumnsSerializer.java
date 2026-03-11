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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.druid.error.NotYetImplemented;
import org.apache.druid.frame.Frame;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.WireTransferable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

/**
 * Serializer for {@link RowsAndColumns} by converting the instance to {@link WireTransferable}. See the javadoc
 * on {@link WireTransferable} for information on the serde contract.
 *
 * There is a legacy serialization format for {@link FrameRowsAndColumns} that serializes the signature object
 * followed by frame bytes. This legacy format can be enabled via
 * {@link WireTransferableContext#useLegacyFrameSerialization()} for compatibility during rolling updates.
 * Once all servers have been upgraded, this can be set to false.
 */
public class RowsAndColumnsSerializer extends StdSerializer<RowsAndColumns>
{
  private final WireTransferableContext wtContext;

  public RowsAndColumnsSerializer(WireTransferableContext wtContext)
  {
    super(RowsAndColumns.class);
    this.wtContext = wtContext;
  }

  @Override
  public void serialize(
      RowsAndColumns rac,
      JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider
  ) throws IOException
  {
    // Check if we should use the legacy Frame serialization path
    if (wtContext.useLegacyFrameSerialization()) {
      final FrameRowsAndColumns frameRAC = rac.as(FrameRowsAndColumns.class);
      if (frameRAC != null) {
        serializeFrameLegacy(frameRAC, jsonGenerator, serializerProvider);
        return;
      }
    }

    // Use WireTransferable path
    final WireTransferable as = rac.as(WireTransferable.class);
    if (as != null) {
      final WireTransferable.ByteArrayOffsetAndLen serMe = wtContext.serializedBytes(as);
      jsonGenerator.writeBinary(serMe.getArray(), serMe.getOffset(), serMe.getLength());
      jsonGenerator.flush();
      return;
    }

    throw NotYetImplemented.ex(null, "RAC [%s] cannot be serialized, implement WireTransferable", rac.getClass());
  }

  /**
   * Legacy serialization format for FrameRowsAndColumns: signature object followed by frame bytes.
   * This format exists for compatibility during rolling updates and should be removed in a future release.
   */
  private static void serializeFrameLegacy(
      FrameRowsAndColumns frameRAC,
      JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider
  ) throws IOException
  {
    JacksonUtils.writeObjectUsingSerializerProvider(jsonGenerator, serializerProvider, frameRAC.getSignature());

    final Frame frame = frameRAC.getFrame();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    frame.writeTo(Channels.newChannel(baos), false, null);

    jsonGenerator.writeBinary(baos.toByteArray());
  }
}
