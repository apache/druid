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

package org.apache.druid.frame.wire;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.Frame;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.WireTransferable;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A {@link WireTransferable} implementation for frames, used to serialize
 * {@link RowBasedFrameRowsAndColumns} and {@link ColumnBasedFrameRowsAndColumns}.
 *
 * Signature-included format is:
 * 1) 0x05 (length of type code)
 * 2) "frame" (type code)
 * 3) flags byte
 * 4) signature length, if signature is present (little-endian int)
 * 5) signature bytes, encoded with object mapper, if signature is present
 * 6) frame bytes
 */
public class FrameWireTransferable implements WireTransferable
{
  public static final String TYPE = "frame";
  public static final byte[] TYPE_BYTES = StringUtils.toUtf8(TYPE);
  private static final byte HAS_SIGNATURE = 0x01;

  private final Frame frame;
  @Nullable
  private final RowSignature signature;

  public FrameWireTransferable(Frame frame, @Nullable RowSignature signature)
  {
    this.frame = frame;
    this.signature = signature;
  }

  @Override
  public ByteArrayOffsetAndLen serializedBytes(ObjectMapper mapper) throws IOException
  {
    final byte[] signatureBytes;

    if (signature != null) {
      signatureBytes = mapper.writeValueAsBytes(signature);
    } else {
      signatureBytes = null;
    }

    final int serializedLength = Ints.checkedCast(
        1 // type code length
        + TYPE_BYTES.length // type code
        + 1 // flags
        + (signatureBytes != null ? Integer.BYTES + signatureBytes.length : 0) // signature
        + frame.numBytes() // frame data
    );

    final ByteBuffer serializedBytes = ByteBuffer.allocate(serializedLength).order(ByteOrder.LITTLE_ENDIAN);

    // Write type code length (1 byte) + type code
    serializedBytes.put((byte) TYPE_BYTES.length);
    serializedBytes.put(TYPE_BYTES);

    // Write flags
    serializedBytes.put(signature != null ? HAS_SIGNATURE : 0x00);

    // Write signature length (4 bytes, little-endian) + signature, if present
    if (signature != null) {
      serializedBytes.putInt(signatureBytes.length);
      serializedBytes.put(signatureBytes);
    }

    // Write frame bytes
    frame.readableMemory()
         .getByteArray(0, serializedBytes.array(), serializedBytes.position(), Ints.checkedCast(frame.numBytes()));

    return new ByteArrayOffsetAndLen(serializedBytes.array(), 0, serializedLength);
  }

  private static RowsAndColumns deserializeFrame(ObjectMapper mapper, ByteBuffer theBytes)
  {
    // Skip the type code.
    final int typeCodeLen = 0xFF & theBytes.get();
    theBytes.position(theBytes.position() + typeCodeLen);

    // Read flags.
    final byte flags = theBytes.get();
    final boolean hasSignature = (flags & HAS_SIGNATURE) != 0;

    final RowSignature signature;
    if (hasSignature) {
      // Read signature.
      final int signatureLen = theBytes.getInt();
      final byte[] signatureBytes = new byte[signatureLen];
      theBytes.get(signatureBytes);

      try {
        signature = mapper.readValue(signatureBytes, RowSignature.class);
      }
      catch (IOException e) {
        throw new RE(e, "Failed to deserialize RowSignature");
      }
    } else {
      signature = null;
    }

    // Read remaining bytes as a Frame.
    final Frame frame = Frame.wrap(Memory.wrap(theBytes, ByteOrder.LITTLE_ENDIAN)
                                         .region(theBytes.position(), theBytes.remaining()));

    if (frame.type().isRowBased()) {
      return new RowBasedFrameRowsAndColumns(frame, signature);
    } else {
      return new ColumnBasedFrameRowsAndColumns(frame, signature);
    }
  }

  /**
   * Deserializer for frames.
   */
  public static class Deserializer implements WireTransferable.Deserializer
  {
    @Override
    public RowsAndColumns deserialize(ObjectMapper mapper, ByteBuffer theBytes)
    {
      return deserializeFrame(mapper, theBytes);
    }
  }
}
