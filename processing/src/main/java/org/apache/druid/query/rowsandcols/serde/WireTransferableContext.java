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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.WireTransferable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Object that holds the components and configuration needed for serde of {@link RowsAndColumns}
 * via {@link WireTransferable}.
 */
public class WireTransferableContext
{
  public static final boolean DEFAULT_LEGACY_FRAME_SERIALIZATION = true;

  private final ObjectMapper smileMapper;
  private final WireTransferable.ConcreteDeserializer concreteDeserializer;
  private final boolean useLegacyFrameSerialization;

  public WireTransferableContext(
      final ObjectMapper smileMapper,
      final WireTransferable.ConcreteDeserializer concreteDeserializer,
      final boolean useLegacyFrameSerialization
  )
  {
    this.smileMapper = smileMapper;
    this.concreteDeserializer = concreteDeserializer;
    this.useLegacyFrameSerialization = useLegacyFrameSerialization;
  }

  /**
   * Serializes an object into bytes.
   *
   * @return bytes that represent the wire-format of this object
   *
   * @throws IOException a problem with IO
   */
  public WireTransferable.ByteArrayOffsetAndLen serializedBytes(WireTransferable wireTransferable)
      throws IOException
  {
    return wireTransferable.serializedBytes(smileMapper);
  }

  /**
   * Deserializes a ByteBuffer into a RowsAndColumns. The deserialized object owns the passed-in buffer as well
   * as the data it points to. It is safe for the deserializer to mutate the buffer, retain a reference to it, etc.
   *
   * @param theBytes ByteBuffer to be deserialized, in little-endian order
   *
   * @return RowsAndColumns object deserialized from the ByteBuffer
   */
  public RowsAndColumns deserialize(ByteBuffer theBytes)
  {
    if (concreteDeserializer == null) {
      throw DruidException.defensive("Cannot deserialize, no concreteDeserializer");
    }
    return concreteDeserializer.deserialize(theBytes);
  }

  /**
   * Returns the Smile ObjectMapper for serialization, or null if not available.
   */
  public ObjectMapper smileMapper()
  {
    return smileMapper;
  }

  /**
   * Returns the deserializer for RowsAndColumns wire format, or null if not available.
   */
  public WireTransferable.ConcreteDeserializer concreteDeserializer()
  {
    return concreteDeserializer;
  }

  /**
   * Returns true if legacy frame serialization formats should be used in {@link RowsAndColumnsSerializer} and
   * {@link FrameFileWriter}. Returns false if the {@link WireTransferable} mechanism should be used.
   */
  public boolean useLegacyFrameSerialization()
  {
    return useLegacyFrameSerialization;
  }
}
