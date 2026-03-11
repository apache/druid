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

package org.apache.druid.query.rowsandcols.semantic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.DruidSecondaryModule;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A Semantic interface that enables serializing a {@link RowsAndColumns} over the wire.
 * <p>
 * Serialization and Deserialization are a weird beast.  It is very easy to follow an Object-Oriented mechanism for
 * serializing out an object, but when you want to deserialize, it is unclear exactly which class should be
 * deserialized.  This requires extra metadata that helps point the deserializing code to how to interpret the
 * bytes.
 * <p>
 * The {@link WireTransferable} works by serializing the object as a singular byte array, the first byte of the byte array
 * is interpretted as an unsigned integer (N) and represents the length of the "extra metadata" to identify the correct
 * deserializer.  Those N bytes are read as a utf8 String and used to look up the deserialization logic.  This
 * identifier must be registered with {@link DruidSecondaryModule#getWireTransferableDeserializerBinder}, which will
 * use the identifier to look up the logic and deserialize the RowsAndColumns object.
 * <p>
 * There is, intentionally, no default implementation of {@link WireTransferable} that just wraps a RowsAndColumns,
 * this is because the point at which data is transfered over the wire tends to be a very sensitive point in terms of
 * what the best format for transmission is and what optimizations are available.  As such, we don't want a concrete
 * class that makes its way to the wire to accidentally get a least-common-denominator serialization and instead want
 * it to be explicitly chosen.
 */
public interface WireTransferable
{
  /**
   * Serializes the object into bytes.  This method is passed a jackson ObjectMapper that can be used to serialize
   * objects if needed.  In general, it's better to generate binary serializations and not rely on the mapper
   * but it's there as an expediency layer to support some compatibility stuff.  Hopefully it goes away at some point.
   *
   * @param mapper ObjectMapper
   * @return bytes that represent the wire-format of this object
   * @throws IOException a problem with IO
   */
  ByteArrayOffsetAndLen serializedBytes(ObjectMapper mapper) throws IOException;

  interface Deserializer
  {
    /**
     * Deserializes a ByteBuffer into a RowsAndColumns. The deserialized object owns the passed-in buffer as well
     * as the data it points to. It is safe for the deserializer to mutate the buffer, retain a reference to it, etc.
     *
     * @param mapper ObjectMapper
     * @param theBytes ByteBuffer to deserialize, in little-endian order
     * @return RowsAndColumns object deserialized from the ByteBuffer
     */
    RowsAndColumns deserialize(ObjectMapper mapper, ByteBuffer theBytes);
  }

  /**
   * A holder class for a byte array, offset and length, this exists to minimize copies.  It's pretty similar to a
   * ByteBuffer, but is separate because jackson cannot write a ByteBuffer anyway so might as well wrap in this.
   */
  class ByteArrayOffsetAndLen
  {
    private final byte[] array;
    private final int offset;
    private final int length;

    public ByteArrayOffsetAndLen(byte[] array, int offset, int length)
    {
      this.array = array;
      this.offset = offset;
      this.length = length;
    }

    public byte[] getArray()
    {
      return array;
    }

    public int getOffset()
    {
      return offset;
    }

    public int getLength()
    {
      return length;
    }

    public ByteBuffer asByteBuffer()
    {
      return ByteBufferUtils.wrapLE(array, offset, length);
    }
  }

  class ConcreteDeserializer
  {
    private final ObjectMapper mapper;
    private final Map<ByteBuffer, Deserializer> delegates;

    public ConcreteDeserializer(ObjectMapper mapper, Map<ByteBuffer, Deserializer> delegates)
    {
      this.mapper = mapper;
      this.delegates = delegates;
    }

    /**
     * Deserializes a ByteBuffer into a RowsAndColumns. The deserialized object owns the passed-in buffer as well
     * as the data it points to. It is safe for the deserializer to mutate the buffer, retain a reference to it, etc.
     *
     * @param theBytes ByteBuffer to be deserialized, in little-endian order
     * @return RowsAndColumns object deserialized from the ByteBuffer
     */
    public RowsAndColumns deserialize(ByteBuffer theBytes)
    {
      int originalPos = theBytes.position();
      int originalLimit = theBytes.limit();

      int identifierLen = 0xff & theBytes.get();
      theBytes.limit(theBytes.position() + identifierLen);
      final Deserializer deserializer = delegates.get(theBytes);
      if (deserializer == null) {
        throw DruidException
            .forPersona(DruidException.Persona.OPERATOR)
            .ofCategory(DruidException.Category.RUNTIME_FAILURE)
            .build(
                "Got unhandled RAC blob type [%s], either an extension is missing or code is not wired correctly.",
                StringUtils.fromUtf8Nullable(theBytes)
            );
      }
      theBytes.position(originalPos);
      theBytes.limit(originalLimit);
      return deserializer.deserialize(mapper, theBytes);
    }
  }
}
