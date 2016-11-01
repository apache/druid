/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.input.thrift.util;

import io.druid.java.util.common.logger.Logger;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public final class ThriftUtils
{

  private static final Logger log = new Logger(ThriftUtils.class);

  private static final java.util.Base64.Encoder B64_ENCODER = java.util.Base64.getEncoder().withoutPadding();
  private static final java.util.Base64.Decoder B64_DECODER = java.util.Base64.getMimeDecoder();

  private static final ThreadLocal<TSerializer> SERIALIZER = new ThreadLocal<TSerializer>()
  {
    @Override
    protected TSerializer initialValue()
    {
      return new TSerializer();
    }
  };


  private static final ThreadLocal<TDeserializer> DESERIALIZER = new ThreadLocal<TDeserializer>()
  {
    @Override
    protected TDeserializer initialValue()
    {
      return new TDeserializer();
    }
  };

  private static final ThreadLocal<TDeserializer> DESERIALIZER_COMPACT = new ThreadLocal<TDeserializer>()
  {
    @Override
    protected TDeserializer initialValue()
    {
      return new TDeserializer(new TCompactProtocol.Factory());
    }
  };

  private static final ThreadLocal<TDeserializer> DESERIALIZER_JSON = new ThreadLocal<TDeserializer>()
  {
    @Override
    protected TDeserializer initialValue()
    {
      return new TDeserializer(new TJSONProtocol.Factory());
    }
  };


  private static final byte[] EMPTY_BYTES = new byte[0];

  public static byte[] decodeB64IfNeeded(final byte[] src)
  {
    if (requireNonNull(src).length <= 0) {
      return EMPTY_BYTES;
    }
    final byte last = src[src.length - 1];
    return (0 == last || '}' == last) ? src : B64_DECODER.decode(src);
  }

  /**
   * Attempt to determine the protocol used to serialize some data.
   * <p>
   * The guess algorithm is copied from TProtocolUtil.java of thrift java runtime library.
   * In some cases, no guess can be done, in that case we use TBinaryProtocol.
   * To be certain to correctly detect the protocol, the first encoded
   * field should have a field id < 256, and this is always true for our projects.
   *
   * @param data The serialized data to guess the protocol for.
   *
   * @return a deserializer with correct protocol for the current thread.
   */
  private static ThreadLocal<TDeserializer> guessProtocol(final byte[] data)
  {
    if (data.length <= 1) {
      return DESERIALIZER;
    }

    final byte first = data[0], last = data[data.length - 1];

    if ('{' == first && '}' == last) {
      return DESERIALIZER_JSON;
    }

    if (last != 0) {
      return DESERIALIZER;
    }

    if (first > 0x10) {
      return DESERIALIZER_COMPACT;
    }

    final byte second = data[1];
    if (0 == second) {
      return DESERIALIZER;
    }

    if ((second & 0x80) != 0) {
      return DESERIALIZER_COMPACT;
    }

    return DESERIALIZER; // fallback
  }

  /**
   * Deserializes byte-array into thrift object.
   * <p>
   * Supporting binary, compact and json protocols,
   * and the byte array could be or not be encoded by Base64.
   *
   * @param bytes     the byte-array to deserialize
   * @param thriftObj the output thrift object
   *
   * @return the output thrift object, or null if error occurs
   */
  public static <T extends TBase<?, ?>> T detectAndDeserialize(final byte[] bytes, final T thriftObj) throws TException
  {
    requireNonNull(thriftObj).clear();
    try {
      final byte[] src = decodeB64IfNeeded(bytes);
      guessProtocol(src).get().deserialize(thriftObj, src);
    }
    catch (final IllegalArgumentException e) {
      throw new TException(e);
    }
    return thriftObj;
  }


  /**
   * Serializes thrift object using binary protocol, and then encodes the binary using base64.
   *
   * @param thriftObj the thrift object
   *
   * @return the encoded base64 string, or null if error occurs
   */
  public static String encodeBase64String(TBase<?, ?> thriftObj)
  {
    try {
      byte[] binaryData = SERIALIZER.get().serialize(thriftObj);
      return new String(B64_ENCODER.encode(binaryData), StandardCharsets.ISO_8859_1);
    }
    catch (TException e) {
      log.warn("Error occurs when encoding thrift object, %s" + e.getMessage());
      return null;
    }
  }

  /**
   * Decode base64 string into byte-array, and then deserializes it into thrift object using binary protocol
   *
   * @param str       the base64 string to decode
   * @param thriftObj the output thrift object
   *
   * @return the decoded thrift object, or null if error occurs
   */
  @SuppressWarnings("rawtypes")
  public static <T extends TBase> T decodeBase64String(String str, T thriftObj)
  {
    try {
      byte[] binaryData = Base64.decodeBase64(str.getBytes(StandardCharsets.UTF_8));
      thriftObj.clear();
      DESERIALIZER.get().deserialize(thriftObj, binaryData);
      return thriftObj;
    }
    catch (TException e) {
      log.warn("Error occurs when decoding thrift object, %s" + e.getMessage());
      return null;
    }
  }


}
