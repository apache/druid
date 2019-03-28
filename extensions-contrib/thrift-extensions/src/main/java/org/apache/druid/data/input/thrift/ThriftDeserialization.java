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

package org.apache.druid.data.input.thrift;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftDeserialization
{
  private static final Logger log = LoggerFactory.getLogger(ThriftDeserialization.class);


  private static final ThreadLocal<TDeserializer> DESERIALIZER_COMPACT = new ThreadLocal<TDeserializer>()
  {
    @Override
    protected TDeserializer initialValue()
    {
      return new TDeserializer(new TCompactProtocol.Factory());
    }
  };

  private static final ThreadLocal<TDeserializer> DESERIALIZER_BINARY = new ThreadLocal<TDeserializer>()
  {
    @Override
    protected TDeserializer initialValue()
    {
      return new TDeserializer(new TBinaryProtocol.Factory());
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

  public static final ThreadLocal<TSerializer> SERIALIZER_SIMPLE_JSON = new ThreadLocal<TSerializer>()
  {
    @Override
    protected TSerializer initialValue()
    {
      return new TSerializer(new TSimpleJSONProtocol.Factory());
    }
  };


  private static final byte[] EMPTY_BYTES = new byte[0];

  public static byte[] decodeB64IfNeeded(final byte[] src)
  {
    Preconditions.checkNotNull(src, "src bytes cannot be null");
    if (src.length <= 0) {
      return EMPTY_BYTES;
    }
    final byte last = src[src.length - 1];
    return (0 == last || '}' == last) ? src : StringUtils.decodeBase64(src);
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
  public static <T extends TBase> T detectAndDeserialize(final byte[] bytes, final T thriftObj) throws TException
  {
    Preconditions.checkNotNull(thriftObj);
    try {
      final byte[] src = decodeB64IfNeeded(bytes);
      final TProtocolFactory protocolFactory = TProtocolUtil.guessProtocolFactory(src, null);
      Preconditions.checkNotNull(protocolFactory);
      if (protocolFactory instanceof TCompactProtocol.Factory) {
        DESERIALIZER_COMPACT.get().deserialize(thriftObj, src);
      } else if (protocolFactory instanceof TBinaryProtocol.Factory) {
        DESERIALIZER_BINARY.get().deserialize(thriftObj, src);
      } else {
        DESERIALIZER_JSON.get().deserialize(thriftObj, src);
      }
    }
    catch (final IllegalArgumentException e) {
      throw new TException(e);
    }
    return thriftObj;
  }
}
