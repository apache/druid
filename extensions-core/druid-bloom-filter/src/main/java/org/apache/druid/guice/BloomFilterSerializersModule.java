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
package org.apache.druid.guice;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.druid.query.filter.BloomDimFilter;
import org.apache.druid.query.filter.BloomFilterHolder;
import org.apache.hive.common.util.BloomFilter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BloomFilterSerializersModule extends SimpleModule
{
  public static String BLOOM_FILTER_TYPE_NAME = "bloom";

  public BloomFilterSerializersModule()
  {
    registerSubtypes(new NamedType(BloomDimFilter.class, BLOOM_FILTER_TYPE_NAME));
    addSerializer(BloomFilter.class, new BloomFilterSerializer());
    addDeserializer(BloomFilter.class, new BloomFilterDeserializer());
    addDeserializer(BloomFilterHolder.class, new BloomFilterHolderDeserializer());
  }

  private static class BloomFilterSerializer extends StdSerializer<BloomFilter>
  {
    BloomFilterSerializer()
    {
      super(BloomFilter.class);
    }

    @Override
    public void serialize(BloomFilter bloomFilter, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException
    {
      jsonGenerator.writeBinary(bloomFilterToBytes(bloomFilter));
    }
  }

  private static class BloomFilterDeserializer extends StdDeserializer<BloomFilter>
  {
    BloomFilterDeserializer()
    {
      super(BloomFilter.class);
    }

    @Override
    public BloomFilter deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException
    {
      return bloomFilterFromBytes(jsonParser.getBinaryValue());
    }
  }

  private static class BloomFilterHolderDeserializer extends StdDeserializer<BloomFilterHolder>
  {
    BloomFilterHolderDeserializer()
    {
      super(BloomFilterHolder.class);
    }

    @Override
    public BloomFilterHolder deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException
    {
      byte[] bytes = jsonParser.getBinaryValue();
      return BloomFilterHolder.fromBytes(bytes);
    }
  }

  public static byte[] bloomFilterToBytes(BloomFilter bloomFilter) throws IOException
  {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    BloomFilter.serialize(byteArrayOutputStream, bloomFilter);
    return byteArrayOutputStream.toByteArray();
  }

  public static BloomFilter bloomFilterFromBytes(byte[] bytes) throws IOException
  {
    return BloomFilter.deserialize(new ByteArrayInputStream(bytes));
  }
}
