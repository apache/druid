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
import org.apache.druid.query.aggregation.bloom.BloomFilterAggregatorFactory;
import org.apache.druid.query.aggregation.bloom.BloomFilterSerde;
import org.apache.druid.query.filter.BloomDimFilter;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.filter.BloomKFilterHolder;
import org.apache.druid.segment.serde.ComplexMetrics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BloomFilterSerializersModule extends SimpleModule
{
  public static final String BLOOM_FILTER_TYPE_NAME = "bloom";

  public BloomFilterSerializersModule()
  {
    registerSubtypes(
        new NamedType(BloomDimFilter.class, BLOOM_FILTER_TYPE_NAME),
        new NamedType(BloomFilterAggregatorFactory.class, BLOOM_FILTER_TYPE_NAME)
    );
    addSerializer(BloomKFilter.class, new BloomKFilterSerializer());
    addDeserializer(BloomKFilter.class, new BloomKFilterDeserializer());
    addDeserializer(BloomKFilterHolder.class, new BloomKFilterHolderDeserializer());

    ComplexMetrics.registerSerde(BLOOM_FILTER_TYPE_NAME, new BloomFilterSerde());
  }

  private static class BloomKFilterSerializer extends StdSerializer<BloomKFilter>
  {
    BloomKFilterSerializer()
    {
      super(BloomKFilter.class);
    }

    @Override
    public void serialize(BloomKFilter bloomKFilter, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException
    {
      jsonGenerator.writeBinary(bloomKFilterToBytes(bloomKFilter));
    }
  }

  private static class BloomKFilterDeserializer extends StdDeserializer<BloomKFilter>
  {
    BloomKFilterDeserializer()
    {
      super(BloomKFilter.class);
    }

    @Override
    public BloomKFilter deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException
    {
      return bloomKFilterFromBytes(jsonParser.getBinaryValue());
    }
  }

  private static class BloomKFilterHolderDeserializer extends StdDeserializer<BloomKFilterHolder>
  {
    BloomKFilterHolderDeserializer()
    {
      super(BloomKFilterHolder.class);
    }

    @Override
    public BloomKFilterHolder deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException
    {
      return BloomKFilterHolder.fromBytes(jsonParser.getBinaryValue());
    }
  }

  public static byte[] bloomKFilterToBytes(BloomKFilter bloomKFilter) throws IOException
  {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    BloomKFilter.serialize(byteArrayOutputStream, bloomKFilter);
    return byteArrayOutputStream.toByteArray();
  }

  public static BloomKFilter bloomKFilterFromBytes(byte[] bytes) throws IOException
  {
    return BloomKFilter.deserialize(new ByteArrayInputStream(bytes));
  }
}
