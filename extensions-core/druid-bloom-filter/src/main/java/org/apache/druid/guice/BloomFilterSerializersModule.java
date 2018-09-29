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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.druid.query.filter.BloomDimFilter;
import org.apache.hive.common.util.BloomKFilter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BloomFilterSerializersModule extends SimpleModule
{
  public static String BLOOM_FILTER_TYPE_NAME = "bloom";

  public BloomFilterSerializersModule()
  {
    registerSubtypes(
        new NamedType(BloomDimFilter.class, BLOOM_FILTER_TYPE_NAME)
    );
    addSerializer(BloomKFilter.class, new BloomKFilterSerializer());
    addDeserializer(BloomKFilter.class, new BloomKFilterDeserializer());
  }

  public static class BloomKFilterSerializer extends StdSerializer<BloomKFilter>
  {

    public BloomKFilterSerializer()
    {
      super(BloomKFilter.class);
    }

    @Override
    public void serialize(
        BloomKFilter bloomKFilter, JsonGenerator jsonGenerator, SerializerProvider serializerProvider
    ) throws IOException
    {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      BloomKFilter.serialize(byteArrayOutputStream, bloomKFilter);
      byte[] bytes = byteArrayOutputStream.toByteArray();
      jsonGenerator.writeBinary(bytes);
    }
  }

  public static class BloomKFilterDeserializer extends StdDeserializer<BloomKFilter>
  {

    protected BloomKFilterDeserializer()
    {
      super(BloomKFilter.class);
    }

    @Override
    public BloomKFilter deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext
    ) throws IOException, JsonProcessingException
    {
      byte[] bytes = jsonParser.getBinaryValue();
      return BloomKFilter.deserialize(new ByteArrayInputStream(bytes));

    }
  }
}
