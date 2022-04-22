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

package org.apache.druid.common.jackson;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class JacksonUtilsTest
{
  @Test
  public void testWriteObjectUsingSerializerProvider() throws IOException
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    final ObjectMapper objectMapper = new ObjectMapper();
    final SerializerProvider serializers = objectMapper.getSerializerProviderInstance();

    final JsonGenerator jg = objectMapper.getFactory().createGenerator(baos);
    jg.writeStartArray();
    JacksonUtils.writeObjectUsingSerializerProvider(jg, serializers, new SerializableClass(2));
    JacksonUtils.writeObjectUsingSerializerProvider(jg, serializers, null);
    JacksonUtils.writeObjectUsingSerializerProvider(jg, serializers, new SerializableClass(3));
    jg.writeEndArray();
    jg.close();

    final List<SerializableClass> deserializedValues = objectMapper.readValue(
        baos.toByteArray(),
        new TypeReference<List<SerializableClass>>() {}
    );

    Assert.assertEquals(
        Arrays.asList(new SerializableClass(2), null, new SerializableClass(3)),
        deserializedValues
    );
  }

  @Test
  public void testWritePrimitivesUsingSerializerProvider() throws IOException
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    final ObjectMapper objectMapper = new ObjectMapper();
    final SerializerProvider serializers = objectMapper.getSerializerProviderInstance();

    final JsonGenerator jg = objectMapper.getFactory().createGenerator(baos);
    jg.writeStartArray();
    JacksonUtils.writeObjectUsingSerializerProvider(jg, serializers, "foo");
    JacksonUtils.writeObjectUsingSerializerProvider(jg, serializers, null);
    JacksonUtils.writeObjectUsingSerializerProvider(jg, serializers, 1.23);
    jg.writeEndArray();
    jg.close();

    final List<Object> deserializedValues = objectMapper.readValue(
        baos.toByteArray(),
        new TypeReference<List<Object>>() {}
    );

    Assert.assertEquals(
        Arrays.asList("foo", null, 1.23),
        deserializedValues
    );
  }

  public static class SerializableClass
  {
    private final int value;

    @JsonCreator
    public SerializableClass(@JsonProperty("value") final int value)
    {
      this.value = value;
    }

    @JsonProperty
    public int getValue()
    {
      return value;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SerializableClass that = (SerializableClass) o;
      return value == that.value;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(value);
    }

    @Override
    public String toString()
    {
      return "SerializableClass{" +
             "value=" + value +
             '}';
    }
  }
}
