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

package org.apache.druid.data.input.protobuf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.commons.io.IOUtils;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.DynamicConfigProviderUtils;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

public class SchemaRegistryBasedProtobufBytesDecoderTest
{
  private SchemaRegistryClient registry;

  @Before
  public void setUp()
  {
    registry = Mockito.mock(CachedSchemaRegistryClient.class);
  }

  @Test
  public void testParse() throws Exception
  {

    Mockito.when(registry.getSchemaById(ArgumentMatchers.eq(1234))).thenReturn(parseProtobufSchema());
    ProtoTestEventWrapper.ProtoTestEvent event = getTestEvent();
    byte[] bytes = event.toByteArray();
    ByteBuffer bb = ByteBuffer.allocate(bytes.length + 6).put((byte) 0).putInt(1234).put((byte) 0).put(bytes);
    bb.rewind();
    // When
    DynamicMessage actual = new SchemaRegistryBasedProtobufBytesDecoder(registry).parse(bb);
    // Then
    Assert.assertEquals(actual.getField(actual.getDescriptorForType().findFieldByName("id")), event.getId());
  }

  @Test(expected = ParseException.class)
  public void testParseCorrupted() throws Exception
  {
    Mockito.when(registry.getSchemaById(ArgumentMatchers.eq(1234))).thenReturn(parseProtobufSchema());
    byte[] bytes = getTestEvent().toByteArray();
    ByteBuffer bb = ByteBuffer.allocate(bytes.length + 6).put((byte) 0).putInt(1234).put((bytes), 5, 10);
    bb.rewind();
    // When
    new SchemaRegistryBasedProtobufBytesDecoder(registry).parse(bb);
  }

  @Test(expected = ParseException.class)
  public void testParseWrongId() throws Exception
  {
    // Given
    Mockito.when(registry.getSchemaById(ArgumentMatchers.anyInt())).thenThrow(new IOException("no pasaran"));
    byte[] bytes = getTestEvent().toByteArray();
    ByteBuffer bb = ByteBuffer.allocate(bytes.length + 6).put((byte) 0).putInt(1234).put((byte) 0).put(bytes);
    bb.rewind();
    // When
    new SchemaRegistryBasedProtobufBytesDecoder(registry).parse(bb);
  }

  @Test
  public void testDefaultCapacity()
  {
    // Given
    SchemaRegistryBasedProtobufBytesDecoder schemaRegistryBasedProtobufBytesDecoder = new SchemaRegistryBasedProtobufBytesDecoder("http://test", null, null, null, null, null);
    // When
    Assert.assertEquals(schemaRegistryBasedProtobufBytesDecoder.getIdentityMapCapacity(), Integer.MAX_VALUE);
  }

  @Test
  public void testGivenCapacity()
  {
    int capacity = 100;
    // Given
    SchemaRegistryBasedProtobufBytesDecoder schemaRegistryBasedProtobufBytesDecoder = new SchemaRegistryBasedProtobufBytesDecoder("http://test", capacity, null, null, null, null);
    // When
    Assert.assertEquals(schemaRegistryBasedProtobufBytesDecoder.getIdentityMapCapacity(), capacity);
  }

  private ProtoTestEventWrapper.ProtoTestEvent getTestEvent()
  {
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildFlatData(dateTime);
    return event;
  }


  @Test
  public void testMultipleUrls() throws Exception
  {
    String json = "{\"urls\":[\"http://localhost\"],\"type\": \"schema_registry\"}";
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper())
    );
    SchemaRegistryBasedProtobufBytesDecoder decoder;
    decoder = (SchemaRegistryBasedProtobufBytesDecoder) mapper
        .readerFor(ProtobufBytesDecoder.class)
        .readValue(json);

    // Then
    Assert.assertNotEquals(decoder.hashCode(), 0);
  }

  @Test
  public void testUrl() throws Exception
  {
    String json = "{\"url\":\"http://localhost\",\"type\": \"schema_registry\"}";
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper())
    );
    SchemaRegistryBasedProtobufBytesDecoder decoder;
    decoder = (SchemaRegistryBasedProtobufBytesDecoder) mapper
        .readerFor(ProtobufBytesDecoder.class)
        .readValue(json);

    // Then
    Assert.assertNotEquals(decoder.hashCode(), 0);
  }

  @Test
  public void testConfig() throws Exception
  {
    String json = "{\"url\":\"http://localhost\",\"type\": \"schema_registry\", \"config\":{}}";
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper())
    );
    SchemaRegistryBasedProtobufBytesDecoder decoder;
    decoder = (SchemaRegistryBasedProtobufBytesDecoder) mapper
        .readerFor(ProtobufBytesDecoder.class)
        .readValue(json);

    // Then
    Assert.assertNotEquals(decoder.hashCode(), 0);
  }

  @Test
  public void testParseHeader() throws JsonProcessingException
  {
    String json = "{\"url\":\"http://localhost\",\"type\":\"schema_registry\",\"config\":{},\"headers\":{\"druid.dynamic.config.provider\":{\"type\":\"mapString\", \"config\":{\"registry.header.prop.2\":\"value.2\", \"registry.header.prop.3\":\"value.3\"}},\"registry.header.prop.1\":\"value.1\",\"registry.header.prop.2\":\"value.4\"}}";
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper())
    );
    SchemaRegistryBasedProtobufBytesDecoder decoder;
    decoder = (SchemaRegistryBasedProtobufBytesDecoder) mapper
        .readerFor(ProtobufBytesDecoder.class)
        .readValue(json);

    Map<String, String> header = DynamicConfigProviderUtils.extraConfigAndSetStringMap(decoder.getHeaders(), SchemaRegistryBasedProtobufBytesDecoder.DRUID_DYNAMIC_CONFIG_PROVIDER_KEY, new DefaultObjectMapper());

    // Then
    Assert.assertEquals(3, header.size());
    Assert.assertEquals("value.1", header.get("registry.header.prop.1"));
    Assert.assertEquals("value.2", header.get("registry.header.prop.2"));
    Assert.assertEquals("value.3", header.get("registry.header.prop.3"));
  }

  @Test
  public void testParseConfig() throws JsonProcessingException
  {
    String json = "{\"url\":\"http://localhost\",\"type\":\"schema_registry\",\"config\":{\"druid.dynamic.config.provider\":{\"type\":\"mapString\", \"config\":{\"registry.config.prop.2\":\"value.2\", \"registry.config.prop.3\":\"value.3\"}},\"registry.config.prop.1\":\"value.1\",\"registry.config.prop.2\":\"value.4\"},\"headers\":{}}";
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper())
    );
    SchemaRegistryBasedProtobufBytesDecoder decoder;
    decoder = (SchemaRegistryBasedProtobufBytesDecoder) mapper
        .readerFor(ProtobufBytesDecoder.class)
        .readValue(json);

    Map<String, ?> heaeder = DynamicConfigProviderUtils.extraConfigAndSetObjectMap(decoder.getConfig(), SchemaRegistryBasedProtobufBytesDecoder.DRUID_DYNAMIC_CONFIG_PROVIDER_KEY, new DefaultObjectMapper());

    // Then
    Assert.assertEquals(3, heaeder.size());
    Assert.assertEquals("value.1", heaeder.get("registry.config.prop.1"));
    Assert.assertEquals("value.2", heaeder.get("registry.config.prop.2"));
    Assert.assertEquals("value.3", heaeder.get("registry.config.prop.3"));
  }

  private ProtobufSchema parseProtobufSchema() throws IOException
  {
    // Given
    InputStream fin;
    fin = this.getClass().getClassLoader().getResourceAsStream("ProtoTest.proto");
    String protobufString = IOUtils.toString(fin, StandardCharsets.UTF_8);

    fin = this.getClass().getClassLoader().getResourceAsStream("google/protobuf/timestamp.proto");
    String timestampProtobufString = IOUtils.toString(fin, StandardCharsets.UTF_8);
    return new ProtobufSchema(protobufString, Collections.emptyList(),
        ImmutableMap.of("google/protobuf/timestamp.proto", timestampProtobufString), null, null);
  }
}
