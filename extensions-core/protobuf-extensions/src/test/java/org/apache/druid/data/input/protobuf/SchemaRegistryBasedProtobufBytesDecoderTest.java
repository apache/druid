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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
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
    // Given
    InputStream fin;
    fin = this.getClass().getClassLoader().getResourceAsStream("ProtoTest.proto");
    String protobufString = IOUtils.toString(fin, StandardCharsets.UTF_8);
    Mockito.when(registry.getSchemaById(ArgumentMatchers.eq(1234))).thenReturn(new ProtobufSchema(protobufString));
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
    // Given
    InputStream fin;
    fin = this.getClass().getClassLoader().getResourceAsStream("ProtoTest.proto");
    String protobufString = IOUtils.toString(fin, StandardCharsets.UTF_8);
    Mockito.when(registry.getSchemaById(ArgumentMatchers.eq(1234))).thenReturn(new ProtobufSchema(protobufString));
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
    SchemaRegistryBasedProtobufBytesDecoder schemaRegistryBasedProtobufBytesDecoder = new SchemaRegistryBasedProtobufBytesDecoder("http://test", null, null, null, null);
    // When
    Assert.assertEquals(schemaRegistryBasedProtobufBytesDecoder.getIdentityMapCapacity(), Integer.MAX_VALUE);
  }

  @Test
  public void testGivenCapacity()
  {
    int capacity = 100;
    // Given
    SchemaRegistryBasedProtobufBytesDecoder schemaRegistryBasedProtobufBytesDecoder = new SchemaRegistryBasedProtobufBytesDecoder("http://test", capacity, null, null, null);
    // When
    Assert.assertEquals(schemaRegistryBasedProtobufBytesDecoder.getIdentityMapCapacity(), capacity);
  }

  private ProtoTestEventWrapper.ProtoTestEvent getTestEvent()
  {
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtoTestEventWrapper.ProtoTestEvent.newBuilder()
        .setDescription("description")
        .setEventType(ProtoTestEventWrapper.ProtoTestEvent.EventCategory.CATEGORY_ONE)
        .setId(4711L)
        .setIsValid(true)
        .setSomeOtherId(4712)
        .setTimestamp(dateTime.toString())
        .setSomeFloatColumn(47.11F)
        .setSomeIntColumn(815)
        .setSomeLongColumn(816L)
        .build();
    return event;
  }


  @Test
  public void testMultipleUrls() throws Exception
  {
    String json = "{\"urls\":[\"http://localhost\"],\"type\": \"schema_registry\"}";
    ObjectMapper mapper = new ObjectMapper();
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
    ObjectMapper mapper = new ObjectMapper();
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
    ObjectMapper mapper = new ObjectMapper();
    SchemaRegistryBasedProtobufBytesDecoder decoder;
    decoder = (SchemaRegistryBasedProtobufBytesDecoder) mapper
        .readerFor(ProtobufBytesDecoder.class)
        .readValue(json);

    // Then
    Assert.assertNotEquals(decoder.hashCode(), 0);
  }
}
