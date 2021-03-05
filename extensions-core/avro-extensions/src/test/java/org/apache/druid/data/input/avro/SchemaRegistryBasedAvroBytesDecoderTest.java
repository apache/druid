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

package org.apache.druid.data.input.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.druid.data.input.AvroStreamInputRowParserTest;
import org.apache.druid.data.input.SomeAvroDatum;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SchemaRegistryBasedAvroBytesDecoderTest
{
  private SchemaRegistryClient registry;

  @Before
  public void setUp()
  {
    registry = Mockito.mock(SchemaRegistryClient.class);
  }

  @Test
  public void testMultipleUrls() throws Exception
  {
    String json = "{\"urls\":[\"http://localhost\"],\"type\": \"schema_registry\"}";
    ObjectMapper mapper = new ObjectMapper();
    SchemaRegistryBasedAvroBytesDecoder decoder;
    decoder = (SchemaRegistryBasedAvroBytesDecoder) mapper
        .readerFor(AvroBytesDecoder.class)
        .readValue(json);

    // Then
    Assert.assertNotEquals(decoder.hashCode(), 0);
  }

  @Test
  public void testUrl() throws Exception
  {
    String json = "{\"url\":\"http://localhost\",\"type\": \"schema_registry\"}";
    ObjectMapper mapper = new ObjectMapper();
    SchemaRegistryBasedAvroBytesDecoder decoder;
    decoder = (SchemaRegistryBasedAvroBytesDecoder) mapper
        .readerFor(AvroBytesDecoder.class)
        .readValue(json);

    // Then
    Assert.assertNotEquals(decoder.hashCode(), 0);
  }

  @Test
  public void testConfig() throws Exception
  {
    String json = "{\"url\":\"http://localhost\",\"type\": \"schema_registry\", \"config\":{}}";
    ObjectMapper mapper = new ObjectMapper();
    SchemaRegistryBasedAvroBytesDecoder decoder;
    decoder = (SchemaRegistryBasedAvroBytesDecoder) mapper
        .readerFor(AvroBytesDecoder.class)
        .readValue(json);

    // Then
    Assert.assertNotEquals(decoder.hashCode(), 0);
  }

  @Test
  public void testParse() throws Exception
  {
    // Given
    Mockito.when(registry.getSchemaById(ArgumentMatchers.eq(1234)))
           .thenReturn(new AvroSchema(SomeAvroDatum.getClassSchema()));
    GenericRecord someAvroDatum = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    Schema schema = SomeAvroDatum.getClassSchema();
    byte[] bytes = getAvroDatum(schema, someAvroDatum);
    ByteBuffer bb = ByteBuffer.allocate(bytes.length + 5).put((byte) 0).putInt(1234).put(bytes);
    bb.rewind();
    // When
    new SchemaRegistryBasedAvroBytesDecoder(registry).parse(bb);
  }

  @Test(expected = ParseException.class)
  public void testParseCorruptedNotEnoughBytesToEvenGetSchemaInfo()
  {
    // Given
    ByteBuffer bb = ByteBuffer.allocate(2).put((byte) 0).put(1, (byte) 1);
    bb.rewind();
    // When
    new SchemaRegistryBasedAvroBytesDecoder(registry).parse(bb);
  }

  @Test(expected = ParseException.class)
  public void testParseCorruptedPartial() throws Exception
  {
    // Given
    Mockito.when(registry.getSchemaById(ArgumentMatchers.eq(1234)))
           .thenReturn(new AvroSchema(SomeAvroDatum.getClassSchema()));
    GenericRecord someAvroDatum = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    Schema schema = SomeAvroDatum.getClassSchema();
    byte[] bytes = getAvroDatum(schema, someAvroDatum);
    ByteBuffer bb = ByteBuffer.allocate(4 + 5).put((byte) 0).putInt(1234).put(bytes, 5, 4);
    bb.rewind();
    // When
    new SchemaRegistryBasedAvroBytesDecoder(registry).parse(bb);
  }

  @Test(expected = RE.class)
  public void testParseWrongSchemaType() throws Exception
  {
    // Given
    Mockito.when(registry.getSchemaById(ArgumentMatchers.eq(1234))).thenReturn(Mockito.mock(ParsedSchema.class));
    ByteBuffer bb = ByteBuffer.allocate(5).put((byte) 0).putInt(1234);
    bb.rewind();
    // When
    new SchemaRegistryBasedAvroBytesDecoder(registry).parse(bb);
  }

  @Test(expected = RE.class)
  public void testParseWrongId() throws Exception
  {
    // Given
    Mockito.when(registry.getSchemaById(ArgumentMatchers.anyInt())).thenThrow(new IOException("no pasaran"));
    ByteBuffer bb = ByteBuffer.allocate(5).put((byte) 0).putInt(1234);
    bb.rewind();
    // When
    new SchemaRegistryBasedAvroBytesDecoder(registry).parse(bb);
  }

  private byte[] getAvroDatum(Schema schema, GenericRecord someAvroDatum) throws IOException
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
    writer.write(someAvroDatum, EncoderFactory.get().directBinaryEncoder(out, null));
    return out.toByteArray();
  }
}
