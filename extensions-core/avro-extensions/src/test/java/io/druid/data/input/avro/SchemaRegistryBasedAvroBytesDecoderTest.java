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

package io.druid.data.input.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.druid.data.input.AvroStreamInputRowParserTest;
import io.druid.data.input.SomeAvroDatum;
import io.druid.java.util.common.parsers.ParseException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 */
public class SchemaRegistryBasedAvroBytesDecoderTest
{

  SchemaRegistryClient registry;

  @Before
  public void setUp() throws Exception
  {
    registry = mock(SchemaRegistryClient.class);
  }

  @Test
  public void testParse() throws Exception
  {
    // Given
    when(registry.getByID(eq(1234))).thenReturn(SomeAvroDatum.getClassSchema());
    GenericRecord someAvroDatum = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    Schema schema = SomeAvroDatum.getClassSchema();
    byte[] bytes = getAvroDatum(schema, someAvroDatum);
    ByteBuffer bb = ByteBuffer.allocate(bytes.length + 5).put((byte) 0).putInt(1234).put(bytes);
    bb.rewind();
    // When
    GenericRecord actual = new SchemaRegistryBasedAvroBytesDecoder(registry).parse(bb);
    // Then
    Assert.assertEquals(someAvroDatum.get("id"), actual.get("id"));
  }

  @Test(expected = ParseException.class)
  public void testParseCorrupted() throws Exception
  {
    // Given
    when(registry.getByID(eq(1234))).thenReturn(SomeAvroDatum.getClassSchema());
    GenericRecord someAvroDatum = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    Schema schema = SomeAvroDatum.getClassSchema();
    byte[] bytes = getAvroDatum(schema, someAvroDatum);
    ByteBuffer bb = ByteBuffer.allocate(bytes.length + 5).put((byte) 0).putInt(1234).put((bytes), 5, 10);
    // When
    new SchemaRegistryBasedAvroBytesDecoder(registry).parse(bb);
  }

  @Test(expected = ParseException.class)
  public void testParseWrongId() throws Exception
  {
    // Given
    when(registry.getByID(anyInt())).thenThrow(new IOException("no pasaran"));
    GenericRecord someAvroDatum = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    Schema schema = SomeAvroDatum.getClassSchema();
    byte[] bytes = getAvroDatum(schema, someAvroDatum);
    ByteBuffer bb = ByteBuffer.allocate(bytes.length + 5).put((byte) 0).putInt(1234).put(bytes);
    // When
    new SchemaRegistryBasedAvroBytesDecoder(registry).parse(bb);
  }

  byte[] getAvroDatum(Schema schema, GenericRecord someAvroDatum) throws IOException
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    writer.write(someAvroDatum, EncoderFactory.get().directBinaryEncoder(out, null));
    return out.toByteArray();
  }

}
