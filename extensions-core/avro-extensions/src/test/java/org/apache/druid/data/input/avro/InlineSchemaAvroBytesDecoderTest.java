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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.druid.data.input.AvroStreamInputRowParserTest;
import org.apache.druid.data.input.SomeAvroDatum;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 */
public class InlineSchemaAvroBytesDecoderTest
{
  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"schema_inline\",\n"
                     + "  \"schema\": {\n"
                     + "    \"namespace\": \"org.apache.druid.data.input\",\n"
                     + "   \"name\": \"SomeData\",\n"
                     + "    \"type\": \"record\",\n"
                     + "    \"fields\" : [\n"
                     + "      {\"name\":\"timestamp\",\"type\":\"long\"},\n"
                     + "      {\"name\":\"eventType\",\"type\":\"string\"},\n"
                     + "      {\"name\":\"id\",\"type\":\"long\"}\n"
                     + "    ]\n"
                     + "  }\n"
                     + "}";

    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, mapper)
    );
    InlineSchemaAvroBytesDecoder actual = (InlineSchemaAvroBytesDecoder) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                AvroBytesDecoder.class
            )
        ),
        AvroBytesDecoder.class
    );

    Assert.assertEquals(actual.getSchema().get("name"), "SomeData");
  }

  @Test
  public void testParse() throws Exception
  {
    GenericRecord someAvroDatum = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    Schema schema = SomeAvroDatum.getClassSchema();

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
    writer.write(someAvroDatum, EncoderFactory.get().directBinaryEncoder(out, null));

    GenericRecord actual = new InlineSchemaAvroBytesDecoder(schema).parse(ByteBuffer.wrap(out.toByteArray()));
    Assert.assertEquals(someAvroDatum.get("id"), actual.get("id"));
  }
}
