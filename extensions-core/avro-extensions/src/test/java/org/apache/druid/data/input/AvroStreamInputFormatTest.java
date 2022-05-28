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

package org.apache.druid.data.input;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.druid.data.input.avro.AvroExtensionsModule;
import org.apache.druid.data.input.avro.AvroStreamInputFormat;
import org.apache.druid.data.input.avro.SchemaRegistryBasedAvroBytesDecoder;
import org.apache.druid.data.input.avro.SchemaRepoBasedAvroBytesDecoder;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.schemarepo.Avro1124RESTRepositoryClientWrapper;
import org.apache.druid.data.input.schemarepo.Avro1124SubjectAndIdConverter;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.Repository;
import org.schemarepo.SchemaValidationException;
import org.schemarepo.api.TypedSchemaRepository;
import org.schemarepo.api.converter.AvroSchemaConverter;
import org.schemarepo.api.converter.IdentityConverter;
import org.schemarepo.api.converter.IntegerConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.apache.druid.data.input.AvroStreamInputRowParserTest.assertInputRowCorrect;
import static org.apache.druid.data.input.AvroStreamInputRowParserTest.buildSomeAvroDatum;

public class AvroStreamInputFormatTest
{
  private static final String EVENT_TYPE = "eventType";
  private static final String ID = "id";
  private static final String SOME_OTHER_ID = "someOtherId";
  private static final String IS_VALID = "isValid";
  private static final String TOPIC = "aTopic";
  static final List<String> DIMENSIONS = Arrays.asList(EVENT_TYPE, ID, SOME_OTHER_ID, IS_VALID);
  private static final List<String> DIMENSIONS_SCHEMALESS = Arrays.asList(
      "nested",
      SOME_OTHER_ID,
      "someStringArray",
      "someIntArray",
      "someFloat",
      EVENT_TYPE,
      "someFixed",
      "someBytes",
      "someUnion",
      ID,
      "someEnum",
      "someLong",
      "someInt",
      "timestamp"
  );


  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private TimestampSpec timestampSpec;
  private DimensionsSpec dimensionsSpec;
  private JSONPathSpec flattenSpec;

  @Before
  public void before()
  {
    timestampSpec = new TimestampSpec("nested", "millis", null);
    dimensionsSpec = new DimensionsSpec(DimensionsSpec.getDefaultSchemas(DIMENSIONS));
    flattenSpec = new JSONPathSpec(
      true,
      ImmutableList.of(
          new JSONPathFieldSpec(JSONPathFieldType.PATH, "nested", "someRecord.subLong")
      )
  );
    for (Module jacksonModule : new AvroExtensionsModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
    jsonMapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper())
    );
  }

  @Test
  public void testSerde() throws IOException
  {
    Repository repository = new Avro1124RESTRepositoryClientWrapper("http://github.io");
    AvroStreamInputFormat inputFormat = new AvroStreamInputFormat(
        flattenSpec,
        new SchemaRepoBasedAvroBytesDecoder<>(new Avro1124SubjectAndIdConverter(TOPIC), repository),
        false,
        false
    );
    NestedInputFormat inputFormat2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        NestedInputFormat.class
    );

    Assert.assertEquals(inputFormat, inputFormat2);
  }

  @Test
  public void testSerdeNonDefault() throws IOException
  {
    Repository repository = new Avro1124RESTRepositoryClientWrapper("http://github.io");
    AvroStreamInputFormat inputFormat = new AvroStreamInputFormat(
        flattenSpec,
        new SchemaRepoBasedAvroBytesDecoder<>(new Avro1124SubjectAndIdConverter(TOPIC), repository),
        true,
        true
    );
    NestedInputFormat inputFormat2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        NestedInputFormat.class
    );

    Assert.assertEquals(inputFormat, inputFormat2);
  }

  @Test
  public void testSerdeForSchemaRegistry() throws IOException
  {
    AvroStreamInputFormat inputFormat = new AvroStreamInputFormat(
        flattenSpec,
        new SchemaRegistryBasedAvroBytesDecoder("http://test:8081", 100, null, null, null, null),
        false,
        false
    );
    NestedInputFormat inputFormat2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        NestedInputFormat.class
    );
    Assert.assertEquals(inputFormat, inputFormat2);
  }

  @Test
  public void testParse() throws SchemaValidationException, IOException
  {
    Repository repository = new InMemoryRepository(null);
    AvroStreamInputFormat inputFormat = new AvroStreamInputFormat(
        flattenSpec,
        new SchemaRepoBasedAvroBytesDecoder<>(new Avro1124SubjectAndIdConverter(TOPIC), repository),
        false,
        false
    );
    NestedInputFormat inputFormat2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        NestedInputFormat.class
    );
    repository = ((SchemaRepoBasedAvroBytesDecoder) ((AvroStreamInputFormat) inputFormat2).getAvroBytesDecoder()).getSchemaRepository();

    // prepare data
    GenericRecord someAvroDatum = buildSomeAvroDatum();

    // encode schema id
    Avro1124SubjectAndIdConverter converter = new Avro1124SubjectAndIdConverter(TOPIC);
    TypedSchemaRepository<Integer, Schema, String> repositoryClient = new TypedSchemaRepository<>(
        repository,
        new IntegerConverter(),
        new AvroSchemaConverter(),
        new IdentityConverter()
    );
    Integer id = repositoryClient.registerSchema(TOPIC, SomeAvroDatum.getClassSchema());
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    converter.putSubjectAndId(id, byteBuffer);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(byteBuffer.array());
    // encode data
    DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(someAvroDatum.getSchema());
    // write avro datum to bytes
    writer.write(someAvroDatum, EncoderFactory.get().directBinaryEncoder(out, null));

    final ByteEntity entity = new ByteEntity(ByteBuffer.wrap(out.toByteArray()));

    InputRow inputRow = inputFormat2.createReader(new InputRowSchema(timestampSpec, dimensionsSpec, null), entity, null).read().next();

    assertInputRowCorrect(inputRow, DIMENSIONS, false);
  }

  @Test
  public void testParseSchemaless() throws SchemaValidationException, IOException
  {
    Repository repository = new InMemoryRepository(null);
    AvroStreamInputFormat inputFormat = new AvroStreamInputFormat(
        flattenSpec,
        new SchemaRepoBasedAvroBytesDecoder<>(new Avro1124SubjectAndIdConverter(TOPIC), repository),
        false,
        false
    );
    NestedInputFormat inputFormat2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        NestedInputFormat.class
    );
    repository = ((SchemaRepoBasedAvroBytesDecoder) ((AvroStreamInputFormat) inputFormat2).getAvroBytesDecoder()).getSchemaRepository();

    // prepare data
    GenericRecord someAvroDatum = buildSomeAvroDatum();

    // encode schema id
    Avro1124SubjectAndIdConverter converter = new Avro1124SubjectAndIdConverter(TOPIC);
    TypedSchemaRepository<Integer, Schema, String> repositoryClient = new TypedSchemaRepository<>(
        repository,
        new IntegerConverter(),
        new AvroSchemaConverter(),
        new IdentityConverter()
    );
    Integer id = repositoryClient.registerSchema(TOPIC, SomeAvroDatum.getClassSchema());
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    converter.putSubjectAndId(id, byteBuffer);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      out.write(byteBuffer.array());
      // encode data
      DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(someAvroDatum.getSchema());
      // write avro datum to bytes
      writer.write(someAvroDatum, EncoderFactory.get().directBinaryEncoder(out, null));

      final ByteEntity entity = new ByteEntity(ByteBuffer.wrap(out.toByteArray()));

      InputRow inputRow = inputFormat2.createReader(new InputRowSchema(timestampSpec, DimensionsSpec.EMPTY, null), entity, null).read().next();

      assertInputRowCorrect(inputRow, DIMENSIONS_SCHEMALESS, false);
    }
  }
}
