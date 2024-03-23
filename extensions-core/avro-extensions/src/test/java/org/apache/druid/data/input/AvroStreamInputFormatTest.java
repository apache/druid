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
import com.google.common.collect.ImmutableMap;
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
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.schemarepo.Avro1124RESTRepositoryClientWrapper;
import org.apache.druid.data.input.schemarepo.Avro1124SubjectAndIdConverter;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.transform.TransformingInputEntityReader;
import org.apache.druid.testing.InitializedNullHandlingTest;
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

/**
 * test data row:
 * {
 *  "timestamp": 1445801400000,
 *  "eventType": "type-a",
 *  "id": 1976491,
 *  "someOtherId": 6568719896,
 *  "isValid": true,
 *  "someIntArray": [1, 2, 4, 8],
 *  "someStringArray": ["8", "4", "2", "1", null],
 *  "someIntValueMap": {"8": 8, "1": 1, "2": 2, "4": 4},
 *  "someStringValueMap": {"8": "8", "1": "1", "2": "2", "4": "4"},
 *  "someUnion": "string as union",
 *  "someMultiMemberUnion": 1,
 *  "someNull": null,
 *  "someFixed": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
 *  "someBytes": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
 *  "someEnum": "ENUM1",
 *  "someRecord": {"subInt": 4892, "subLong": 1543698},
 *  "someLong": 679865987569912369,
 *  "someInt": 1,
 *  "someFloat": 0.23555,
 *  "someRecordArray": [{"nestedString": "string in record"}]
 *  }
 */
public class AvroStreamInputFormatTest extends InitializedNullHandlingTest
{
  private static final String EVENT_TYPE = "eventType";
  private static final String ID = "id";
  private static final String SOME_OTHER_ID = "someOtherId";
  private static final String NESTED_ARRAY_VAL = "nestedArrayVal";
  private static final String IS_VALID = "isValid";
  private static final String TOPIC = "aTopic";
  static final List<String> DIMENSIONS = Arrays.asList(EVENT_TYPE, ID, SOME_OTHER_ID, IS_VALID, NESTED_ARRAY_VAL);
  private static final List<String> DIMENSIONS_SCHEMALESS = Arrays.asList(
      NESTED_ARRAY_VAL,
      SOME_OTHER_ID,
      "someIntArray",
      "someFloat",
      "someUnion",
      EVENT_TYPE,
      ID,
      "someFixed",
      "someBytes",
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
          new JSONPathFieldSpec(JSONPathFieldType.PATH, "nested", "someRecord.subLong"),
          new JSONPathFieldSpec(JSONPathFieldType.PATH, "nestedArrayVal", "someRecordArray[?(@.nestedString=='string in record')].nestedString")

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
  public void testMissingAvroBytesDecoderRaisesIAE()
  {
    Assert.assertThrows(
        "avroBytesDecoder is required to decode Avro records",
        IAE.class,
        () -> new AvroStreamInputFormat(
            flattenSpec,
            null,
            true,
            true
        )
    );
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
  public void testParseTransformNested() throws SchemaValidationException, IOException
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

    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        ImmutableList.of(
            new AutoTypeColumnSchema("someIntValueMap", null),
            new AutoTypeColumnSchema("someStringValueMap", null),
            new AutoTypeColumnSchema("someRecord", null),
            new AutoTypeColumnSchema("someRecordArray", null),
            new LongDimensionSchema("tSomeIntValueMap8"),
            new LongDimensionSchema("tSomeIntValueMap8_2"),
            new StringDimensionSchema("tSomeStringValueMap8"),
            new LongDimensionSchema("tSomeRecordSubLong"),
            new AutoTypeColumnSchema("tSomeRecordArray0", null),
            new StringDimensionSchema("tSomeRecordArray0nestedString")
        )
    );
    InputEntityReader reader = inputFormat2.createReader(
        new InputRowSchema(timestampSpec, dimensionsSpec, null),
        entity,
        null
    );
    TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform(
                "tSomeIntValueMap8",
                "json_value(someIntValueMap, '$.8')",
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionTransform(
                "tSomeIntValueMap8_2",
                "json_value(json_query(someIntValueMap, '$'), '$.8')",
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionTransform(
                "tSomeStringValueMap8",
                "json_value(someStringValueMap, '$.8')",
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionTransform(
                "tSomeRecordSubLong",
                "json_value(someRecord, '$.subLong')",
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionTransform(
                "tSomeRecordArray0",
                "json_query(someRecordArray, '$[0]')",
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionTransform(
                "tSomeRecordArray0nestedString",
                "json_value(someRecordArray, '$[0].nestedString')",
                TestExprMacroTable.INSTANCE
            )
        )
    );
    TransformingInputEntityReader transformingReader = new TransformingInputEntityReader(
        reader,
        transformSpec.toTransformer()
    );
    InputRow inputRow = transformingReader.read().next();

    Assert.assertEquals(1543698L, inputRow.getTimestampFromEpoch());
    Assert.assertEquals(
        AvroStreamInputRowParserTest.SOME_INT_VALUE_MAP_VALUE,
        StructuredData.unwrap(inputRow.getRaw("someIntValueMap"))
    );
    Assert.assertEquals(
        AvroStreamInputRowParserTest.SOME_STRING_VALUE_MAP_VALUE,
        StructuredData.unwrap(inputRow.getRaw("someStringValueMap"))
    );
    Assert.assertEquals(
        ImmutableMap.of("subInt", 4892, "subLong", 1543698L),
        StructuredData.unwrap(inputRow.getRaw("someRecord"))
    );
    Assert.assertEquals(
        ImmutableList.of(ImmutableMap.of("nestedString", "string in record")),
        StructuredData.unwrap(inputRow.getRaw("someRecordArray"))
    );

    Assert.assertEquals(8L, inputRow.getRaw("tSomeIntValueMap8"));
    Assert.assertEquals(8L, inputRow.getRaw("tSomeIntValueMap8_2"));
    Assert.assertEquals("8", inputRow.getRaw("tSomeStringValueMap8"));
    Assert.assertEquals(1543698L, inputRow.getRaw("tSomeRecordSubLong"));
    Assert.assertEquals(
        ImmutableMap.of("nestedString", "string in record"),
        StructuredData.unwrap(inputRow.getRaw("tSomeRecordArray0"))
    );
    Assert.assertEquals("string in record", inputRow.getRaw("tSomeRecordArray0nestedString"));
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
