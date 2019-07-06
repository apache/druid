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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.druid.data.input.avro.AvroExtensionsModule;
import org.apache.druid.data.input.avro.AvroParseSpec;
import org.apache.druid.data.input.avro.SchemaRepoBasedAvroBytesDecoder;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.schemarepo.Avro1124RESTRepositoryClientWrapper;
import org.apache.druid.data.input.schemarepo.Avro1124SubjectAndIdConverter;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class AvroStreamInputRowParserTest
{
  private static final String EVENT_TYPE = "eventType";
  private static final String ID = "id";
  private static final String SOME_OTHER_ID = "someOtherId";
  private static final String IS_VALID = "isValid";
  private static final String TOPIC = "aTopic";
  private static final String EVENT_TYPE_VALUE = "type-a";
  private static final long ID_VALUE = 1976491L;
  private static final long SOME_OTHER_ID_VALUE = 6568719896L;
  private static final float SOME_FLOAT_VALUE = 0.23555f;
  private static final int SOME_INT_VALUE = 1;
  private static final long SOME_LONG_VALUE = 679865987569912369L;
  private static final DateTime DATE_TIME = new DateTime(2015, 10, 25, 19, 30, ISOChronology.getInstanceUTC());
  static final List<String> DIMENSIONS = Arrays.asList(EVENT_TYPE, ID, SOME_OTHER_ID, IS_VALID);
  private static final List<String> DIMENSIONS_SCHEMALESS = Arrays.asList(
      "nested",
      SOME_OTHER_ID,
      "someStringArray",
      "someIntArray",
      "someFloat",
      "someUnion",
      EVENT_TYPE,
      ID,
      "someBytes",
      "someLong",
      "someInt",
      "timestamp"
  );
  static final AvroParseSpec PARSE_SPEC = new AvroParseSpec(
      new TimestampSpec("nested", "millis", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(DIMENSIONS), Collections.emptyList(), null),
      new JSONPathSpec(
          true,
          ImmutableList.of(
              new JSONPathFieldSpec(JSONPathFieldType.PATH, "nested", "someRecord.subLong")
          )
      )
  );
  private static final AvroParseSpec PARSE_SPEC_SCHEMALESS = new AvroParseSpec(
      new TimestampSpec("nested", "millis", null),
      new DimensionsSpec(null, null, null),
      new JSONPathSpec(
          true,
          ImmutableList.of(
              new JSONPathFieldSpec(JSONPathFieldType.PATH, "nested", "someRecord.subLong")
          )
      )
  );
  private static final MyFixed SOME_FIXED_VALUE = new MyFixed(ByteBuffer.allocate(16).array());
  private static final long SUB_LONG_VALUE = 1543698L;
  private static final int SUB_INT_VALUE = 4892;
  private static final MySubRecord SOME_RECORD_VALUE = MySubRecord.newBuilder()
                                                                  .setSubInt(SUB_INT_VALUE)
                                                                  .setSubLong(SUB_LONG_VALUE)
                                                                  .build();
  private static final List<CharSequence> SOME_STRING_ARRAY_VALUE = Arrays.asList("8", "4", "2", "1");
  private static final List<Integer> SOME_INT_ARRAY_VALUE = Arrays.asList(1, 2, 4, 8);
  private static final Map<CharSequence, Integer> SOME_INT_VALUE_MAP_VALUE = Maps.asMap(
      new HashSet<>(Arrays.asList("8", "2", "4", "1")), new Function<CharSequence, Integer>()
      {
        @Nonnull
        @Override
        public Integer apply(@Nullable CharSequence input)
        {
          return Integer.parseInt(input.toString());
        }
      }
  );
  private static final Map<CharSequence, CharSequence> SOME_STRING_VALUE_MAP_VALUE = Maps.asMap(
      new HashSet<>(Arrays.asList("8", "2", "4", "1")), new Function<CharSequence, CharSequence>()
      {
        @Nonnull
        @Override
        public CharSequence apply(@Nullable CharSequence input)
        {
          return input.toString();
        }
      }
  );
  private static final String SOME_UNION_VALUE = "string as union";
  private static final ByteBuffer SOME_BYTES_VALUE = ByteBuffer.allocate(8);

  private static final Pattern BRACES_AND_SPACE = Pattern.compile("[{} ]");

  private final ObjectMapper jsonMapper = new ObjectMapper();


  @Before
  public void before()
  {
    jsonMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    for (Module jacksonModule : new AvroExtensionsModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
  }

  @Test
  public void testSerde() throws IOException
  {
    Repository repository = new Avro1124RESTRepositoryClientWrapper("http://github.io");
    AvroStreamInputRowParser parser = new AvroStreamInputRowParser(
        PARSE_SPEC,
        new SchemaRepoBasedAvroBytesDecoder<>(new Avro1124SubjectAndIdConverter(TOPIC), repository)
    );
    ByteBufferInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(parser),
        ByteBufferInputRowParser.class
    );

    Assert.assertEquals(parser, parser2);
  }

  @Test
  public void testParse() throws SchemaValidationException, IOException
  {
    // serde test
    Repository repository = new InMemoryRepository(null);
    AvroStreamInputRowParser parser = new AvroStreamInputRowParser(
        PARSE_SPEC,
        new SchemaRepoBasedAvroBytesDecoder<>(new Avro1124SubjectAndIdConverter(TOPIC), repository)
    );
    ByteBufferInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(parser),
        ByteBufferInputRowParser.class
    );
    repository = ((SchemaRepoBasedAvroBytesDecoder) ((AvroStreamInputRowParser) parser2).getAvroBytesDecoder()).getSchemaRepository();

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

    InputRow inputRow = parser2.parseBatch(ByteBuffer.wrap(out.toByteArray())).get(0);

    assertInputRowCorrect(inputRow, DIMENSIONS);
  }

  @Test
  public void testParseSchemaless() throws SchemaValidationException, IOException
  {
    // serde test
    Repository repository = new InMemoryRepository(null);
    AvroStreamInputRowParser parser = new AvroStreamInputRowParser(
        PARSE_SPEC_SCHEMALESS,
        new SchemaRepoBasedAvroBytesDecoder<>(new Avro1124SubjectAndIdConverter(TOPIC), repository)
    );
    ByteBufferInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(parser),
        ByteBufferInputRowParser.class
    );
    repository = ((SchemaRepoBasedAvroBytesDecoder) ((AvroStreamInputRowParser) parser2).getAvroBytesDecoder()).getSchemaRepository();

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

      InputRow inputRow = parser2.parseBatch(ByteBuffer.wrap(out.toByteArray())).get(0);

      assertInputRowCorrect(inputRow, DIMENSIONS_SCHEMALESS);
    }
  }

  static void assertInputRowCorrect(InputRow inputRow, List<String> expectedDimensions)
  {
    Assert.assertEquals(expectedDimensions, inputRow.getDimensions());
    Assert.assertEquals(1543698L, inputRow.getTimestampFromEpoch());

    // test dimensions
    Assert.assertEquals(Collections.singletonList(EVENT_TYPE_VALUE), inputRow.getDimension(EVENT_TYPE));
    Assert.assertEquals(Collections.singletonList(String.valueOf(ID_VALUE)), inputRow.getDimension(ID));
    Assert.assertEquals(
        Collections.singletonList(String.valueOf(SOME_OTHER_ID_VALUE)),
        inputRow.getDimension(SOME_OTHER_ID)
    );
    Assert.assertEquals(Collections.singletonList(String.valueOf(true)), inputRow.getDimension(IS_VALID));
    Assert.assertEquals(
        Lists.transform(SOME_INT_ARRAY_VALUE, String::valueOf),
        inputRow.getDimension("someIntArray")
    );
    Assert.assertEquals(
        Lists.transform(SOME_STRING_ARRAY_VALUE, String::valueOf),
        inputRow.getDimension("someStringArray")
    );
    // towards Map avro field as druid dimension, need to convert its toString() back to HashMap to check equality
    Assert.assertEquals(1, inputRow.getDimension("someIntValueMap").size());
    Assert.assertEquals(
        SOME_INT_VALUE_MAP_VALUE,
        new HashMap<CharSequence, Integer>(
            Maps.transformValues(
                Splitter
                    .on(",")
                    .withKeyValueSeparator("=")
                    .split(BRACES_AND_SPACE.matcher(inputRow.getDimension("someIntValueMap").get(0)).replaceAll("")),
                new Function<String, Integer>()
                {
                  @Nullable
                  @Override
                  public Integer apply(@Nullable String input)
                  {
                    return Integer.valueOf(input);
                  }
                }
            )
        )
    );
    Assert.assertEquals(
        SOME_STRING_VALUE_MAP_VALUE,
        new HashMap<CharSequence, CharSequence>(
            Splitter
                .on(",")
                .withKeyValueSeparator("=")
                .split(BRACES_AND_SPACE.matcher(inputRow.getDimension("someIntValueMap").get(0)).replaceAll(""))
        )
    );
    Assert.assertEquals(Collections.singletonList(SOME_UNION_VALUE), inputRow.getDimension("someUnion"));
    Assert.assertEquals(Collections.emptyList(), inputRow.getDimension("someNull"));
    Assert.assertEquals(SOME_FIXED_VALUE, inputRow.getRaw("someFixed"));
    Assert.assertEquals(
        Arrays.toString(SOME_BYTES_VALUE.array()),
        Arrays.toString((byte[]) (inputRow.getRaw("someBytes")))
    );
    Assert.assertEquals(Collections.singletonList(String.valueOf(MyEnum.ENUM1)), inputRow.getDimension("someEnum"));
    Assert.assertEquals(
        Collections.singletonList(String.valueOf(SOME_RECORD_VALUE)),
        inputRow.getDimension("someRecord")
    );

    // test metrics
    Assert.assertEquals(SOME_FLOAT_VALUE, inputRow.getMetric("someFloat").floatValue(), 0);
    Assert.assertEquals(SOME_LONG_VALUE, inputRow.getMetric("someLong"));
    Assert.assertEquals(SOME_INT_VALUE, inputRow.getMetric("someInt"));
  }

  public static SomeAvroDatum buildSomeAvroDatum()
  {
    return SomeAvroDatum.newBuilder()
                        .setTimestamp(DATE_TIME.getMillis())
                        .setEventType(EVENT_TYPE_VALUE)
                        .setId(ID_VALUE)
                        .setSomeOtherId(SOME_OTHER_ID_VALUE)
                        .setIsValid(true)
                        .setSomeFloat(SOME_FLOAT_VALUE)
                        .setSomeInt(SOME_INT_VALUE)
                        .setSomeLong(SOME_LONG_VALUE)
                        .setSomeIntArray(SOME_INT_ARRAY_VALUE)
                        .setSomeStringArray(SOME_STRING_ARRAY_VALUE)
                        .setSomeIntValueMap(SOME_INT_VALUE_MAP_VALUE)
                        .setSomeStringValueMap(SOME_STRING_VALUE_MAP_VALUE)
                        .setSomeUnion(SOME_UNION_VALUE)
                        .setSomeFixed(SOME_FIXED_VALUE)
                        .setSomeBytes(SOME_BYTES_VALUE)
                        .setSomeNull(null)
                        .setSomeEnum(MyEnum.ENUM1)
                        .setSomeRecord(SOME_RECORD_VALUE)
                        .build();
  }
}
