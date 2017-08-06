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
package io.druid.data.input;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.avro.AvroExtensionsModule;
import io.druid.data.input.avro.SchemaRepoBasedAvroBytesDecoder;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.data.input.schemarepo.Avro1124RESTRepositoryClientWrapper;
import io.druid.data.input.schemarepo.Avro1124SubjectAndIdConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.Repository;
import org.schemarepo.SchemaValidationException;
import org.schemarepo.api.TypedSchemaRepository;
import org.schemarepo.api.converter.AvroSchemaConverter;
import org.schemarepo.api.converter.IdentityConverter;
import org.schemarepo.api.converter.IntegerConverter;

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

import static org.junit.Assert.assertEquals;

public class AvroStreamInputRowParserTest
{
  public static final String EVENT_TYPE = "eventType";
  public static final String ID = "id";
  public static final String SOME_OTHER_ID = "someOtherId";
  public static final String IS_VALID = "isValid";
  public static final String TOPIC = "aTopic";
  public static final String EVENT_TYPE_VALUE = "type-a";
  public static final long ID_VALUE = 1976491L;
  public static final long SOME_OTHER_ID_VALUE = 6568719896L;
  public static final float SOME_FLOAT_VALUE = 0.23555f;
  public static final int SOME_INT_VALUE = 1;
  public static final long SOME_LONG_VALUE = 679865987569912369L;
  public static final DateTime DATE_TIME = new DateTime(2015, 10, 25, 19, 30);
  public static final List<String> DIMENSIONS = Arrays.asList(EVENT_TYPE, ID, SOME_OTHER_ID, IS_VALID);
  public static final TimeAndDimsParseSpec PARSE_SPEC = new TimeAndDimsParseSpec(
      new TimestampSpec("timestamp", "millis", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(DIMENSIONS), Collections.<String>emptyList(), null)
  );
  public static final MyFixed SOME_FIXED_VALUE = new MyFixed(ByteBuffer.allocate(16).array());
  private static final long SUB_LONG_VALUE = 1543698L;
  private static final int SUB_INT_VALUE = 4892;
  public static final MySubRecord SOME_RECORD_VALUE = MySubRecord.newBuilder()
                                                                 .setSubInt(SUB_INT_VALUE)
                                                                 .setSubLong(SUB_LONG_VALUE)
                                                                 .build();
  public static final List<CharSequence> SOME_STRING_ARRAY_VALUE = Arrays.asList((CharSequence) "8", "4", "2", "1");
  public static final List<Integer> SOME_INT_ARRAY_VALUE = Arrays.asList(1, 2, 4, 8);
  public static final Map<CharSequence, Integer> SOME_INT_VALUE_MAP_VALUE = Maps.asMap(
      new HashSet<CharSequence>(Arrays.asList("8", "2", "4", "1")), new Function<CharSequence, Integer>()
      {
        @Nullable
        @Override
        public Integer apply(@Nullable CharSequence input)
        {
          return Integer.parseInt(input.toString());
        }
      }
  );
  public static final Map<CharSequence, CharSequence> SOME_STRING_VALUE_MAP_VALUE = Maps.asMap(
      new HashSet<CharSequence>(Arrays.asList("8", "2", "4", "1")), new Function<CharSequence, CharSequence>()
      {
        @Nullable
        @Override
        public CharSequence apply(@Nullable CharSequence input)
        {
          return input.toString();
        }
      }
  );
  public static final String SOME_UNION_VALUE = "string as union";
  public static final ByteBuffer SOME_BYTES_VALUE = ByteBuffer.allocate(8);

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
        new SchemaRepoBasedAvroBytesDecoder<String, Integer>(new Avro1124SubjectAndIdConverter(TOPIC), repository)
    );
    ByteBufferInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(parser),
        ByteBufferInputRowParser.class
    );

    assertEquals(parser, parser2);
  }

  @Test
  public void testParse() throws SchemaValidationException, IOException
  {
    // serde test
    Repository repository = new InMemoryRepository(null);
    AvroStreamInputRowParser parser = new AvroStreamInputRowParser(
        PARSE_SPEC,
        new SchemaRepoBasedAvroBytesDecoder<String, Integer>(new Avro1124SubjectAndIdConverter(TOPIC), repository)
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
    TypedSchemaRepository<Integer, Schema, String> repositoryClient = new TypedSchemaRepository<Integer, Schema, String>(
        repository,
        new IntegerConverter(),
        new AvroSchemaConverter(),
        new IdentityConverter()
    );
    Integer id = repositoryClient.registerSchema(TOPIC, SomeAvroDatum.getClassSchema());
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    converter.putSubjectAndId(TOPIC, id, byteBuffer);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(byteBuffer.array());
    // encode data
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(someAvroDatum.getSchema());
    // write avro datum to bytes
    writer.write(someAvroDatum, EncoderFactory.get().directBinaryEncoder(out, null));

    InputRow inputRow = parser2.parse(ByteBuffer.wrap(out.toByteArray()));

    assertInputRowCorrect(inputRow);
  }

  public static void assertInputRowCorrect(InputRow inputRow)
  {
    assertEquals(DIMENSIONS, inputRow.getDimensions());
    assertEquals(DATE_TIME.getMillis(), inputRow.getTimestampFromEpoch());

    // test dimensions
    assertEquals(Collections.singletonList(String.valueOf(EVENT_TYPE_VALUE)), inputRow.getDimension(EVENT_TYPE));
    assertEquals(Collections.singletonList(String.valueOf(ID_VALUE)), inputRow.getDimension(ID));
    assertEquals(Collections.singletonList(String.valueOf(SOME_OTHER_ID_VALUE)), inputRow.getDimension(SOME_OTHER_ID));
    assertEquals(Collections.singletonList(String.valueOf(true)), inputRow.getDimension(IS_VALID));
    assertEquals(
        Lists.transform(SOME_INT_ARRAY_VALUE, String::valueOf),
        inputRow.getDimension("someIntArray")
    );
    assertEquals(
        Lists.transform(SOME_STRING_ARRAY_VALUE, String::valueOf),
        inputRow.getDimension("someStringArray")
    );
    // towards Map avro field as druid dimension, need to convert its toString() back to HashMap to check equality
    assertEquals(1, inputRow.getDimension("someIntValueMap").size());
    assertEquals(
        SOME_INT_VALUE_MAP_VALUE, new HashMap<CharSequence, Integer>(
            Maps.transformValues(
                Splitter.on(",")
                        .withKeyValueSeparator("=")
                        .split(inputRow.getDimension("someIntValueMap").get(0).replaceAll("[\\{\\} ]", "")),
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
    assertEquals(
        SOME_STRING_VALUE_MAP_VALUE, new HashMap<CharSequence, CharSequence>(
            Splitter.on(",")
                    .withKeyValueSeparator("=")
                    .split(inputRow.getDimension("someIntValueMap").get(0).replaceAll("[\\{\\} ]", ""))
        )
    );
    assertEquals(Collections.singletonList(SOME_UNION_VALUE), inputRow.getDimension("someUnion"));
    assertEquals(Collections.emptyList(), inputRow.getDimension("someNull"));
    assertEquals(Collections.singletonList(String.valueOf(SOME_FIXED_VALUE)), inputRow.getDimension("someFixed"));
    assertEquals(
        Collections.singletonList(Arrays.toString(SOME_BYTES_VALUE.array())),
        inputRow.getDimension("someBytes")
    );
    assertEquals(Collections.singletonList(String.valueOf(MyEnum.ENUM1)), inputRow.getDimension("someEnum"));
    assertEquals(Collections.singletonList(String.valueOf(SOME_RECORD_VALUE)), inputRow.getDimension("someRecord"));

    // test metrics
    assertEquals(SOME_FLOAT_VALUE, inputRow.getFloatMetric("someFloat"), 0);
    assertEquals(SOME_LONG_VALUE, inputRow.getLongMetric("someLong"));
    assertEquals(SOME_INT_VALUE, inputRow.getLongMetric("someInt"));
  }

  public static GenericRecord buildSomeAvroDatum() throws IOException
  {
    SomeAvroDatum datum = SomeAvroDatum.newBuilder()
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

    return datum;
  }
}
