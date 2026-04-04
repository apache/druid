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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.transform.TransformingInputEntityReader;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProtobufInputFormatTest
{

  private TimestampSpec timestampSpec;
  private DimensionsSpec dimensionsSpec;
  private JSONPathSpec flattenSpec;
  private FileBasedProtobufBytesDecoder decoder;
  private InlineDescriptorProtobufBytesDecoder inlineSchemaDecoder;

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @BeforeEach
  public void setUp() throws Exception
  {
    ExpressionProcessing.initializeForTests();
    timestampSpec = new TimestampSpec("timestamp", "iso", null);
    dimensionsSpec = new DimensionsSpec(Lists.newArrayList(
        new StringDimensionSchema("event"),
        new StringDimensionSchema("id"),
        new StringDimensionSchema("someOtherId"),
        new StringDimensionSchema("isValid"),
        new StringDimensionSchema("someBytesColumn")
    ));
    flattenSpec = new JSONPathSpec(
        true,
        Lists.newArrayList(
            new JSONPathFieldSpec(JSONPathFieldType.ROOT, "eventType", "eventType"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "foobar", "$.foo.bar"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "bar0", "$.bar[0].bar")
        )
    );
    decoder = new FileBasedProtobufBytesDecoder("proto_test_event.desc", "ProtoTestEvent");

    File descFile = new File(this.getClass()
                                 .getClassLoader()
                                 .getResource("proto_test_event.desc")
                                 .toURI());
    String descString = StringUtils.encodeBase64String(Files.toByteArray(descFile));
    inlineSchemaDecoder = new InlineDescriptorProtobufBytesDecoder(descString, "ProtoTestEvent");

    for (Module jacksonModule : new ProtobufExtensionsModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
    jsonMapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper())
    );
  }

  @Test
  public void testSerde() throws IOException
  {
    ProtobufInputFormat inputFormat = new ProtobufInputFormat(
        flattenSpec,
        decoder
    );
    NestedInputFormat inputFormat2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        NestedInputFormat.class
    );

    assertEquals(inputFormat, inputFormat2);
  }

  @Test
  public void testSerdeForSchemaRegistry() throws IOException
  {
    ProtobufInputFormat inputFormat = new ProtobufInputFormat(
        flattenSpec,
        new SchemaRegistryBasedProtobufBytesDecoder("http://test:8081", 100, null, null, null, null)
    );
    NestedInputFormat inputFormat2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        NestedInputFormat.class
    );
    assertEquals(inputFormat, inputFormat2);
  }

  @Test
  public void testParseFlattenData() throws Exception
  {
    //configure parser with desc file
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(flattenSpec, decoder);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(toByteBuffer(event));

    InputRow row = protobufInputFormat.createReader(
        new InputRowSchema(timestampSpec, dimensionsSpec, null),
        entity,
        null
    ).read().next();

    assertEquals(
        ImmutableList.builder()
                     .add("event")
                     .add("id")
                     .add("someOtherId")
                     .add("isValid")
                     .add("someBytesColumn")
                     .build(),
        row.getDimensions()
    );

    verifyNestedData(row, dateTime);
  }

  @Test
  public void testParseFlattenDataJq() throws Exception
  {
    //configure parser with desc file
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(
        new JSONPathSpec(
            true,
            Lists.newArrayList(
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "eventType", "eventType"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "foobar", ".foo.bar"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "bar0", ".bar[0].bar")
            )
        ),
        decoder
    );

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(toByteBuffer(event));

    InputRow row = protobufInputFormat.createReader(
        new InputRowSchema(timestampSpec, dimensionsSpec, null),
        entity,
        null
    ).read().next();

    assertEquals(
        ImmutableList.builder()
                     .add("event")
                     .add("id")
                     .add("someOtherId")
                     .add("isValid")
                     .add("someBytesColumn")
                     .build(),
        row.getDimensions()
    );

    verifyNestedData(row, dateTime);
  }

  @Test
  public void testParseFlattenDataDiscover() throws Exception
  {
    //configure parser with desc file
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(flattenSpec, decoder);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(toByteBuffer(event));

    InputRow row = protobufInputFormat.createReader(
        new InputRowSchema(timestampSpec, new DimensionsSpec(Collections.emptyList()), null),
        entity,
        null
    ).read().next();

    assertEquals(
        ImmutableSet.builder()
                     .add("eventType")
                     .add("foobar")
                     .add("bar0")
                     .add("someOtherId")
                     .add("someIntColumn")
                     .add("isValid")
                     .add("description")
                     .add("someLongColumn")
                     .add("someFloatColumn")
                     .add("id")
                     .add("someBytesColumn")
                     .build(),
        new HashSet<>(row.getDimensions())
    );

    verifyNestedData(row, dateTime);
  }

  @Test
  public void testParseNestedData() throws Exception
  {
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(
        JSONPathSpec.DEFAULT,
        decoder
    );

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(toByteBuffer(event));

    InputEntityReader reader = protobufInputFormat.createReader(
        new InputRowSchema(
            timestampSpec,
            new DimensionsSpec(
                Lists.newArrayList(
                    AutoTypeColumnSchema.of("event"),
                    AutoTypeColumnSchema.of("id"),
                    AutoTypeColumnSchema.of("someOtherId"),
                    AutoTypeColumnSchema.of("isValid"),
                    AutoTypeColumnSchema.of("eventType"),
                    AutoTypeColumnSchema.of("foo"),
                    AutoTypeColumnSchema.of("bar"),
                    AutoTypeColumnSchema.of("someBytesColumn")
                )
            ),
            null
        ),
        entity,
        null
    );

    TransformSpec transformSpec = new TransformSpec(
        null,
        Lists.newArrayList(
            new ExpressionTransform("foobar", "JSON_VALUE(foo, '$.bar')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("bar0", "JSON_VALUE(bar, '$[0].bar')", TestExprMacroTable.INSTANCE)
        )
    );
    TransformingInputEntityReader transformingReader = new TransformingInputEntityReader(
        reader,
        transformSpec.toTransformer()
    );


    InputRow row = transformingReader.read().next();

    assertEquals(
        ImmutableList.builder()
                     .add("event")
                     .add("id")
                     .add("someOtherId")
                     .add("isValid")
                     .add("eventType")
                     .add("foo")
                     .add("bar")
                     .add("someBytesColumn")
                     .build(),
        row.getDimensions()
    );

    assertEquals(ImmutableMap.of("bar", "baz"), row.getRaw("foo"));
    assertEquals(
        ImmutableList.of(ImmutableMap.of("bar", "bar0"), ImmutableMap.of("bar", "bar1")),
        row.getRaw("bar")
    );
    assertArrayEquals(
        new byte[]{0x01, 0x02, 0x03, 0x04},
        (byte[]) row.getRaw("someBytesColumn")
    );
    verifyNestedData(row, dateTime);

  }

  @Test
  public void testParseNestedDataSchemaless() throws Exception
  {
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(
        JSONPathSpec.DEFAULT,
        decoder
    );

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(toByteBuffer(event));

    InputEntityReader reader = protobufInputFormat.createReader(
        new InputRowSchema(
            timestampSpec,
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
            null,
            null
        ),
        entity,
        null
    );

    TransformSpec transformSpec = new TransformSpec(
        null,
        Lists.newArrayList(
            new ExpressionTransform("foobar", "JSON_VALUE(foo, '$.bar')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("bar0", "JSON_VALUE(bar, '$[0].bar')", TestExprMacroTable.INSTANCE)
        )
    );
    TransformingInputEntityReader transformingReader = new TransformingInputEntityReader(
        reader,
        transformSpec.toTransformer()
    );


    InputRow row = transformingReader.read().next();

    assertEquals(
        ImmutableSet.of(
            "someOtherId",
            "someIntColumn",
            "isValid",
            "foo",
            "description",
            "someLongColumn",
            "someFloatColumn",
            "eventType",
            "bar",
            "id",
            "someBytesColumn"
        ),
        new HashSet<>(row.getDimensions())
    );

    assertEquals(ImmutableMap.of("bar", "baz"), row.getRaw("foo"));
    assertEquals(
        ImmutableList.of(ImmutableMap.of("bar", "bar0"), ImmutableMap.of("bar", "bar1")),
        row.getRaw("bar")
    );
    assertArrayEquals(
        new byte[]{0x01, 0x02, 0x03, 0x04},
        (byte[]) row.getRaw("someBytesColumn")
    );
    verifyNestedData(row, dateTime);

  }

  @Test
  public void testParseNestedDataTransformsOnly() throws Exception
  {
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(
        JSONPathSpec.DEFAULT,
        decoder
    );

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(toByteBuffer(event));

    InputEntityReader reader = protobufInputFormat.createReader(
        new InputRowSchema(
            timestampSpec,
            new DimensionsSpec(
                Lists.newArrayList(
                    new StringDimensionSchema("event"),
                    new StringDimensionSchema("id"),
                    new StringDimensionSchema("someOtherId"),
                    new StringDimensionSchema("isValid"),
                    new StringDimensionSchema("eventType")
                )
            ),
            null
        ),
        entity,
        null
    );

    TransformSpec transformSpec = new TransformSpec(
        null,
        Lists.newArrayList(
            new ExpressionTransform("foobar", "JSON_VALUE(foo, '$.bar')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("bar0", "JSON_VALUE(bar, '$[0].bar')", TestExprMacroTable.INSTANCE)
        )
    );
    TransformingInputEntityReader transformingReader = new TransformingInputEntityReader(
        reader,
        transformSpec.toTransformer()
    );


    InputRow row = transformingReader.read().next();
    verifyNestedData(row, dateTime);

  }

  @Test
  public void testParseFlatData() throws Exception
  {
    //configure parser with desc file
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(null, decoder);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = buildFlatData(dateTime);

    final ByteEntity entity = new ByteEntity(toByteBuffer(event));

    InputRow row = protobufInputFormat.createReader(
        new InputRowSchema(timestampSpec, dimensionsSpec, null),
        entity,
        null
    ).read().next();

    verifyFlatData(row, dateTime, false);
  }

  @Test
  public void testParseNestedDataWithInlineSchema() throws Exception
  {
    //configure parser with inline schema decoder
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(flattenSpec, inlineSchemaDecoder);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(toByteBuffer(event));

    InputRow row = protobufInputFormat.createReader(
        new InputRowSchema(timestampSpec, dimensionsSpec, null),
        entity,
        null
    ).read().next();

    verifyNestedData(row, dateTime);
  }



  private static void assertDimensionEquals(InputRow row, String dimension, Object expected)
  {
    List<String> values = row.getDimension(dimension);
    assertEquals(1, values.size());
    assertEquals(expected, values.get(0));
  }

  static ProtoTestEventWrapper.ProtoTestEvent buildFlatData(DateTime dateTime)
  {
    return ProtoTestEventWrapper.ProtoTestEvent.newBuilder()
                                               .setDescription("description")
                                               .setEventType(ProtoTestEventWrapper.ProtoTestEvent.EventCategory.CATEGORY_ONE)
                                               .setId(4711L)
                                               .setIsValid(true)
                                               .setSomeOtherId(4712)
                                               .setTimestamp(dateTime.toString())
                                               .setSomeFloatColumn(47.11F)
                                               .setSomeIntColumn(815)
                                               .setSomeLongColumn(816L)
                                               .setSomeBytesColumn(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03, 0x04}))
                                               .build();
  }

  static void verifyFlatData(InputRow row, DateTime dateTime, boolean badBytesConversion)
  {
    assertEquals(dateTime.getMillis(), row.getTimestampFromEpoch());

    assertDimensionEquals(row, "id", "4711");
    assertDimensionEquals(row, "isValid", "true");
    assertDimensionEquals(row, "someOtherId", "4712");
    assertDimensionEquals(row, "description", "description");
    if (badBytesConversion) {
      // legacy flattener used by parser doesn't convert bytes, instead calls tostring
      // this can be removed if we update the parser to use the protobuf flattener used by the input format/reader
      assertDimensionEquals(row, "someBytesColumn", Objects.requireNonNull(row.getRaw("someBytesColumn")).toString());
    } else {
      assertDimensionEquals(row, "someBytesColumn", StringUtils.encodeBase64String(new byte[]{0x01, 0x02, 0x03, 0x04}));
    }

    assertEquals(47.11F, row.getMetric("someFloatColumn").floatValue(), 0.0);
    assertEquals(815.0F, row.getMetric("someIntColumn").floatValue(), 0.0);
    assertEquals(816.0F, row.getMetric("someLongColumn").floatValue(), 0.0);
  }

  static ProtoTestEventWrapper.ProtoTestEvent buildNestedData(DateTime dateTime)
  {
    return ProtoTestEventWrapper.ProtoTestEvent.newBuilder()
                                               .setDescription("description")
                                               .setEventType(ProtoTestEventWrapper.ProtoTestEvent.EventCategory.CATEGORY_ONE)
                                               .setId(4711L)
                                               .setIsValid(true)
                                               .setSomeOtherId(4712)
                                               .setTimestamp(dateTime.toString())
                                               .setSomeFloatColumn(47.11F)
                                               .setSomeIntColumn(815)
                                               .setSomeLongColumn(816L)
                                               .setSomeBytesColumn(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03, 0x04}))
                                               .setFoo(ProtoTestEventWrapper.ProtoTestEvent.Foo
                                                           .newBuilder()
                                                           .setBar("baz"))
                                               .addBar(ProtoTestEventWrapper.ProtoTestEvent.Foo
                                                           .newBuilder()
                                                           .setBar("bar0"))
                                               .addBar(ProtoTestEventWrapper.ProtoTestEvent.Foo
                                                           .newBuilder()
                                                           .setBar("bar1"))
                                               .build();
  }

  static void verifyNestedData(InputRow row, DateTime dateTime)
  {
    assertEquals(dateTime.getMillis(), row.getTimestampFromEpoch());

    assertDimensionEquals(row, "id", "4711");
    assertDimensionEquals(row, "isValid", "true");
    assertDimensionEquals(row, "someOtherId", "4712");
    assertDimensionEquals(row, "description", "description");

    assertDimensionEquals(row, "eventType", ProtoTestEventWrapper.ProtoTestEvent.EventCategory.CATEGORY_ONE.name());
    assertDimensionEquals(row, "foobar", "baz");
    assertDimensionEquals(row, "bar0", "bar0");
    assertDimensionEquals(row, "someBytesColumn", StringUtils.encodeBase64String(new byte[]{0x01, 0x02, 0x03, 0x04}));

    assertEquals(47.11F, row.getMetric("someFloatColumn").floatValue(), 0.0);
    assertEquals(815.0F, row.getMetric("someIntColumn").floatValue(), 0.0);
    assertEquals(816.0F, row.getMetric("someLongColumn").floatValue(), 0.0);
  }

  static ProtoTestEventWrapper.ProtoTestEvent buildFlatDataWithComplexTimestamp(DateTime dateTime)
  {
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(dateTime.getMillis() / 1000).setNanos((int) ((dateTime.getMillis() % 1000) * 1000 * 1000)).build();
    return ProtoTestEventWrapper.ProtoTestEvent.newBuilder()
                                               .setDescription("description")
                                               .setEventType(ProtoTestEventWrapper.ProtoTestEvent.EventCategory.CATEGORY_ONE)
                                               .setId(4711L)
                                               .setIsValid(true)
                                               .setSomeOtherId(4712)
                                               .setOtherTimestamp(timestamp)
                                               .setTimestamp("unused")
                                               .setSomeFloatColumn(47.11F)
                                               .setSomeIntColumn(815)
                                               .setSomeLongColumn(816L)
                                               .setSomeBytesColumn(ByteString.copyFrom(new byte[]{0x01, 0x02, 0x03, 0x04}))
                                               .build();
  }

  static void verifyFlatDataWithComplexTimestamp(InputRow row, DateTime dateTime, boolean badBytesConversion)
  {
    verifyFlatData(row, dateTime, badBytesConversion);
  }

  static ByteBuffer toByteBuffer(ProtoTestEventWrapper.ProtoTestEvent event) throws IOException
  {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      event.writeTo(out);
      return ByteBuffer.wrap(out.toByteArray());
    }
  }
}
