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
import org.apache.druid.common.config.NullHandling;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

public class ProtobufInputFormatTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private TimestampSpec timestampSpec;
  private DimensionsSpec dimensionsSpec;
  private JSONPathSpec flattenSpec;
  private FileBasedProtobufBytesDecoder decoder;
  private InlineDescriptorProtobufBytesDecoder inlineSchemaDecoder;

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Before
  public void setUp() throws Exception
  {
    NullHandling.initializeForTests();
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
    decoder = new FileBasedProtobufBytesDecoder("prototest.desc", "ProtoTestEvent");

    File descFile = new File(this.getClass()
                                 .getClassLoader()
                                 .getResource("prototest.desc")
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

    Assert.assertEquals(inputFormat, inputFormat2);
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
    Assert.assertEquals(inputFormat, inputFormat2);
  }

  @Test
  public void testParseFlattenData() throws Exception
  {
    //configure parser with desc file
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(flattenSpec, decoder);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(ProtobufInputRowParserTest.toByteBuffer(event));

    InputRow row = protobufInputFormat.createReader(
        new InputRowSchema(timestampSpec, dimensionsSpec, null),
        entity,
        null
    ).read().next();

    Assert.assertEquals(
        ImmutableList.builder()
                     .add("event")
                     .add("id")
                     .add("someOtherId")
                     .add("isValid")
                     .add("someBytesColumn")
                     .build(),
        row.getDimensions()
    );

    ProtobufInputRowParserTest.verifyNestedData(row, dateTime);
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
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(ProtobufInputRowParserTest.toByteBuffer(event));

    InputRow row = protobufInputFormat.createReader(
        new InputRowSchema(timestampSpec, dimensionsSpec, null),
        entity,
        null
    ).read().next();

    Assert.assertEquals(
        ImmutableList.builder()
                     .add("event")
                     .add("id")
                     .add("someOtherId")
                     .add("isValid")
                     .add("someBytesColumn")
                     .build(),
        row.getDimensions()
    );

    ProtobufInputRowParserTest.verifyNestedData(row, dateTime);
  }

  @Test
  public void testParseFlattenDataDiscover() throws Exception
  {
    //configure parser with desc file
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(flattenSpec, decoder);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(ProtobufInputRowParserTest.toByteBuffer(event));

    InputRow row = protobufInputFormat.createReader(
        new InputRowSchema(timestampSpec, new DimensionsSpec(Collections.emptyList()), null),
        entity,
        null
    ).read().next();

    Assert.assertEquals(
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

    ProtobufInputRowParserTest.verifyNestedData(row, dateTime);
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
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(ProtobufInputRowParserTest.toByteBuffer(event));

    InputEntityReader reader = protobufInputFormat.createReader(
        new InputRowSchema(
            timestampSpec,
            new DimensionsSpec(
                Lists.newArrayList(
                    new AutoTypeColumnSchema("event", null),
                    new AutoTypeColumnSchema("id", null),
                    new AutoTypeColumnSchema("someOtherId", null),
                    new AutoTypeColumnSchema("isValid", null),
                    new AutoTypeColumnSchema("eventType", null),
                    new AutoTypeColumnSchema("foo", null),
                    new AutoTypeColumnSchema("bar", null),
                    new AutoTypeColumnSchema("someBytesColumn", null)
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

    Assert.assertEquals(
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

    Assert.assertEquals(ImmutableMap.of("bar", "baz"), row.getRaw("foo"));
    Assert.assertEquals(
        ImmutableList.of(ImmutableMap.of("bar", "bar0"), ImmutableMap.of("bar", "bar1")),
        row.getRaw("bar")
    );
    Assert.assertArrayEquals(
        new byte[]{0x01, 0x02, 0x03, 0x04},
        (byte[]) row.getRaw("someBytesColumn")
    );
    ProtobufInputRowParserTest.verifyNestedData(row, dateTime);

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
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(ProtobufInputRowParserTest.toByteBuffer(event));

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

    Assert.assertEquals(
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

    Assert.assertEquals(ImmutableMap.of("bar", "baz"), row.getRaw("foo"));
    Assert.assertEquals(
        ImmutableList.of(ImmutableMap.of("bar", "bar0"), ImmutableMap.of("bar", "bar1")),
        row.getRaw("bar")
    );
    Assert.assertArrayEquals(
        new byte[]{0x01, 0x02, 0x03, 0x04},
        (byte[]) row.getRaw("someBytesColumn")
    );
    ProtobufInputRowParserTest.verifyNestedData(row, dateTime);

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
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(ProtobufInputRowParserTest.toByteBuffer(event));

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
    ProtobufInputRowParserTest.verifyNestedData(row, dateTime);

  }

  @Test
  public void testParseFlatData() throws Exception
  {
    //configure parser with desc file
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(null, decoder);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildFlatData(dateTime);

    final ByteEntity entity = new ByteEntity(ProtobufInputRowParserTest.toByteBuffer(event));

    InputRow row = protobufInputFormat.createReader(
        new InputRowSchema(timestampSpec, dimensionsSpec, null),
        entity,
        null
    ).read().next();

    ProtobufInputRowParserTest.verifyFlatData(row, dateTime, false);
  }

  @Test
  public void testParseNestedDataWithInlineSchema() throws Exception
  {
    //configure parser with inline schema decoder
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(flattenSpec, inlineSchemaDecoder);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildNestedData(dateTime);

    final ByteEntity entity = new ByteEntity(ProtobufInputRowParserTest.toByteBuffer(event));

    InputRow row = protobufInputFormat.createReader(
        new InputRowSchema(timestampSpec, dimensionsSpec, null),
        entity,
        null
    ).read().next();

    ProtobufInputRowParserTest.verifyNestedData(row, dateTime);
  }
}
