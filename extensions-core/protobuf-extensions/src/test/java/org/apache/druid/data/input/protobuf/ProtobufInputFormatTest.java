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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ProtobufInputFormatTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private TimestampSpec timestampSpec;
  private DimensionsSpec dimensionsSpec;
  private JSONPathSpec flattenSpec;
  private FileBasedProtobufBytesDecoder decoder;

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    timestampSpec = new TimestampSpec("timestamp", "iso", null);
    dimensionsSpec = new DimensionsSpec(Lists.newArrayList(
        new StringDimensionSchema("event"),
        new StringDimensionSchema("id"),
        new StringDimensionSchema("someOtherId"),
        new StringDimensionSchema("isValid")
    ), null, null);
    flattenSpec = new JSONPathSpec(
        true,
        Lists.newArrayList(
            new JSONPathFieldSpec(JSONPathFieldType.ROOT, "eventType", "eventType"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "foobar", "$.foo.bar"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "bar0", "$.bar[0].bar")
        )
    );
    decoder = new FileBasedProtobufBytesDecoder("prototest.desc", "ProtoTestEvent");
    for (Module jacksonModule : new ProtobufExtensionsModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
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
        new SchemaRegistryBasedProtobufBytesDecoder("http://test:8081", 100, null, null, null)
    );
    NestedInputFormat inputFormat2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        NestedInputFormat.class
    );
    Assert.assertEquals(inputFormat, inputFormat2);
  }

  @Test
  public void testParseNestedData() throws Exception
  {
    //configure parser with desc file
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(flattenSpec, decoder);

    //create binary of proto test event
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

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    event.writeTo(out);

    final ByteEntity entity = new ByteEntity(ByteBuffer.wrap(out.toByteArray()));

    InputRow row = protobufInputFormat.createReader(new InputRowSchema(timestampSpec, dimensionsSpec, null), entity, null).read().next();

    Assert.assertEquals(dateTime.getMillis(), row.getTimestampFromEpoch());

    assertDimensionEquals(row, "id", "4711");
    assertDimensionEquals(row, "isValid", "true");
    assertDimensionEquals(row, "someOtherId", "4712");
    assertDimensionEquals(row, "description", "description");

    assertDimensionEquals(row, "eventType", ProtoTestEventWrapper.ProtoTestEvent.EventCategory.CATEGORY_ONE.name());
    assertDimensionEquals(row, "foobar", "baz");
    assertDimensionEquals(row, "bar0", "bar0");


    Assert.assertEquals(47.11F, row.getMetric("someFloatColumn").floatValue(), 0.0);
    Assert.assertEquals(815.0F, row.getMetric("someIntColumn").floatValue(), 0.0);
    Assert.assertEquals(816.0F, row.getMetric("someLongColumn").floatValue(), 0.0);
  }

  @Test
  public void testParseFlatData() throws Exception
  {
    //configure parser with desc file
    ProtobufInputFormat protobufInputFormat = new ProtobufInputFormat(null, decoder);

    //create binary of proto test event
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

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    event.writeTo(out);

    final ByteEntity entity = new ByteEntity(ByteBuffer.wrap(out.toByteArray()));

    InputRow row = protobufInputFormat.createReader(new InputRowSchema(timestampSpec, dimensionsSpec, null), entity, null).read().next();

    System.out.println(row);

    Assert.assertEquals(dateTime.getMillis(), row.getTimestampFromEpoch());

    assertDimensionEquals(row, "id", "4711");
    assertDimensionEquals(row, "isValid", "true");
    assertDimensionEquals(row, "someOtherId", "4712");
    assertDimensionEquals(row, "description", "description");


    Assert.assertEquals(47.11F, row.getMetric("someFloatColumn").floatValue(), 0.0);
    Assert.assertEquals(815.0F, row.getMetric("someIntColumn").floatValue(), 0.0);
    Assert.assertEquals(816.0F, row.getMetric("someLongColumn").floatValue(), 0.0);
  }

  private void assertDimensionEquals(InputRow row, String dimension, Object expected)
  {
    List<String> values = row.getDimension(dimension);
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(expected, values.get(0));
  }
}
