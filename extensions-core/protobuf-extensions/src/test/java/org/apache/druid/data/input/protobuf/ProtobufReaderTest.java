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

import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;

public class ProtobufReaderTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private InputRowSchema inputRowSchema;
  private InputRowSchema inputRowSchemaWithComplexTimestamp;
  private JSONPathSpec flattenSpec;
  private FileBasedProtobufBytesDecoder decoder;

  @Before
  public void setUp()
  {
    TimestampSpec timestampSpec = new TimestampSpec("timestamp", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Lists.newArrayList(
            new StringDimensionSchema("event"),
            new StringDimensionSchema("id"),
            new StringDimensionSchema("someOtherId"),
            new StringDimensionSchema("isValid")
        )
    );
    flattenSpec = new JSONPathSpec(
        true,
        Lists.newArrayList(
            new JSONPathFieldSpec(JSONPathFieldType.ROOT, "eventType", "eventType"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "foobar", "$.foo.bar"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "bar0", "$.bar[0].bar")
        )
    );

    inputRowSchema = new InputRowSchema(timestampSpec, dimensionsSpec, null);
    inputRowSchemaWithComplexTimestamp = new InputRowSchema(
        new TimestampSpec("otherTimestamp", "iso", null),
        dimensionsSpec,
        null
    );
    decoder = new FileBasedProtobufBytesDecoder("prototest.desc", "ProtoTestEvent");
  }

  @Test
  public void testParseNestedData() throws Exception
  {
    ProtobufReader reader = new ProtobufReader(inputRowSchema, null, decoder, flattenSpec);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildNestedData(dateTime);

    ByteBuffer buffer = ProtobufInputRowParserTest.toByteBuffer(event);

    InputRow row = reader.parseInputRows(decoder.parse(buffer)).get(0);

    ProtobufInputRowParserTest.verifyNestedData(row, dateTime);
  }

  @Test
  public void testParseFlatData() throws Exception
  {
    ProtobufReader reader = new ProtobufReader(inputRowSchema, null, decoder, null);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildFlatData(dateTime);

    ByteBuffer buffer = ProtobufInputRowParserTest.toByteBuffer(event);

    InputRow row = reader.parseInputRows(decoder.parse(buffer)).get(0);

    ProtobufInputRowParserTest.verifyFlatData(row, dateTime);
  }

  @Test
  public void testParseFlatDataWithComplexTimestamp() throws Exception
  {
    ProtobufReader reader = new ProtobufReader(inputRowSchemaWithComplexTimestamp, null, decoder, null);

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildFlatDataWithComplexTimestamp(dateTime);

    ByteBuffer buffer = ProtobufInputRowParserTest.toByteBuffer(event);

    InputRow row = reader.parseInputRows(decoder.parse(buffer)).get(0);

    ProtobufInputRowParserTest.verifyFlatDataWithComplexTimestamp(row, dateTime);
  }

  @Test
  public void testParseFlatDataWithComplexTimestampWithDefaultFlattenSpec() throws Exception
  {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("is unparseable!");
    ProtobufReader reader = new ProtobufReader(
        inputRowSchemaWithComplexTimestamp,
        null,
        decoder,
        JSONPathSpec.DEFAULT
    );

    //create binary of proto test event
    DateTime dateTime = new DateTime(2012, 7, 12, 9, 30, ISOChronology.getInstanceUTC());
    ProtoTestEventWrapper.ProtoTestEvent event = ProtobufInputRowParserTest.buildFlatDataWithComplexTimestamp(dateTime);

    ByteBuffer buffer = ProtobufInputRowParserTest.toByteBuffer(event);

    InputRow row = reader.parseInputRows(decoder.parse(buffer)).get(0);

    ProtobufInputRowParserTest.verifyFlatDataWithComplexTimestamp(row, dateTime);
  }
}
