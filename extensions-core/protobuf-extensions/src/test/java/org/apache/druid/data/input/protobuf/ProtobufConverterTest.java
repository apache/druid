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

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.ListValue;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ProtobufConverterTest
{
  private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

  @ParameterizedTest(name = "{0}")
  @MethodSource("messageProvider")
  public void testConvertMessages(
      final String testName,
      final Message input,
      @Nullable final Map<String, Object> expectedObject,
      @Nullable final String expectedJson
  )
  {
    final Map<String, Object> result = assertDoesNotThrow(() -> ProtobufConverter.convertMessage(input));

    assertEquals(expectedObject, result);

    // Since the ProtobufConverter should behave like the JsonFormat.Printer (just output objects instead of JSON),
    // compare to JSON as well.
    final var jsonString = assertDoesNotThrow(() -> PRINTER.print(input));
    assertEquals(expectedJson, jsonString);
  }

  @Test
  public void convertBytesValues()
  {
    final var input = BytesValue.of(ByteString.copyFromUtf8("test bytes"));

    final Map<String, Object> result = assertDoesNotThrow(() -> ProtobufConverter.convertMessage(input));

    assertArrayEquals("test bytes".getBytes(StandardCharsets.UTF_8), (byte[]) result.get("value"));
  }

  @Test
  public void testDynamicProtoEventWithNestedStruct() throws Exception
  {
    // Define a custom UserInfo message type
    final var userInfoProto = DescriptorProtos.DescriptorProto
        .newBuilder()
        .setName("UserInfo")
        .addField(
            DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setName("user_id")
                .setNumber(1)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build()
        )
        .addField(
            DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setName("permissions")
                .setNumber(2)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                .build()
        )
        .addField(
            DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setName("metadata")
                .setNumber(3)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName("google.protobuf.Struct")
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build()
        )
        .build();

    // Define the main ProtoEvent message type with custom UserInfo field
    final var eventProto = DescriptorProtos.DescriptorProto
        .newBuilder()
        .setName("ProtoEvent")
        .addField(
            DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setName("timestamp")
                .setNumber(1)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build()
        )
        .addField(
            DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setName("name")
                .setNumber(2)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build()
        )
        .addField(
            DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setName("attributes")
                .setNumber(3)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName("google.protobuf.Struct")
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build()
        )
        .addField(
            DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setName("user_info")
                .setNumber(4)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".io.test.UserInfo")
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .build()
        )
        .build();

    final var fileProto = DescriptorProtos.FileDescriptorProto
        .newBuilder()
        .setName("proto_event.proto")
        .setSyntax("proto3")
        .setPackage("io.test")
        .addDependency("google/protobuf/struct.proto")
        .addMessageType(userInfoProto)  // Add UserInfo first
        .addMessageType(eventProto)     // Then ProtoEvent that references it
        .build();

    final var fileDescriptor = Descriptors.FileDescriptor
        .buildFrom(fileProto, new Descriptors.FileDescriptor[]{Struct.getDescriptor().getFile()});

    final var eventDescriptor = fileDescriptor.findMessageTypeByName("ProtoEvent");
    final var userInfoDescriptor = fileDescriptor.findMessageTypeByName("UserInfo");

    // Build the Struct for attributes
    final Struct attributes = Struct
        .newBuilder()
        .putFields("one", Value.newBuilder().setStringValue("one thing").build())
        .putFields("two", Value.newBuilder().setStringValue("another thing").build())
        .build();

    // Build the nested UserInfo metadata Struct
    final Struct userMetadata = Struct
        .newBuilder()
        .putFields("role", Value.newBuilder().setStringValue("admin").build())
        .putFields("department", Value.newBuilder().setStringValue("engineering").build())
        .build();

    // Build the custom UserInfo message
    final var userInfo = DynamicMessage
        .newBuilder(userInfoDescriptor)
        .setField(userInfoDescriptor.findFieldByName("user_id"), "user123")
        .setField(
            userInfoDescriptor.findFieldByName("permissions"),
            List.of("read", "write", "admin")
        )
        .setField(userInfoDescriptor.findFieldByName("metadata"), userMetadata)
        .build();

    // Build the main ProtoEvent message
    final var message = DynamicMessage
        .newBuilder(eventDescriptor)
        .setField(eventDescriptor.findFieldByName("timestamp"), 1755041934232L)
        .setField(eventDescriptor.findFieldByName("name"), "a1b2c3d4")
        .setField(eventDescriptor.findFieldByName("attributes"), attributes)
        .setField(eventDescriptor.findFieldByName("user_info"), userInfo)
        .build();

    // Convert the dynamic ProtoEvent message
    final Map<String, Object> result = assertDoesNotThrow(() -> ProtobufConverter.convertMessage(message));

    // Verify basic fields
    assertEquals(1755041934232L, result.get("timestamp"));
    assertEquals("a1b2c3d4", result.get("name"));

    // Verify attributes (Struct with Value fields)
    @SuppressWarnings("unchecked")
    final var resultAttributes = (Map<String, Object>) result.get("attributes");
    assertEquals("one thing", resultAttributes.get("one"));
    assertEquals("another thing", resultAttributes.get("two"));

    // Verify the custom UserInfo nested message
    @SuppressWarnings("unchecked")
    final var resultUserInfo = (Map<String, Object>) result.get("userInfo");
    assertEquals("user123", resultUserInfo.get("userId"));

    // Verify repeated permissions field
    @SuppressWarnings("unchecked")
    final var permissions = (List<String>) resultUserInfo.get("permissions");
    assertEquals(3, permissions.size());
    assertEquals("read", permissions.get(0));
    assertEquals("write", permissions.get(1));
    assertEquals("admin", permissions.get(2));

    // Verify nested metadata Struct within UserInfo
    @SuppressWarnings("unchecked")
    final var resultMetadata = (Map<String, Object>) resultUserInfo.get("metadata");
    assertEquals("admin", resultMetadata.get("role"));
    assertEquals("engineering", resultMetadata.get("department"));

    // sanity check with JSON printer - our map should "look like" that JSON object
    final var jsonString = assertDoesNotThrow(() -> PRINTER.print(message));
    final var expectedJson = "{\"timestamp\":\"1755041934232\",\"name\":\"a1b2c3d4\",\"attributes\":{\"one\":\"one thing\",\"two\":\"another thing\"},\"userInfo\":{\"userId\":\"user123\",\"permissions\":[\"read\",\"write\",\"admin\"],\"metadata\":{\"role\":\"admin\",\"department\":\"engineering\"}}}";
    assertEquals(expectedJson, jsonString);
  }

  @Test
  public void convertNullValue()
  {
    final Map<String, Object> result = assertDoesNotThrow(() -> ProtobufConverter.convertMessage(null));
    assertNull(result);
  }

  private static Stream<Arguments> messageProvider()
  {
    return Stream.of(
        Arguments.of(
            "StringValue wrapper",
            StringValue.of("test string"),
            Map.of("value", "test string"),
            "\"test string\""
        ),
        Arguments.of("BoolValue wrapper", BoolValue.of(true), Map.of("value", true), "true"),
        Arguments.of("Int32Value wrapper", Int32Value.of(42), Map.of("value", 42), "42"),
        Arguments.of("Int64Value wrapper", Int64Value.of(123456789L), Map.of("value", 123456789L), "\"123456789\""),
        Arguments.of("UInt32Value wrapper", UInt32Value.of(2000000000), Map.of("value", 2000000000), "2000000000"),
        Arguments.of(
            "UInt64Value wrapper",
            UInt64Value.of(9000000000000000000L),
            Map.of("value", 9000000000000000000L),
            "\"9000000000000000000\""
        ),
        Arguments.of("FloatValue wrapper", FloatValue.of(3.14f), Map.of("value", 3.14f), "3.14"),
        Arguments.of("DoubleValue wrapper", DoubleValue.of(2.71828), Map.of("value", 2.71828), "2.71828"),

        Arguments.of(
            "Timestamp with seconds and nanos",
            Timestamp.newBuilder().setSeconds(1234567890L).setNanos(123456789).build(),
            Map.of("seconds", 1234567890L, "nanos", 123456789),
            "\"2009-02-13T23:31:30.123456789Z\""
        ),

        Arguments.of(
            "Struct with mixed field types",
            Struct.newBuilder()
                  .putFields("stringField", Value.newBuilder().setStringValue("test").build())
                  .putFields("numberField", Value.newBuilder().setNumberValue(123.45).build())
                  .putFields("boolField", Value.newBuilder().setBoolValue(true).build())
                  .build(),
            Map.ofEntries(
                Map.entry("stringField", "test"),
                Map.entry("numberField", 123.45),
                Map.entry("boolField", true)
            ),
            "{\"stringField\":\"test\",\"numberField\":123.45,\"boolField\":true}"
        ),

        Arguments.of(
            "ListValue with string items",
            ListValue.newBuilder()
                     .addValues(Value.newBuilder().setStringValue("item1").build())
                     .addValues(Value.newBuilder().setStringValue("item2").build())
                     .build(),
            Map.of("values", List.of(Map.of("stringValue", "item1"), Map.of("stringValue", "item2"))),
            "[\"item1\",\"item2\"]"
        ),

        Arguments.of(
            "Nested struct with outer and inner fields",
            Struct.newBuilder()
                  .putFields("outerField", Value.newBuilder().setStringValue("outer").build())
                  .putFields(
                      "nestedStruct",
                      Value.newBuilder()
                           .setStructValue(
                               Struct.newBuilder()
                                     .putFields(
                                         "nestedString",
                                         Value.newBuilder().setStringValue("nested_value").build()
                                     )
                                     .putFields("nestedNumber", Value.newBuilder().setNumberValue(456.78).build())
                                     .build())
                           .build()
                  ).build(),
            Map.ofEntries(
                Map.entry(
                    "nestedStruct",
                    Map.ofEntries(
                        Map.entry("nestedString", "nested_value"),
                        Map.entry("nestedNumber", 456.78)
                    )
                ),
                Map.entry("outerField", "outer")

            ),
            "{\"outerField\":\"outer\",\"nestedStruct\":{\"nestedString\":\"nested_value\",\"nestedNumber\":456.78}}"
        )
    );
  }
}
