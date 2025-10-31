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

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("ResultOfObjectAllocationIgnored")
public class FileBasedProtobufBytesDecoderTest
{
  @Test
  public void testShortMessageType()
  {
    final var decoder = new FileBasedProtobufBytesDecoder("proto_test_event.desc", "ProtoTestEvent");

    assertDoesNotThrow(decoder::initializeDescriptor);

    assertEquals("prototest.ProtoTestEvent", decoder.getDescriptor().getFullName());
  }

  @Test
  public void testLongMessageType()
  {
    final var decoder = new FileBasedProtobufBytesDecoder(
        "proto_test_event.desc",
        "prototest.ProtoTestEvent"
    );

    assertEquals("prototest.ProtoTestEvent", decoder.getDescriptor().getFullName());
  }

  @Test
  public void testMoreComplexProtoFile()
  {
    final var decoder = new FileBasedProtobufBytesDecoder(
        "proto_nested_event.desc",
        "ProtoNestedEvent"
    );

    assertEquals("prototest.ProtoNestedEvent", decoder.getDescriptor().getFullName());
  }

  @Test
  public void testParsingWithMoreComplexProtoFile() throws Exception
  {
    // given
    final var decoder = new FileBasedProtobufBytesDecoder(
        "proto_nested_event.desc",
        "ProtoNestedEvent"
    );

    final var myStruct = Struct
        .newBuilder()
        .putFields("key1", Value.newBuilder().setStringValue("value1").build())
        .putFields("key2", Value.newBuilder().setNumberValue(42.0).build())
        .build();

    final var testMessage = ProtoNestedEvent
        .newBuilder()
        .setTimestamp(1234567890L)
        .setName("test-event")
        .setLog("This is a test log message")
        .setMyStruct(myStruct)
        .putMyMap("mapKey1", Value.newBuilder().setStringValue("mapValue1").build())
        .putMyMap("mapKey2", Value.newBuilder().setBoolValue(true).build())
        .build();

    // when
    final var decodedMessage = decoder.parse(ByteBuffer.wrap(testMessage.toByteArray()));

    // then
    assertEquals(JsonFormat.printer().print(testMessage), JsonFormat.printer().print(decodedMessage));
  }

  @Test
  public void testBadProto()
  {
    final var ex = assertThrows(
        ParseException.class,
        () -> new FileBasedProtobufBytesDecoder("proto_test_event.desc", "BadName")
    );

    assertEquals(
        "Protobuf message type [BadName] not found in the descriptor set. Available types: [Foo, ProtoTestEvent, ProtoTestEvent.Foo, Timestamp, google.protobuf.Timestamp, prototest.ProtoTestEvent, prototest.ProtoTestEvent.Foo]",
        ex.getMessage()
    );
  }

  @Test
  public void testMalformedDescriptorUrl()
  {
    final var ex = assertThrows(
        ParseException.class,
        () -> new FileBasedProtobufBytesDecoder("file:/nonexist.desc", "BadName")
    );

    assertEquals(
        "Descriptor not found in class path [file:/nonexist.desc]",
        ex.getMessage()
    );
  }

  @Test
  public void testSingleDescriptorNoMessageType()
  {
    final var decoder = new FileBasedProtobufBytesDecoder("proto_test_event.desc", null);

    // Descriptor order may return Timestamp or ProtoTestEvent.
    String actual = decoder.getDescriptor().getFullName();
    assertTrue(
        "google.protobuf.Timestamp".equals(actual)
            || "prototest.ProtoTestEvent".equals(actual)
    );
  }

  @Test
  public void testEquals()
  {
    // Test basic equality
    final var decoder1 = new FileBasedProtobufBytesDecoder(
        "proto_test_event.desc",
        "ProtoTestEvent"
    );
    final var decoder2 = new FileBasedProtobufBytesDecoder(
        "proto_test_event.desc",
        "ProtoTestEvent"
    );
    final var decoder3 = new FileBasedProtobufBytesDecoder(
        "proto_test_event.desc",
        "ProtoTestEvent.Foo"
    );
    final var decoder4 = new FileBasedProtobufBytesDecoder(
        "proto_test_event.desc",
        null
    );

    // Symmetry: x.equals(y) == y.equals(x)
    assertEquals(decoder1, decoder2);
    assertEquals(decoder2, decoder1);

    // Inequality tests
    assertNotEquals(decoder1, decoder3); // different protoMessageType (short vs long form)
    assertNotEquals(decoder1, decoder4); // different protoMessageType (non-null vs null)
    assertNotEquals(null, decoder1);

    // HashCode consistency
    assertEquals(decoder1.hashCode(), decoder2.hashCode());
    assertNotEquals(decoder1.hashCode(), decoder3.hashCode());
    assertNotEquals(decoder1.hashCode(), decoder4.hashCode());
  }
}
