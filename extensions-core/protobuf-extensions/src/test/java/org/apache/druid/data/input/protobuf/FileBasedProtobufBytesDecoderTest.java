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

import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FileBasedProtobufBytesDecoderTest
{
  /**
   * Configure parser with desc file, and specify which file name to use.
   */
  @Test
  public void testShortMessageType()
  {
    final var decoder = new FileBasedProtobufBytesDecoder("prototest.desc", "ProtoTestEvent");

    assertDoesNotThrow(decoder::initDescriptor);
  }

  /**
   * Configure parser with desc file, and specify which file name to use
   */
  @Test
  public void testLongMessageType()
  {
    final var decoder = new FileBasedProtobufBytesDecoder(
        "prototest.desc",
        "prototest.ProtoTestEvent"
    );

    assertDoesNotThrow(decoder::initDescriptor);
  }

  @Test
  public void testBadProto()
  {
    assertThrows(
        ParseException.class,
        () -> {
          // configure parser with desc file
          final var decoder = new FileBasedProtobufBytesDecoder("prototest.desc", "BadName");
          decoder.initDescriptor();
        }
    );
  }

  @Test
  public void testMalformedDescriptorUrl()
  {
    assertThrows(
        ParseException.class,
        () -> {
          // configure parser with non existent desc file
          final var decoder = new FileBasedProtobufBytesDecoder("file:/nonexist.desc", "BadName");
          decoder.initDescriptor();
        }
    );
  }

  /**
   * For the backward compatibility, protoMessageType allows null when the desc file has only one message type.
   */
  @Test
  public void testSingleDescriptorNoMessageType()
  {
    final var decoder = new FileBasedProtobufBytesDecoder("prototest.desc", null);

    assertDoesNotThrow(decoder::initDescriptor);
  }

  @Test
  public void testEquals()
  {
    // Test basic equality
    final var decoder1 = new FileBasedProtobufBytesDecoder(
        "prototest.desc",
        "ProtoTestEvent"
    );
    final var decoder2 = new FileBasedProtobufBytesDecoder(
        "prototest.desc",
        "ProtoTestEvent"
    );
    final var decoder3 = new FileBasedProtobufBytesDecoder(
        "prototest.desc",
        "ProtoTestEvent.Foo"
    );
    final var decoder4 = new FileBasedProtobufBytesDecoder(
        "prototest.desc",
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
