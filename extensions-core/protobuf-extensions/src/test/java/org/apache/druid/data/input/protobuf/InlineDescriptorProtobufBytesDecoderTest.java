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

import com.google.common.io.Files;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InlineDescriptorProtobufBytesDecoderTest
{
  private String descString;

  @BeforeEach
  public void initDescriptorString() throws Exception
  {
    final URL resource = this.getClass()
                             .getClassLoader()
                             .getResource("prototest.desc");
    assertNotNull(resource);

    final var descFile = new File(resource.toURI());
    descString = StringUtils.encodeBase64String(Files.toByteArray(descFile));
  }

  @Test
  public void testShortMessageType()
  {
    final var decoder = new InlineDescriptorProtobufBytesDecoder(
        descString,
        "ProtoTestEvent"
    );

    assertDoesNotThrow(decoder::initDescriptor);
  }

  @Test
  public void testLongMessageType()
  {
    final var decoder = new InlineDescriptorProtobufBytesDecoder(
        descString,
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
          final var decoder = new InlineDescriptorProtobufBytesDecoder(descString, "BadName");

          decoder.initDescriptor();
        }
    );
  }

  @Test
  public void testMalformedDescriptorBase64()
  {
    assertThrows(
        IAE.class,
        () -> {
          final var decoder = new InlineDescriptorProtobufBytesDecoder("invalidString", "BadName");

          decoder.initDescriptor();
        }
    );
  }

  @Test
  public void testMalformedDescriptorValidBase64InvalidDescriptor()
  {
    assertThrows(
        ParseException.class,
        () -> {
          final var decoder = new InlineDescriptorProtobufBytesDecoder(
              "aGVsbG8gd29ybGQ=",
              "BadName"
          );

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
    final var decoder = new InlineDescriptorProtobufBytesDecoder(descString, null);
    assertDoesNotThrow(decoder::initDescriptor);
  }

  @Test
  public void testEquals()
  {
    // Test basic equality
    final var decoder1 = new InlineDescriptorProtobufBytesDecoder(
        descString,
        "ProtoTestEvent"
    );
    final var decoder2 = new InlineDescriptorProtobufBytesDecoder(
        descString,
        "ProtoTestEvent"
    );
    final var decoder3 = new InlineDescriptorProtobufBytesDecoder(
        descString,
        "ProtoTestEvent.Foo"
    );
    final var decoder4 = new InlineDescriptorProtobufBytesDecoder(
        descString,
        null
    );

    // Symmetry: x.equals(y) == y.equals(x)
    assertEquals(decoder1, decoder2);
    assertEquals(decoder2, decoder1);

    // Inequality tests
    assertNotEquals(decoder1, decoder3); // different protoMessageType
    assertNotEquals(decoder1, decoder4); // different protoMessageType (non-null vs null)
    assertNotEquals(null, decoder1);

    // HashCode consistency
    assertEquals(decoder1.hashCode(), decoder2.hashCode());
    assertNotEquals(decoder1.hashCode(), decoder3.hashCode());
    assertNotEquals(decoder1.hashCode(), decoder4.hashCode());
  }

}
