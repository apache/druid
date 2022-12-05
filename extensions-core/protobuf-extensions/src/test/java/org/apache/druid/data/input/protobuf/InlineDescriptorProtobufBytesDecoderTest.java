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
import com.google.protobuf.Descriptors;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class InlineDescriptorProtobufBytesDecoderTest
{
  private String descString;

  @Before
  public void initDescriptorString() throws Exception
  {
    File descFile = new File(this.getClass()
                                 .getClassLoader()
                                 .getResource("prototest.desc")
                                 .toURI());
    descString = StringUtils.encodeBase64String(Files.toByteArray(descFile));
  }

  @Test
  public void testShortMessageType()
  {
    @SuppressWarnings("unused") // expected to create parser without exception
    InlineDescriptorProtobufBytesDecoder decoder = new InlineDescriptorProtobufBytesDecoder(
        descString,
        "ProtoTestEvent"
    );
    decoder.initDescriptor();
  }

  @Test
  public void testLongMessageType()
  {
    @SuppressWarnings("unused") // expected to create parser without exception
    InlineDescriptorProtobufBytesDecoder decoder = new InlineDescriptorProtobufBytesDecoder(
        descString,
        "prototest.ProtoTestEvent"
    );
    decoder.initDescriptor();
  }

  @Test(expected = ParseException.class)
  public void testBadProto()
  {
    @SuppressWarnings("unused") // expected exception
    InlineDescriptorProtobufBytesDecoder decoder = new InlineDescriptorProtobufBytesDecoder(descString, "BadName");
    decoder.initDescriptor();
  }

  @Test(expected = IAE.class)
  public void testMalformedDescriptorBase64()
  {
    @SuppressWarnings("unused") // expected exception
    InlineDescriptorProtobufBytesDecoder decoder = new InlineDescriptorProtobufBytesDecoder("invalidString", "BadName");
    decoder.initDescriptor();
  }

  @Test(expected = ParseException.class)
  public void testMalformedDescriptorValidBase64InvalidDescriptor()
  {
    @SuppressWarnings("unused") // expected exception
    InlineDescriptorProtobufBytesDecoder decoder = new InlineDescriptorProtobufBytesDecoder(
        "aGVsbG8gd29ybGQ=",
        "BadName"
    );
    decoder.initDescriptor();
  }

  @Test
  public void testSingleDescriptorNoMessageType()
  {
    // For the backward compatibility, protoMessageType allows null when the desc file has only one message type.
    @SuppressWarnings("unused") // expected to create parser without exception
    InlineDescriptorProtobufBytesDecoder decoder = new InlineDescriptorProtobufBytesDecoder(descString, null);
    decoder.initDescriptor();
  }

  @Test
  public void testEquals()
  {
    InlineDescriptorProtobufBytesDecoder decoder = new InlineDescriptorProtobufBytesDecoder(descString, "ProtoTestEvent");
    decoder.initDescriptor();
    Descriptors.Descriptor descriptorA = decoder.getDescriptor();

    decoder = new InlineDescriptorProtobufBytesDecoder(descString, "ProtoTestEvent.Foo");
    decoder.initDescriptor();
    Descriptors.Descriptor descriptorB = decoder.getDescriptor();

    EqualsVerifier.forClass(InlineDescriptorProtobufBytesDecoder.class)
                  .usingGetClass()
                  .withIgnoredFields("descriptor")
                  .withPrefabValues(Descriptors.Descriptor.class, descriptorA, descriptorB)
                  .verify();
  }

}
