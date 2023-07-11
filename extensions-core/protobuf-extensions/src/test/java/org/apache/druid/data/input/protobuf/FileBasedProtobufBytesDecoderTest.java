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

import com.google.protobuf.Descriptors;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FileBasedProtobufBytesDecoderTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testShortMessageType()
  {
    //configure parser with desc file, and specify which file name to use
    @SuppressWarnings("unused") // expected to create parser without exception
    FileBasedProtobufBytesDecoder decoder = new FileBasedProtobufBytesDecoder("prototest.desc", "ProtoTestEvent");
    decoder.initDescriptor();
  }

  @Test
  public void testLongMessageType()
  {
    //configure parser with desc file, and specify which file name to use
    @SuppressWarnings("unused") // expected to create parser without exception
    FileBasedProtobufBytesDecoder decoder = new FileBasedProtobufBytesDecoder("prototest.desc", "prototest.ProtoTestEvent");
    decoder.initDescriptor();
  }

  @Test(expected = ParseException.class)
  public void testBadProto()
  {
    //configure parser with desc file
    @SuppressWarnings("unused") // expected exception
    FileBasedProtobufBytesDecoder decoder = new FileBasedProtobufBytesDecoder("prototest.desc", "BadName");
    decoder.initDescriptor();
  }

  @Test(expected = ParseException.class)
  public void testMalformedDescriptorUrl()
  {
    //configure parser with non existent desc file
    @SuppressWarnings("unused") // expected exception
    FileBasedProtobufBytesDecoder decoder = new FileBasedProtobufBytesDecoder("file:/nonexist.desc", "BadName");
    decoder.initDescriptor();
  }

  @Test
  public void testSingleDescriptorNoMessageType()
  {
    // For the backward compatibility, protoMessageType allows null when the desc file has only one message type.
    @SuppressWarnings("unused") // expected to create parser without exception
    FileBasedProtobufBytesDecoder decoder = new FileBasedProtobufBytesDecoder("prototest.desc", null);
    decoder.initDescriptor();
  }

  @Test
  public void testEquals()
  {
    FileBasedProtobufBytesDecoder decoder = new FileBasedProtobufBytesDecoder("prototest.desc", "ProtoTestEvent");
    decoder.initDescriptor();
    Descriptors.Descriptor descriptorA = decoder.getDescriptor();

    decoder = new FileBasedProtobufBytesDecoder("prototest.desc", "ProtoTestEvent.Foo");
    decoder.initDescriptor();
    Descriptors.Descriptor descriptorB = decoder.getDescriptor();

    EqualsVerifier.forClass(FileBasedProtobufBytesDecoder.class)
                  .usingGetClass()
                  .withIgnoredFields("descriptor")
                  .withPrefabValues(Descriptors.Descriptor.class, descriptorA, descriptorB)
                  .verify();
  }
}
