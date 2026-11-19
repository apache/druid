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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DynamicMessage;
import org.apache.druid.data.input.protobuf.Proto3TestEventWrapper.Proto3TestEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ProtobufConverterTest
{
  @Test
  public void testProto3ImplicitPresenceFieldsKeepDefaultValues() throws Exception
  {
    Assertions.assertEquals(
        ImmutableMap.of(
            "someInt", 0,
            "someString", "",
            "category", "CATEGORY_ZERO"
        ),
        convert(Proto3TestEvent.getDefaultInstance())
    );
  }

  @Test
  public void testProto3NestedMessageKeepsDefaultValues() throws Exception
  {
    final Proto3TestEvent event =
        Proto3TestEvent.newBuilder().setNested(Proto3TestEvent.Nested.getDefaultInstance()).build();

    Assertions.assertEquals(ImmutableMap.of("value", 0), convert(event).get("nested"));
  }

  private static Map<String, Object> convert(Proto3TestEvent event) throws Exception
  {
    // Round-trip through bytes like ingestion does.
    return ProtobufConverter.convertMessage(DynamicMessage.parseFrom(Proto3TestEvent.getDescriptor(), event.toByteArray()));
  }
}
