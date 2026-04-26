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

package org.apache.druid.query.rowsandcols.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.WireTransferable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;

public class WireTransferableContextTest
{
  @Test
  public void test_defaultLegacyFrameSerialization()
  {
    Assertions.assertTrue(WireTransferableContext.DEFAULT_LEGACY_FRAME_SERIALIZATION);
  }

  @Test
  public void test_constructor_andGetters()
  {
    final ObjectMapper mockMapper = Mockito.mock(ObjectMapper.class);
    final WireTransferable.ConcreteDeserializer mockDeserializer =
        Mockito.mock(WireTransferable.ConcreteDeserializer.class);

    final WireTransferableContext context = new WireTransferableContext(mockMapper, mockDeserializer, true);

    Assertions.assertSame(mockMapper, context.smileMapper());
    Assertions.assertSame(mockDeserializer, context.concreteDeserializer());
    Assertions.assertTrue(context.useLegacyFrameSerialization());
  }

  @Test
  public void test_constructor_withNullValues()
  {
    final WireTransferableContext context = new WireTransferableContext(null, null, false);

    Assertions.assertNull(context.smileMapper());
    Assertions.assertNull(context.concreteDeserializer());
    Assertions.assertFalse(context.useLegacyFrameSerialization());
  }

  @Test
  public void test_useLegacyFrameSerialization_true()
  {
    final WireTransferableContext context = new WireTransferableContext(null, null, true);
    Assertions.assertTrue(context.useLegacyFrameSerialization());
  }

  @Test
  public void test_useLegacyFrameSerialization_false()
  {
    final WireTransferableContext context = new WireTransferableContext(null, null, false);
    Assertions.assertFalse(context.useLegacyFrameSerialization());
  }

  @Test
  public void test_serializedBytes() throws IOException
  {
    final ObjectMapper mockMapper = Mockito.mock(ObjectMapper.class);
    final WireTransferable mockWireTransferable = Mockito.mock(WireTransferable.class);
    final WireTransferable.ByteArrayOffsetAndLen expectedResult =
        new WireTransferable.ByteArrayOffsetAndLen(new byte[]{1, 2, 3}, 0, 3);

    Mockito.when(mockWireTransferable.serializedBytes(mockMapper)).thenReturn(expectedResult);

    final WireTransferableContext context = new WireTransferableContext(mockMapper, null, true);
    final WireTransferable.ByteArrayOffsetAndLen result = context.serializedBytes(mockWireTransferable);

    Assertions.assertSame(expectedResult, result);
    Mockito.verify(mockWireTransferable).serializedBytes(mockMapper);
  }

  @Test
  public void test_deserialize()
  {
    final WireTransferable.ConcreteDeserializer mockDeserializer =
        Mockito.mock(WireTransferable.ConcreteDeserializer.class);
    final RowsAndColumns mockRac = Mockito.mock(RowsAndColumns.class);
    final ByteBuffer buffer = ByteBuffer.allocate(10);

    Mockito.when(mockDeserializer.deserialize(buffer)).thenReturn(mockRac);

    final WireTransferableContext context = new WireTransferableContext(null, mockDeserializer, true);
    final RowsAndColumns result = context.deserialize(buffer);

    Assertions.assertSame(mockRac, result);
    Mockito.verify(mockDeserializer).deserialize(buffer);
  }

  @Test
  public void test_deserialize_withNullDeserializer_throwsException()
  {
    final WireTransferableContext context = new WireTransferableContext(null, null, true);
    final ByteBuffer buffer = ByteBuffer.allocate(10);

    final DruidException exception = Assertions.assertThrows(
        DruidException.class,
        () -> context.deserialize(buffer)
    );
    Assertions.assertTrue(exception.getMessage().contains("Cannot deserialize"));
    Assertions.assertTrue(exception.getMessage().contains("no concreteDeserializer"));
  }
}
