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

package org.apache.druid.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.serde.ComplexMetricSerde;

/**
 * Wraps a {@link BaseObjectColumnValueSelector} and uses {@link ComplexMetricSerde#toBytes} to write complex objects.
 *
 * See {@link ComplexFieldReader} for format details.
 */
public class ComplexFieldWriter implements FieldWriter
{
  // Different from the values in NullHandling, to be consistent with other writers in this package.
  public static final byte NULL_BYTE = 0x00;
  public static final byte NOT_NULL_BYTE = 0x01;

  static final int HEADER_SIZE = Byte.BYTES /* null byte */ + Integer.BYTES /* length */;

  private final ComplexMetricSerde serde;
  private final BaseObjectColumnValueSelector<?> selector;

  ComplexFieldWriter(
      final ComplexMetricSerde serde,
      final BaseObjectColumnValueSelector<?> selector
  )
  {
    this.serde = serde;
    this.selector = selector;
  }

  @Override
  public long writeTo(final WritableMemory memory, final long position, final long maxSize)
  {
    final Object complexObject = selector.getObject();

    if (maxSize < HEADER_SIZE) {
      return -1;
    }

    if (complexObject == null) {
      memory.putByte(position, NULL_BYTE);
      memory.putInt(position + Byte.BYTES, 0);
      return HEADER_SIZE;
    } else {
      final byte[] bytes = serde.toBytes(complexObject);
      final int fieldLength = HEADER_SIZE + bytes.length;

      if (maxSize < fieldLength) {
        return -1;
      } else {
        memory.putByte(position, NOT_NULL_BYTE);
        memory.putInt(position + Byte.BYTES, bytes.length);
        memory.putByteArray(position + HEADER_SIZE, bytes, 0, bytes.length);
        return fieldLength;
      }
    }
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }
}
