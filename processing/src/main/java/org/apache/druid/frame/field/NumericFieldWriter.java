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
import org.apache.druid.segment.BaseNullableColumnValueSelector;

public abstract class NumericFieldWriter implements FieldWriter
{
  public static final byte NULL_BYTE = 0x00;
  public static final byte NOT_NULL_BYTE = 0x01;

  public static final byte ARRAY_NULL_BYTE = 0x01;
  public static final byte ARRAY_NOT_NULL_BYTE = 0x01;

  private final BaseNullableColumnValueSelector selector;
  private final byte nullIndicatorByte;
  private final byte notNullIndicatorByte;

  public NumericFieldWriter(
      final BaseNullableColumnValueSelector selector,
      final boolean forArray
  )
  {
    this.selector = selector;
    if (!forArray) {
      this.nullIndicatorByte = NULL_BYTE;
      this.notNullIndicatorByte = NOT_NULL_BYTE;
    } else {
      this.nullIndicatorByte = ARRAY_NULL_BYTE;
      this.notNullIndicatorByte = ARRAY_NOT_NULL_BYTE;
    }
  }

  @Override
  public long writeTo(WritableMemory memory, long position, long maxSize)
  {
    int size = getNumericSize() + Byte.BYTES;

    if (maxSize < size) {
      return -1;
    }

    if (selector.isNull()) {
      memory.putByte(position, nullIndicatorByte);
      writeNullToMemory(memory, position + Byte.BYTES);
    } else {
      memory.putByte(position, notNullIndicatorByte);
      writeSelectorToMemory(memory, position + Byte.BYTES);
    }

    return size;
  }

  @Override
  public void close()
  {
    // Nothing to do
  }

  public abstract int getNumericSize();

  public abstract void writeSelectorToMemory(WritableMemory memory, long position);

  public abstract void writeNullToMemory(WritableMemory memory, long position);
}
