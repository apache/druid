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

import org.apache.datasketches.memory.Memory;
import org.apache.druid.segment.column.ValueType;

/**
 * Reads values written by {@link LongFieldWriter}.
 *
 * Values are sortable as bytes without decoding.
 *
 * Format:
 *
 * - 1 byte: {@link LongFieldWriter#NULL_BYTE} or {@link LongFieldWriter#NOT_NULL_BYTE}
 * - 8 bytes: encoded long: big-endian order, with sign flipped
 */
public class LongFieldReader extends NumericFieldReader<Long>
{
  LongFieldReader()
  {
  }

  @Override
  public ValueType getValueType()
  {
    return ValueType.LONG;
  }

  @Override
  public Class<? extends Long> getClassOfObject()
  {
    return Long.class;
  }

  @Override
  public Long getValueFromMemory(Memory memory, long position)
  {
    return TransformUtils.detransformToLong(memory.getLong(position));
  }
}
