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
 * Reads values written by {@link FloatFieldWriter}.
 *
 * Values are sortable as bytes without decoding.
 *
 * Format:
 *
 * - 1 byte: {@link FloatFieldWriter#NULL_BYTE} or {@link FloatFieldWriter#NOT_NULL_BYTE}
 * - 4 bytes: encoded float, using {@link TransformUtils#transformFromFloat(float)}
 */
public class FloatFieldReader extends NumericFieldReader<Float>
{
  FloatFieldReader(final boolean forArray)
  {
    super(forArray);
  }

  @Override
  public ValueType getValueType()
  {
    return ValueType.FLOAT;
  }

  @Override
  public Class<? extends Float> getClassOfObject()
  {
    return Float.class;
  }

  @Override
  public Float getValueFromMemory(Memory memory, long position)
  {
    return TransformUtils.detransformToFloat(memory.getInt(position));
  }
}
