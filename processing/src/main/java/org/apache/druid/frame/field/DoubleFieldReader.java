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
 * Reads values written by {@link DoubleFieldWriter}.
 *
 * Values are sortable as bytes without decoding.
 *
 * Format:
 *
 * - 1 byte: {@link DoubleFieldWriter#NULL_BYTE} or {@link DoubleFieldWriter#NOT_NULL_BYTE}
 * - 8 bytes: encoded double, using {@link TransformUtils#transformFromDouble(double)}
 */
public class DoubleFieldReader extends NumericFieldReader<Double>
{
  DoubleFieldReader()
  {
  }

  @Override
  public ValueType getValueType()
  {
    return ValueType.DOUBLE;
  }

  @Override
  public Class<? extends Double> getClassOfObject()
  {
    return Double.class;
  }

  @Override
  public Double getValueFromMemory(Memory memory, long position)
  {
    return TransformUtils.detransformToDouble(memory.getLong(position));
  }
}
