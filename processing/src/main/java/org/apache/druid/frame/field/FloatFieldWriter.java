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
import org.apache.druid.segment.BaseFloatColumnValueSelector;

/**
 * Wraps a {@link BaseFloatColumnValueSelector} and writes field values.
 *
 * @see NumericFieldWriter for the details of the byte-format that it writes as
 */
public class FloatFieldWriter extends NumericFieldWriter
{
  private final BaseFloatColumnValueSelector selector;

  public static FloatFieldWriter forPrimitive(final BaseFloatColumnValueSelector selector)
  {
    return new FloatFieldWriter(selector, false);
  }

  public static FloatFieldWriter forArray(final BaseFloatColumnValueSelector selector)
  {
    return new FloatFieldWriter(selector, true);
  }

  private FloatFieldWriter(final BaseFloatColumnValueSelector selector, final boolean forArray)
  {
    super(selector, forArray);
    this.selector = selector;
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }

  @Override
  public int getNumericSizeBytes()
  {
    return Float.BYTES;
  }

  @Override
  public void writeSelectorToMemory(WritableMemory memory, long position)
  {
    writeToMemory(memory, position, selector.getFloat());
  }

  @Override
  public void writeNullToMemory(WritableMemory memory, long position)
  {
    writeToMemory(memory, position, 0);
  }

  private void writeToMemory(WritableMemory memory, long position, float value)
  {
    memory.putInt(position, TransformUtils.transformFromFloat(value));
  }
}
