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
import org.apache.druid.segment.BaseLongColumnValueSelector;

/**
 * Wraps a {@link BaseLongColumnValueSelector} and writes individual values into frame rows.
 *
 * @see NumericFieldWriter for the details of the byte-format that it writes as
 */
public class LongFieldWriter extends NumericFieldWriter
{
  private final BaseLongColumnValueSelector selector;

  public static LongFieldWriter forPrimitive(final BaseLongColumnValueSelector selector)
  {
    return new LongFieldWriter(selector, false);
  }

  static LongFieldWriter forArray(final BaseLongColumnValueSelector selector)
  {
    return new LongFieldWriter(selector, true);
  }


  // Different from the values in NullHandling, since we want to be able to sort as bytes, and we want
  // nulls to come before non-nulls.
  private LongFieldWriter(final BaseLongColumnValueSelector selector, final boolean forArray)
  {
    super(selector, forArray);
    this.selector = selector;
  }

  @Override
  public int getNumericSizeBytes()
  {
    return Long.BYTES;
  }

  @Override
  public void writeSelectorToMemory(WritableMemory memory, long position)
  {
    writeToMemory(memory, position, selector.getLong());
  }

  @Override
  public void writeNullToMemory(WritableMemory memory, long position)
  {
    writeToMemory(memory, position, 0);

  }

  private void writeToMemory(WritableMemory memory, long position, long value)
  {
    memory.putLong(position, TransformUtils.transformFromLong(value));
  }

}
