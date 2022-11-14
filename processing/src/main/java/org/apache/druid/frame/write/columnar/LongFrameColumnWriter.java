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

package org.apache.druid.frame.write.columnar;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.segment.BaseLongColumnValueSelector;

public class LongFrameColumnWriter implements FrameColumnWriter
{
  private final BaseLongColumnValueSelector selector;
  private final LongFrameMaker maker;

  LongFrameColumnWriter(
      BaseLongColumnValueSelector selector,
      MemoryAllocator allocator,
      boolean hasNulls
  )
  {
    this.selector = selector;
    this.maker = new LongFrameMaker(allocator, hasNulls);
  }

  @Override
  public boolean addSelection()
  {
    if (selector.isNull()) {
      return maker.addNull();
    } else {
      return maker.add(selector.getLong());
    }
  }

  @Override
  public void undo()
  {
    maker.undo();
  }

  @Override
  public long size()
  {
    return maker.size();
  }

  @Override
  public long writeTo(final WritableMemory memory, final long startPosition)
  {
    return maker.writeTo(memory, startPosition);
  }

  @Override
  public void close()
  {
    maker.close();
  }
}
