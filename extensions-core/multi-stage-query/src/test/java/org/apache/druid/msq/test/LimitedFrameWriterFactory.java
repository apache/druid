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

package org.apache.druid.msq.test;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.RowSignature;

public class LimitedFrameWriterFactory implements FrameWriterFactory
{
  private final FrameWriterFactory baseFactory;
  private final int rowLimit;

  /**
   * Wraps a {@link FrameWriterFactory}, creating a new factory that returns {@link FrameWriter} which write
   * a limited number of rows.
   */
  public LimitedFrameWriterFactory(FrameWriterFactory baseFactory, int rowLimit)
  {
    this.baseFactory = baseFactory;
    this.rowLimit = rowLimit;
  }

  @Override
  public FrameWriter newFrameWriter(ColumnSelectorFactory columnSelectorFactory)
  {
    return new LimitedFrameWriter(baseFactory.newFrameWriter(columnSelectorFactory), rowLimit);
  }

  @Override
  public long allocatorCapacity()
  {
    return baseFactory.allocatorCapacity();
  }

  @Override
  public RowSignature signature()
  {
    return baseFactory.signature();
  }

  @Override
  public FrameType frameType()
  {
    return baseFactory.frameType();
  }

  private static class LimitedFrameWriter implements FrameWriter
  {
    private final FrameWriter baseWriter;
    private final int rowLimit;

    public LimitedFrameWriter(FrameWriter baseWriter, int rowLimit)
    {
      this.baseWriter = baseWriter;
      this.rowLimit = rowLimit;
    }

    @Override
    public boolean addSelection()
    {
      if (baseWriter.getNumRows() >= rowLimit) {
        return false;
      } else {
        return baseWriter.addSelection();
      }
    }

    @Override
    public int getNumRows()
    {
      return baseWriter.getNumRows();
    }

    @Override
    public long getTotalSize()
    {
      return baseWriter.getTotalSize();
    }

    @Override
    public long writeTo(WritableMemory memory, long position)
    {
      return baseWriter.writeTo(memory, position);
    }

    @Override
    public void close()
    {
      baseWriter.close();
    }
  }

}
