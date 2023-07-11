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

package org.apache.druid.frame.segment.columnar;

import org.apache.druid.frame.Frame;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorOffset;

import java.io.IOException;

/**
 * A {@link VectorCursor} that is based on a {@link Frame}.
 *
 * This class is only used for columnar frames. It is not used for row-based frames.
 */
public class FrameVectorCursor implements VectorCursor
{
  private final VectorOffset offset;
  private final VectorColumnSelectorFactory columnSelectorFactory;
  private final Closer closer;

  FrameVectorCursor(
      final VectorOffset offset,
      final VectorColumnSelectorFactory columnSelectorFactory,
      final Closer closer
  )
  {
    this.offset = offset;
    this.columnSelectorFactory = columnSelectorFactory;
    this.closer = closer;
  }

  @Override
  public VectorColumnSelectorFactory getColumnSelectorFactory()
  {
    return columnSelectorFactory;
  }

  @Override
  public void advance()
  {
    offset.advance();
    BaseQuery.checkInterrupted();
  }

  @Override
  public boolean isDone()
  {
    return offset.isDone();
  }

  @Override
  public void reset()
  {
    offset.reset();
  }

  @Override
  public void close()
  {
    try {
      closer.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getMaxVectorSize()
  {
    return offset.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    return offset.getCurrentVectorSize();
  }
}
