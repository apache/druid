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

package org.apache.druid.segment.shim;

import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;

/**
 * Adapter from {@link VectorCursor} to {@link Cursor}. Works by reading a batch and then stepping through it as
 * {@link Cursor#advance()} is called.
 */
public class ShimCursor implements Cursor
{
  private final ShimColumnSelectorFactory columnSelectorFactory;

  final VectorCursor vectorCursor;
  final VectorColumnSelectorFactory vectorColumnSelectorFactory;
  int currentIndexInVector = 0;

  public ShimCursor(VectorCursor vectorCursor)
  {
    this.vectorCursor = vectorCursor;
    this.vectorColumnSelectorFactory = vectorCursor.getColumnSelectorFactory();
    this.columnSelectorFactory = new ShimColumnSelectorFactory(this);
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return columnSelectorFactory;
  }

  @Override
  public void advance()
  {
    currentIndexInVector++;

    if (currentIndexInVector == vectorCursor.getCurrentVectorSize()) {
      vectorCursor.advance();
      currentIndexInVector = 0;
    }
  }

  @Override
  public void advanceUninterruptibly()
  {
    advance();
  }

  @Override
  public boolean isDone()
  {
    return vectorCursor.isDone();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    vectorCursor.reset();
  }
}
