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

package org.apache.druid.collections.bitmap;

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.IntIterator;

/**
 * Wraps a batch iterator so it can be used as if it is an IntIterator (at a performance cost)
 */
public class BatchIteratorWrapperIterator implements IntIterator
{
  private int i;
  private int mark;
  private int[] buffer;
  private BatchIterator delegate;

  private BatchIteratorWrapperIterator(BatchIterator delegate, int i, int mark, int[] buffer)
  {
    this.delegate = delegate;
    this.i = i;
    this.mark = mark;
    this.buffer = buffer;
  }

  /**
   * Wraps the batch iterator.
   * @param delegate the batch iterator to do the actual iteration
   */
  BatchIteratorWrapperIterator(BatchIterator delegate, int batchSize)
  {
    this(delegate, 0, -1, new int[batchSize]);
  }

  @Override
  public boolean hasNext()
  {
    if (i < mark) {
      return true;
    }
    if (!delegate.hasNext() || (mark = delegate.nextBatch(buffer)) == 0) {
      return false;
    }
    i = 0;
    return true;
  }

  @Override
  public int next()
  {
    return buffer[i++];
  }

  @Override
  public IntIterator clone()
  {
    try {
      BatchIteratorWrapperIterator it = (BatchIteratorWrapperIterator) super.clone();
      it.delegate = delegate.clone();
      it.buffer = buffer.clone();
      return it;
    }
    catch (CloneNotSupportedException e) {
      // won't happen
      throw new IllegalStateException();
    }
  }
}
