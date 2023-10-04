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

import com.google.common.base.Preconditions;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.PeekableIntIterator;

public final class BatchIteratorAdapter implements BatchIterator
{
  private final PeekableIntIterator iterator;

  public BatchIteratorAdapter(PeekableIntIterator iterator)
  {
    this.iterator = Preconditions.checkNotNull(iterator, "iterator");
  }

  @Override
  public int nextBatch(int[] buffer)
  {
    int i;
    for (i = 0; i < buffer.length && iterator.hasNext(); i++) {
      buffer[i] = iterator.next();
    }

    return i;
  }

  @Override
  public boolean hasNext()
  {
    return iterator.hasNext();
  }

  @Override
  public void advanceIfNeeded(int target)
  {
    iterator.advanceIfNeeded(target);
  }

  @Override
  public BatchIterator clone()
  {
    // It's okay to make a "new BatchIteratorAdapter" instead of calling super.clone(), since this class is final.
    return new BatchIteratorAdapter(iterator.clone());
  }
}
