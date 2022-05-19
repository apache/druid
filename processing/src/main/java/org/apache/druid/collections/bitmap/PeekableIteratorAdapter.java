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
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;

public class PeekableIteratorAdapter<TIntIterator extends IntIterator> implements PeekableIntIterator
{
  static final int NOT_SET = -1;
  final TIntIterator baseIterator;
  int mark = NOT_SET;

  PeekableIteratorAdapter(TIntIterator iterator)
  {
    this.baseIterator = Preconditions.checkNotNull(iterator, "iterator");
  }

  @Override
  public void advanceIfNeeded(int i)
  {
    while (mark < i && baseIterator.hasNext()) {
      mark = baseIterator.next();
    }
    if (mark < i) {
      mark = NOT_SET;
    }
  }

  @Override
  public int peekNext()
  {
    if (mark == NOT_SET) {
      mark = baseIterator.next();
    }
    return mark;
  }

  @Override
  public PeekableIntIterator clone()
  {
    throw new UnsupportedOperationException(
        "PeekableIteratorAdapter.clone is not implemented, but this should not happen"
    );
  }

  @Override
  public boolean hasNext()
  {
    return mark != NOT_SET || baseIterator.hasNext();
  }

  @Override
  public int next()
  {
    if (mark != NOT_SET) {
      final int currentBit = mark;
      mark = NOT_SET;
      return currentBit;
    }
    return baseIterator.next();
  }
}
