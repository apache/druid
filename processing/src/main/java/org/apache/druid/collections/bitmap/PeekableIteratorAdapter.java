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
  final TIntIterator baseIterator;
  Integer mark;

  PeekableIteratorAdapter(TIntIterator iterator)
  {
    this.baseIterator = Preconditions.checkNotNull(iterator, "iterator");
  }

  PeekableIteratorAdapter(TIntIterator iterator, int mark)
  {
    this(iterator);
    this.mark = mark;
    this.advanceIfNeeded(mark);
  }

  @Override
  public void advanceIfNeeded(int i)
  {
    while ((mark == null || mark < i) && baseIterator.hasNext()) {
      mark = baseIterator.next();
    }
  }

  @Override
  public int peekNext()
  {
    Preconditions.checkArgument(mark != null || baseIterator.hasNext());
    if (mark == null) {
      mark = baseIterator.next();
    }
    return mark;
  }

  @Override
  public PeekableIntIterator clone()
  {
    return new PeekableIteratorAdapter(baseIterator.clone(), mark);
  }

  @Override
  public boolean hasNext()
  {
    return mark != null || baseIterator.hasNext();
  }

  @Override
  public int next()
  {
    if (mark != null) {
      final int currentBit = mark;
      mark = null;
      return currentBit;
    }
    return baseIterator.next();
  }
}
