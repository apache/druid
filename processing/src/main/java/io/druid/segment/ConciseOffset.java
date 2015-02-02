/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment;

import io.druid.segment.data.Offset;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import it.uniroma3.mat.extendedset.intset.IntSet;

/**
 */
public class ConciseOffset implements Offset
{
  private static final int INVALID_VALUE = -1;

  IntSet.IntIterator itr;
  private final ImmutableConciseSet invertedIndex;
  private volatile int val;

  public ConciseOffset(ImmutableConciseSet invertedIndex)
  {
    this.invertedIndex = invertedIndex;
    this.itr = invertedIndex.iterator();
    increment();
  }

  private ConciseOffset(ConciseOffset otherOffset)
  {
    this.invertedIndex = otherOffset.invertedIndex;
    this.itr = otherOffset.itr.clone();
    this.val = otherOffset.val;
  }

  @Override
  public void increment()
  {
    if (itr.hasNext()) {
      val = itr.next();
    } else {
      val = INVALID_VALUE;
    }
  }

  @Override
  public boolean withinBounds()
  {
    return val > INVALID_VALUE;
  }

  @Override
  public Offset clone()
  {
    if (invertedIndex == null || invertedIndex.size() == 0) {
      return new ConciseOffset(new ImmutableConciseSet());
    }

    return new ConciseOffset(this);
  }

  @Override
  public int getOffset()
  {
    return val;
  }
}
