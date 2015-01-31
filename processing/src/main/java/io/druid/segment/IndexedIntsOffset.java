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

import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;

/**
*/
class IndexedIntsOffset implements Offset
{
  int currRow;
  private final IndexedInts invertedIndex;

  public IndexedIntsOffset(IndexedInts invertedIndex)
  {
    this.invertedIndex = invertedIndex;
    currRow = 0;
  }

  @Override
  public void increment()
  {
    ++currRow;
  }

  @Override
  public boolean withinBounds()
  {
    return currRow < invertedIndex.size();
  }

  @Override
  public Offset clone()
  {
    final IndexedIntsOffset retVal = new IndexedIntsOffset(invertedIndex);
    retVal.currRow = currRow;
    return retVal;
  }

  @Override
  public int getOffset()
  {
    return invertedIndex.get(currRow);
  }
}
