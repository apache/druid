/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;


import java.util.Iterator;

/**
 */
public class IndexedIntsIterator implements Iterator<Integer>
{
  private final IndexedInts baseInts;
  private final int size;

  int currIndex = 0;

  public IndexedIntsIterator(
      IndexedInts baseInts
  )
  {
    this.baseInts = baseInts;

    size = baseInts.size();
  }

  @Override
  public boolean hasNext()
  {
    return currIndex < size;
  }

  @Override
  public Integer next()
  {
    return baseInts.get(currIndex++);
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
