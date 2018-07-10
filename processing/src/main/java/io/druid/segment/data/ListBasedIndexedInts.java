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

import it.unimi.dsi.fastutil.ints.IntIterator;

import java.io.IOException;
import java.util.List;

/**
 */
public class ListBasedIndexedInts implements IndexedInts
{
  private final List<Integer> expansion;

  public ListBasedIndexedInts(List<Integer> expansion) {this.expansion = expansion;}

  @Override
  public int size()
  {
    return expansion.size();
  }

  @Override
  public int get(int index)
  {
    return expansion.get(index);
  }

  @Override
  public IntIterator iterator()
  {
    return new IndexedIntsIterator(this);
  }

  @Override
  public void fill(int index, int[] toFill)
  {
    throw new UnsupportedOperationException("fill not supported");
  }

  @Override
  public void close() throws IOException
  {

  }
}
