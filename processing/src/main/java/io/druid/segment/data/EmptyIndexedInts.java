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

package io.druid.segment.data;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Iterator;

/**
 */
public class EmptyIndexedInts implements IndexedInts
{
  public static EmptyIndexedInts instance = new EmptyIndexedInts();

  @Override
  public int size()
  {
    return 0;
  }

  @Override
  public int get(int index)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return ImmutableList.<Integer>of().iterator();
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
