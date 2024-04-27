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

package org.apache.druid.frame.field;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;

/**
 * Stores the memory locations in an array, and spits out the value pointed to by the memory location by pointer,
 * which is settable by the user
 */
public class IndexArrayFieldPointer implements ReadableFieldPointer
{
  private final LongArrayList indices;
  private final LongArrayList lengths;
  private int pointer = 0;

  public IndexArrayFieldPointer(final List<Long> indices, final List<Long> lengths)
  {
    this.indices = new LongArrayList(indices);
    this.lengths = new LongArrayList(lengths);
  }

  private int numIndices()
  {
    return indices.size();
  }

  public void setPointer(int newPointer)
  {
    assert newPointer >= 0 && newPointer < numIndices();
    this.pointer = newPointer;
  }

  @Override
  public long position()
  {
    return indices.getLong(pointer);
  }

  @Override
  public long length()
  {
    return lengths.getLong(pointer);
  }
}
