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

package org.apache.druid.segment.data;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

/**
 * Reusable IndexedInts, that could represent a sub-sequence ("slice") in a larger IndexedInts object. Used in
 * {@link CompressedVSizeColumnarMultiIntsSupplier} implementation.
 *
 * Unsafe for concurrent use from multiple threads.
 */
public final class SliceIndexedInts implements IndexedInts
{
  private final IndexedInts base;
  private int offset = -1;
  private int size = -1;

  public SliceIndexedInts(IndexedInts base)
  {
    this.base = base;
  }

  public void setValues(int offset, int size)
  {
    this.offset = offset;
    this.size = size;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public int get(int index)
  {
    if (index < 0 || index >= size) {
      throw new IAE("Index[%d] >= size[%d] or < 0", index, size);
    }
    return base.get(offset + index);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("base", base);
  }
}
