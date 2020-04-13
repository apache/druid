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
 * Reusable IndexedInts that returns sequences [0, 1, ..., N].
 */
public class RangeIndexedInts implements IndexedInts
{
  private int size;

  public RangeIndexedInts()
  {
  }

  public void setSize(int size)
  {
    if (size < 0) {
      throw new IAE("Size[%d] must be non-negative", size);
    }
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
      throw new IAE("index[%d] >= size[%d] or < 0", index, size);
    }
    return index;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }
}
