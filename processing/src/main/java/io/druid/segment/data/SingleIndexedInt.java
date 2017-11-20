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

import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import java.io.IOException;

public final class SingleIndexedInt implements IndexedInts
{
  private static final int CACHE_SIZE = 128;
  private static final SingleIndexedInt[] CACHE = new SingleIndexedInt[CACHE_SIZE];

  static {
    for (int i = 0; i < CACHE_SIZE; i++) {
      CACHE[i] = new SingleIndexedInt(i);
    }
  }

  private final int value;

  private SingleIndexedInt(int value)
  {
    this.value = value;
  }

  public static SingleIndexedInt of(int value)
  {
    if (value >= 0 && value < CACHE_SIZE) {
      return CACHE[value];
    } else {
      return new SingleIndexedInt(value);
    }
  }

  @Override
  public int size()
  {
    return 1;
  }

  @Override
  public int get(int i)
  {
    if (i != 0) {
      throw new IllegalArgumentException(i + " != 0");
    }
    return value;
  }

  @Override
  public void close() throws IOException
  {
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }
}
