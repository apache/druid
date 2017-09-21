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

/**
 * An IndexedInts that always returns a row containing a single zero.
 */
public class ZeroIndexedInts implements IndexedInts
{
  private final static ZeroIndexedInts INSTANCE = new ZeroIndexedInts();

  private ZeroIndexedInts()
  {
    // Singleton.
  }

  public static ZeroIndexedInts instance()
  {
    return INSTANCE;
  }

  @Override
  public int size()
  {
    return 1;
  }

  @Override
  public int get(int index)
  {
    // Skip range check in production, assume "index" was 0 like it really should have been.
    assert index == 0;
    return 0;
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
