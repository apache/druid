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

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import java.util.Arrays;

/**
 * A {@link ColumnarInts} of a fixed length that returns the same dictionary id for every row. Used to back a
 * single-value dictionary-encoded column whose value is constant across all rows.
 */
public final class ConstantColumnarInts implements ColumnarInts
{
  private final int numRows;
  private final int value;

  public ConstantColumnarInts(int numRows, int value)
  {
    this.numRows = numRows;
    this.value = value;
  }

  @Override
  public int size()
  {
    return numRows;
  }

  @Override
  public int get(int index)
  {
    return value;
  }

  @Override
  public void get(int[] out, int offset, int start, int length)
  {
    Arrays.fill(out, offset, offset + length, value);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("value", value);
  }

  @Override
  public void close()
  {
    // nothing to close
  }
}
