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

package org.apache.druid.segment.serde;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;

/**
 * A {@link IndexedInts} that delegates to an underyling instance, but combines the values 0 and 1 into 0, and shifts
 * all other values down by one. For example:
 *
 * - [2, 0, 1] => [1, 0, 0]
 * - [3, 2, 1] => [2, 1, 0]
 * - [0, 1, 0] => [0, 0, 0]
 *
 * Provided to enable compatibility for segments written under {@link NullHandling#sqlCompatible()} mode but
 * read under {@link NullHandling#replaceWithDefault()} mode.
 *
 * @see NullHandling#mustCombineNullAndEmptyInDictionary(Indexed)
 */
public class CombineFirstTwoValuesIndexedInts implements IndexedInts
{
  private static final int ZERO_ID = 0;

  IndexedInts delegate;

  public CombineFirstTwoValuesIndexedInts(IndexedInts delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public int size()
  {
    return delegate.size();
  }

  @Override
  public int get(int index)
  {
    final int i = delegate.get(index);
    if (i == ZERO_ID) {
      return i;
    } else {
      return i - 1;
    }
  }

  @Override
  public void get(int[] out, int start, int length)
  {
    delegate.get(out, start, length);

    for (int i = 0; i < length; i++) {
      if (out[i] != ZERO_ID) {
        out[i]--;
      }
    }
  }

  @Override
  public void get(int[] out, int[] indexes, int length)
  {
    delegate.get(out, indexes, length);

    for (int i = 0; i < length; i++) {
      if (out[i] != ZERO_ID) {
        out[i]--;
      }
    }
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    delegate.inspectRuntimeShape(inspector);
  }
}
