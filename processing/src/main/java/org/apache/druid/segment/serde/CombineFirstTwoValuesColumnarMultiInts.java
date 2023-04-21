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
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.IndexedIterable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

/**
 * A {@link ColumnarMultiInts} that delegates to an underyling instance, but combines the values 0 and 1 into 0.
 *
 * This class combines *values*, not *indexes*. So [[0, 1], [1, 2]] becomes [[0, 0], [0, 2]]. (The ones were replaced
 * with zeroes.)
 *
 * Provided to enable compatibility for segments written under {@link NullHandling#sqlCompatible()} mode but
 * read under {@link NullHandling#replaceWithDefault()} ()} mode.
 *
 * @see NullHandling#mustCombineNullAndEmpty(Indexed)
 */
public class CombineFirstTwoValuesColumnarMultiInts implements ColumnarMultiInts
{
  private final ColumnarMultiInts delegate;

  public CombineFirstTwoValuesColumnarMultiInts(ColumnarMultiInts delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public IndexedInts get(int index)
  {
    return new CombineFirstTwoValuesIndexedInts(delegate.get(index));
  }

  @Override
  public IndexedInts getUnshared(int index)
  {
    return new CombineFirstTwoValuesIndexedInts(delegate.getUnshared(index));
  }

  @Override
  public int size()
  {
    return delegate.size();
  }

  @Override
  public int indexOf(@Nullable IndexedInts value)
  {
    // No ColumnarMultiInts implement this method
    throw new UnsupportedOperationException("Reverse lookup not allowed.");
  }

  @Override
  public boolean isSorted()
  {
    return delegate.isSorted();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    delegate.inspectRuntimeShape(inspector);
  }

  @Override
  public Iterator<IndexedInts> iterator()
  {
    return IndexedIterable.create(this).iterator();
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }
}
