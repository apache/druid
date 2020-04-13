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

package org.apache.druid.segment.incremental;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.LongColumnSelector;

import javax.annotation.Nullable;

/**
 * IncrementalIndexRowHolder is a simple {@link #get}/{@link #set} holder of {@link IncrementalIndexRow}. It is used
 * to implement various machinery around {@link IncrementalIndex}, e. g. {@link
 * IncrementalIndexColumnSelectorFactory}, {@link IncrementalIndexRowIterator}, etc.
 *
 * By implementing {@link LongColumnSelector}, IncrementalIndexRowHolder plays the role of timestamp column selector, to
 * avoid unneeded level of indirection when timestamp column is selected in {@link
 * IncrementalIndexColumnSelectorFactory#makeColumnValueSelector(String)}.
 */
public class IncrementalIndexRowHolder implements LongColumnSelector
{
  @Nullable
  private IncrementalIndexRow currEntry = null;

  public IncrementalIndexRow get()
  {
    return currEntry;
  }

  public void set(IncrementalIndexRow currEntry)
  {
    this.currEntry = currEntry;
  }

  @Override
  public long getLong()
  {
    return currEntry.getTimestamp();
  }

  @Override
  public boolean isNull()
  {
    // Time column is never null
    return false;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }
}
