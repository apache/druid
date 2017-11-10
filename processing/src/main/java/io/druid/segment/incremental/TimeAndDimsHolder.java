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

package io.druid.segment.incremental;

/**
 * This interface is the core "pointer" interface that is used to create {@link io.druid.segment.ColumnValueSelector}s
 * over incremental index. It's counterpart for historical segments is {@link io.druid.segment.data.Offset}.
 */
public class TimeAndDimsHolder
{
  IncrementalIndex.TimeAndDims currEntry = null;

  public IncrementalIndex.TimeAndDims get()
  {
    return currEntry;
  }

  public void set(IncrementalIndex.TimeAndDims currEntry)
  {
    this.currEntry = currEntry;
  }

  /**
   * This method doesn't have well-defined semantics ("value" of what?), should be removed in favor of chaining
   * get().getRowIndex().
   */
  public int getValue()
  {
    return currEntry.getRowIndex();
  }
}
