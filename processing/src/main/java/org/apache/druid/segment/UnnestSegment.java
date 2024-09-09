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

package org.apache.druid.segment;

import org.apache.druid.query.filter.DimFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UnnestSegment extends WrappedSegmentReference
{
  private final VirtualColumn unnestColumn;
  @Nullable
  private final DimFilter filter;

  public UnnestSegment(
      SegmentReference delegate,
      VirtualColumn unnestColumn,
      @Nullable DimFilter filter
  )
  {
    super(delegate);
    this.unnestColumn = unnestColumn;
    this.filter = filter;
  }

  @Override
  public CursorFactory asCursorFactory()
  {
    return new UnnestCursorFactory(delegate.asCursorFactory(), unnestColumn, filter);
  }

  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    if (TopNOptimizationInspector.class.equals(clazz)) {
      return (T) new SimpleTopNOptimizationInspector(filter == null);
    }
    return super.as(clazz);
  }
}
