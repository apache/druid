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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Segment that exposes a projection of its delegate: its columns, along with {@code virtualColumns}.
 *
 * @see ProjectCursorFactory
 */
public class ProjectSegment extends WrappedSegment
{
  private final VirtualColumns virtualColumns;

  public ProjectSegment(
      final Segment delegate,
      final VirtualColumns virtualColumns
  )
  {
    super(delegate);
    this.virtualColumns = virtualColumns;
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    if (CursorFactory.class.equals(clazz)) {
      return (T) new ProjectCursorFactory(delegate.as(CursorFactory.class), virtualColumns);
    } else if (TopNOptimizationInspector.class.equals(clazz)) {
      // A projection removes no rows, so whether every dictionary id is present is up to the delegate.
      return (T) delegate.as(TopNOptimizationInspector.class);
    } else if (RowCountInspector.class.equals(clazz)) {
      // A projection removes no rows, so we can return the underlying row count inspector.
      return (T) delegate.as(RowCountInspector.class);
    } else if (TimeBoundaryInspector.class.equals(clazz)) {
      // A projection cannot modify the __time column, so the underlying inspector is still valid.
      return (T) delegate.as(TimeBoundaryInspector.class);
    }
    return null;
  }

  @Override
  public String getDebugString()
  {
    return "project->" + delegate.getDebugString();
  }
}
