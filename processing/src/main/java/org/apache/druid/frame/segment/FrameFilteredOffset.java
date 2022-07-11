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

package org.apache.druid.frame.segment;

import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.data.ReadableOffset;

/**
 * Copy of {@link org.apache.druid.segment.FilteredOffset} that does not require bitmap indexes.
 *
 * In a future where {@link org.apache.druid.segment.FilteredOffset} is opened up for usage outside of regular segments,
 * this class could be removed and usages could be migrated to {@link org.apache.druid.segment.FilteredOffset}.
 */
public class FrameFilteredOffset extends Offset
{
  private final Offset baseOffset;
  private final ValueMatcher filterMatcher;

  public FrameFilteredOffset(
      final Offset baseOffset,
      final ColumnSelectorFactory columnSelectorFactory,
      final Filter postFilter
  )
  {
    this.baseOffset = baseOffset;
    this.filterMatcher = postFilter.makeMatcher(columnSelectorFactory);
    incrementIfNeededOnCreationOrReset();
  }

  @Override
  public void increment()
  {
    while (!Thread.currentThread().isInterrupted()) {
      baseOffset.increment();
      if (!baseOffset.withinBounds() || filterMatcher.matches()) {
        return;
      }
    }
  }

  @Override
  public boolean withinBounds()
  {
    return baseOffset.withinBounds();
  }

  @Override
  public void reset()
  {
    baseOffset.reset();
    incrementIfNeededOnCreationOrReset();
  }

  private void incrementIfNeededOnCreationOrReset()
  {
    if (baseOffset.withinBounds()) {
      if (!filterMatcher.matches()) {
        increment();
        // increment() returns early if it detects the current Thread is interrupted. It will leave this
        // FilteredOffset in an illegal state, because it may point to an offset that should be filtered. So must
        // call BaseQuery.checkInterrupted() and thereby throw a QueryInterruptedException.
        BaseQuery.checkInterrupted();
      }
    }
  }

  @Override
  public ReadableOffset getBaseReadableOffset()
  {
    return baseOffset.getBaseReadableOffset();
  }

  /**
   * See {@link org.apache.druid.segment.FilteredOffset#clone()} for notes.
   */
  @Override
  public Offset clone()
  {
    throw new UnsupportedOperationException("FrameFilteredOffset cannot be cloned");
  }

  @Override
  public int getOffset()
  {
    return baseOffset.getOffset();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("baseOffset", baseOffset);
    inspector.visit("filterMatcher", filterMatcher);
  }
}
