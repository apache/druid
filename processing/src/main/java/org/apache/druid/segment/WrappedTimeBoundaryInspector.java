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

import com.google.common.base.Preconditions;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

/**
 * Wrapper for {@link TimeBoundaryInspector} used by {@link Segment} implementations that may filter out rows
 * from an underlying segment, but do not modify {@link ColumnHolder#TIME_COLUMN_NAME}.
 */
public class WrappedTimeBoundaryInspector implements TimeBoundaryInspector
{
  private final TimeBoundaryInspector delegate;

  private WrappedTimeBoundaryInspector(final TimeBoundaryInspector delegate)
  {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate");
  }

  @Nullable
  public static WrappedTimeBoundaryInspector create(@Nullable final TimeBoundaryInspector delegate)
  {
    if (delegate != null) {
      return new WrappedTimeBoundaryInspector(delegate);
    } else {
      return null;
    }
  }

  @Override
  public DateTime getMinTime()
  {
    return delegate.getMinTime();
  }

  @Override
  public DateTime getMaxTime()
  {
    return delegate.getMaxTime();
  }

  @Override
  public boolean isMinMaxExact()
  {
    // Always false, because rows may be filtered out.
    return false;
  }
}
