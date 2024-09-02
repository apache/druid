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

import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * Simple {@link SegmentReference} implementation for a segment that wraps a base segment such as
 * {@link UnnestSegment} or {@link FilteredSegment}
 */
public abstract class WrappedSegmentReference implements SegmentReference
{
  protected final SegmentReference delegate;

  public WrappedSegmentReference(
      SegmentReference delegate
  )
  {
    this.delegate = delegate;
  }

  @Override
  public Optional<Closeable> acquireReferences()
  {
    return delegate.acquireReferences();
  }

  @Override
  public SegmentId getId()
  {
    return delegate.getId();
  }

  @Override
  public Interval getDataInterval()
  {
    return delegate.getDataInterval();
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    return delegate.asQueryableIndex();
  }

  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    if (TimeBoundaryInspector.class.equals(clazz)) {
      return (T) WrappedTimeBoundaryInspector.create(delegate.as(TimeBoundaryInspector.class));
    } else {
      return SegmentReference.super.as(clazz);
    }
  }

  @Override
  public boolean isTombstone()
  {
    return delegate.isTombstone();
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }

  @Override
  public String asString()
  {
    return delegate.asString();
  }
}

