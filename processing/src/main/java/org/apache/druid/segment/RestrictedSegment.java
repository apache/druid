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
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * A wrapped {@link SegmentReference} with a {@link DimFilter} restriction. The restriction must be applied on the
 * segment.
 */
public class RestrictedSegment implements SegmentReference
{
  protected final SegmentReference delegate;
  @Nullable
  private final DimFilter filter;

  public RestrictedSegment(
      SegmentReference delegate,
      @Nullable DimFilter filter
  )
  {
    this.delegate = delegate;
    this.filter = filter;
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

  @Override
  public CursorFactory asCursorFactory()
  {
    return new RestrictedCursorFactory(delegate.asCursorFactory(), filter);
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    return null;
  }

  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    if (CursorFactory.class.equals(clazz)) {
      return (T) asCursorFactory();
    } else if (QueryableIndex.class.equals(clazz)) {
      return null;
    } else if (TimeBoundaryInspector.class.equals(clazz)) {
      return (T) WrappedTimeBoundaryInspector.create(delegate.as(TimeBoundaryInspector.class));
    } else if (TopNOptimizationInspector.class.equals(clazz)) {
      return (T) new SimpleTopNOptimizationInspector(filter == null);
    }

    // Unless we know there's no restriction, it's dangerous to return the implementation of a particular interface.
    if (filter == null) {
      return delegate.as(clazz);
    }
    return null;
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
