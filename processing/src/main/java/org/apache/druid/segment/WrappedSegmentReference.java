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

import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;

/**
 * This class is used as a wrapper for other classes that just want to
 * modify the storage adapter for a datasource. Examples include:
 * {@link org.apache.druid.query.UnnestDataSource}, {@link FilteredDataSource}
 */
public class WrappedSegmentReference implements SegmentReference
{
  private final SegmentReference delegate;
  private final Function<StorageAdapter, StorageAdapter> storageAdapterWrapperFunction;

  public WrappedSegmentReference(
      SegmentReference delegate,
      Function<StorageAdapter, StorageAdapter> storageAdapterWrapperFunction
  )
  {
    this.delegate = delegate;
    this.storageAdapterWrapperFunction = storageAdapterWrapperFunction;
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

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return storageAdapterWrapperFunction.apply(delegate.asStorageAdapter());
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }
}

