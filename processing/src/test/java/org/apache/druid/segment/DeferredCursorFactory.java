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

import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test {@link CursorFactory} that produces its async cursor holder on demand: {@link #makeCursorHolderAsync} hands back
 * an unfinished {@link AsyncCursorHolder}, and {@link #complete()} fills it (by building the delegate's holder for the
 * last requested spec) so a test can drive the await transition deterministically. {@link #canceled} flips when the
 * returned holder is closed before it is completed. Used to exercise the {@code makeCursorHolderAsync} override of
 * wrapping cursor factories (e.g. unnest, join) against a base that loads asynchronously.
 */
public class DeferredCursorFactory implements CursorFactory
{
  private final CursorFactory delegate;
  public final AtomicBoolean canceled = new AtomicBoolean(false);
  private AsyncCursorHolder pending;
  private CursorBuildSpec pendingSpec;

  public DeferredCursorFactory(CursorFactory delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    return delegate.makeCursorHolder(spec);
  }

  @Override
  public AsyncCursorHolder makeCursorHolderAsync(CursorBuildSpec spec)
  {
    pendingSpec = spec;
    pending = new AsyncCursorHolder(() -> canceled.set(true));
    return pending;
  }

  /**
   * Completes the most recent {@link #makeCursorHolderAsync} holder by building the delegate's holder for the spec it
   * was called with.
   */
  public void complete()
  {
    pending.set(delegate.makeCursorHolder(pendingSpec));
  }

  @Override
  public RowSignature getRowSignature()
  {
    return delegate.getRowSignature();
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return delegate.getColumnCapabilities(column);
  }
}
