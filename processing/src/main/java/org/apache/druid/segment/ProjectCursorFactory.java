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

import org.apache.druid.query.ProjectDataSource;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;

/**
 * {@link CursorFactory} for a {@link ProjectDataSource}: presents its delegate's columns along with
 * {@link #virtualColumns}, which it adds beneath the virtual columns of each spec, using
 * {@link CursorBuildSpec.CursorBuildSpecBuilder#addBaseVirtualColumns}.
 */
public class ProjectCursorFactory implements CursorFactory
{
  private final CursorFactory delegate;
  private final VirtualColumns virtualColumns;

  public ProjectCursorFactory(final CursorFactory delegate, final VirtualColumns virtualColumns)
  {
    this.delegate = delegate;
    this.virtualColumns = virtualColumns;
  }

  @Override
  public CursorHolder makeCursorHolder(final CursorBuildSpec spec)
  {
    return delegate.makeCursorHolder(delegateSpec(spec));
  }

  @Override
  public AsyncCursorHolder makeCursorHolderAsync(final CursorBuildSpec spec)
  {
    return delegate.makeCursorHolderAsync(delegateSpec(spec));
  }

  @Override
  public RowSignature getRowSignature()
  {
    return virtualColumns.wrappedSignature(delegate.getRowSignature(), delegate);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(final String column)
  {
    return virtualColumns.getColumnCapabilitiesWithFallback(delegate, column);
  }

  private CursorBuildSpec delegateSpec(final CursorBuildSpec spec)
  {
    return CursorBuildSpec.builder(spec).addBaseVirtualColumns(virtualColumns).build();
  }
}
