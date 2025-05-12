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

import org.apache.druid.query.policy.Policy;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;

/**
 * A factory class for creating {@code Cursor} instances with strict adherence to {@link Policy} restrictions. Created
 * by {@link RestrictedSegment#as(Class)}, and applies policies transparently.
 * <p>
 * The {@code CursorFactory} simplifies the process of initializing and retrieving {@code Cursor} objects while ensuring
 * that any cursor created complies with the {@link Policy} restrictions.
 * <p>
 * Policy enforcement in {@link #makeCursorHolder}:
 * <ul>
 * <li>Row-level restrictions are enforced by adding filters to {@link CursorBuildSpec}, which is then passed to
 * delegate for execution. This ensures that only relevant data are accessible by the client.
 * </ul>
 *
 */
public class RestrictedCursorFactory implements CursorFactory
{
  private final CursorFactory delegate;
  private final Policy policy;

  public RestrictedCursorFactory(
      CursorFactory delegate,
      Policy policy
  )
  {
    this.delegate = delegate;
    this.policy = policy;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    return delegate.makeCursorHolder(policy.visit(spec));
  }

  @Override
  public RowSignature getRowSignature()
  {
    return delegate.getRowSignature();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return delegate.getColumnCapabilities(column);
  }
}
