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

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.AndFilter;

import javax.annotation.Nullable;

public class FilteredCursorFactory implements CursorFactory
{
  private final CursorFactory delegate;
  @Nullable
  private final DimFilter filter;

  public FilteredCursorFactory(CursorFactory delegate, @Nullable DimFilter filter)
  {
    this.delegate = delegate;
    this.filter = filter;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    final CursorBuildSpec.CursorBuildSpecBuilder buildSpecBuilder = CursorBuildSpec.builder(spec);
    final Filter newFilter;
    if (spec.getFilter() == null) {
      if (filter != null) {
        newFilter = filter.toFilter();
      } else {
        newFilter = null;
      }
    } else {
      if (filter != null) {
        newFilter = new AndFilter(ImmutableList.of(spec.getFilter(), filter.toFilter()));
      } else {
        newFilter = spec.getFilter();
      }
    }
    buildSpecBuilder.setFilter(newFilter);
    return delegate.makeCursorHolder(buildSpecBuilder.build());
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
