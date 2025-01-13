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

import com.google.common.collect.Iterables;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.Filters;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FilteredCursorFactory implements CursorFactory
{
  private final CursorFactory delegate;
  @Nullable
  private final DimFilter filter;
  private final VirtualColumns virtualColumns;

  public FilteredCursorFactory(
      CursorFactory delegate,
      @Nullable DimFilter filter,
      @Nullable VirtualColumns virtualColumns)
  {
    this.delegate = delegate;
    this.filter = filter;
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    final CursorBuildSpec.CursorBuildSpecBuilder buildSpecBuilder = CursorBuildSpec.builder(spec);

    // FIXME maybeAnd
    buildSpecBuilder.setFilter(Filters.conjunction(spec.getFilter(), getFilter()));
    buildSpecBuilder.setVirtualColumns(
        VirtualColumns.fromIterable(
            Iterables.concat(
                Arrays.asList(spec.getVirtualColumns().getVirtualColumns()),
                Arrays.asList(virtualColumns.getVirtualColumns())
            )
        )
    );
    buildSpecBuilder.setPhysicalColumns(getPhysicalColumns(spec));

    return delegate.makeCursorHolder(buildSpecBuilder.build());
  }

  private Set<String> getPhysicalColumns(CursorBuildSpec spec)
  {
    Set<String> physicalColumns = spec.getPhysicalColumns();
    if (physicalColumns != null) {
      physicalColumns = new HashSet<>(physicalColumns);
      if (filter != null) {
        for (String column : filter.getRequiredColumns()) {
          if (!spec.getVirtualColumns().exists(column)) {
            physicalColumns.add(column);
          }
        }
      }
      return physicalColumns;
    }
    return null;
  }

  private Filter getFilter()
  {
    if (filter == null) {
      return null;
    }
    return filter.toFilter();
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
