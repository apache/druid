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

package org.apache.druid.sql.calcite.rel;

import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.sql.calcite.table.RowSignature;

import java.util.List;
import java.util.Objects;

public class SelectProjection
{
  private final List<String> directColumns;
  private final List<VirtualColumn> virtualColumns;
  private final RowSignature outputRowSignature;

  public SelectProjection(
      final List<String> directColumns,
      final List<VirtualColumn> virtualColumns,
      final RowSignature outputRowSignature
  )
  {
    this.directColumns = directColumns;
    this.virtualColumns = virtualColumns;
    this.outputRowSignature = outputRowSignature;
  }

  public List<String> getDirectColumns()
  {
    return directColumns;
  }

  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SelectProjection that = (SelectProjection) o;
    return Objects.equals(directColumns, that.directColumns) &&
           Objects.equals(virtualColumns, that.virtualColumns) &&
           Objects.equals(outputRowSignature, that.outputRowSignature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(directColumns, virtualColumns, outputRowSignature);
  }

  @Override
  public String toString()
  {
    return "SelectProjection{" +
           "directColumns=" + directColumns +
           ", virtualColumns=" + virtualColumns +
           ", outputRowSignature=" + outputRowSignature +
           '}';
  }
}
