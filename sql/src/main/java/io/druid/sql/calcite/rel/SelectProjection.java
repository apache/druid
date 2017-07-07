/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.rel;

import io.druid.segment.VirtualColumn;
import org.apache.calcite.rel.core.Project;

import java.util.List;
import java.util.Objects;

public class SelectProjection
{
  private final Project calciteProject;
  private final List<String> directColumns;
  private final List<VirtualColumn> virtualColumns;

  public SelectProjection(
      final Project calciteProject,
      final List<String> directColumns,
      final List<VirtualColumn> virtualColumns
  )
  {
    this.calciteProject = calciteProject;
    this.directColumns = directColumns;
    this.virtualColumns = virtualColumns;
  }

  public Project getCalciteProject()
  {
    return calciteProject;
  }

  public List<String> getDirectColumns()
  {
    return directColumns;
  }

  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
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
    return Objects.equals(calciteProject, that.calciteProject) &&
           Objects.equals(directColumns, that.directColumns) &&
           Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(calciteProject, directColumns, virtualColumns);
  }

  @Override
  public String toString()
  {
    return "SelectProjection{" +
           "calciteProject=" + calciteProject +
           ", directColumns=" + directColumns +
           ", virtualColumns=" + virtualColumns +
           '}';
  }
}
