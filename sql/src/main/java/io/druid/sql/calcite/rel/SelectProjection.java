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

import com.google.common.collect.Sets;
import io.druid.java.util.common.ISE;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.Column;
import org.apache.calcite.rel.core.Project;

import java.util.List;
import java.util.Set;

public class SelectProjection
{
  private final Project project;
  private final List<DimensionSpec> dimensions;
  private final List<String> metrics;

  public SelectProjection(
      final Project project,
      final List<DimensionSpec> dimensions,
      final List<String> metrics
  )
  {
    this.project = project;
    this.dimensions = dimensions;
    this.metrics = metrics;

    // Verify no collisions. Start with TIME_COLUMN_NAME because QueryMaker.executeSelect hard-codes it.
    final Set<String> seen = Sets.newHashSet(Column.TIME_COLUMN_NAME);
    for (DimensionSpec dimensionSpec : dimensions) {
      if (!seen.add(dimensionSpec.getOutputName())) {
        throw new ISE("Duplicate field name: %s", dimensionSpec.getOutputName());
      }
    }
    for (String fieldName : metrics) {
      if (!seen.add(fieldName)) {
        throw new ISE("Duplicate field name: %s", fieldName);
      }
    }
  }

  public Project getProject()
  {
    return project;
  }

  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  public List<String> getMetrics()
  {
    return metrics;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SelectProjection that = (SelectProjection) o;

    if (project != null ? !project.equals(that.project) : that.project != null) {
      return false;
    }
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) {
      return false;
    }
    return metrics != null ? metrics.equals(that.metrics) : that.metrics == null;

  }

  @Override
  public int hashCode()
  {
    int result = project != null ? project.hashCode() : 0;
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "SelectProjection{" +
           "project=" + project +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
           '}';
  }
}
