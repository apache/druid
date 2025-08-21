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

package org.apache.druid.segment.projections;

import org.apache.druid.segment.CursorBuildSpec;

import java.util.Map;
import java.util.Objects;

/**
 * Transformed {@link CursorBuildSpec} to run against a projection and remapping of
 */
public final class ProjectionMatch
{
  private final CursorBuildSpec cursorBuildSpec;
  private final Map<String, String> remapColumns;

  public ProjectionMatch(CursorBuildSpec cursorBuildSpec, Map<String, String> remapColumns)
  {
    this.cursorBuildSpec = cursorBuildSpec;
    this.remapColumns = remapColumns;
  }

  public CursorBuildSpec getCursorBuildSpec()
  {
    return cursorBuildSpec;
  }

  public Map<String, String> getRemapColumns()
  {
    return remapColumns;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ProjectionMatch)) {
      return false;
    }
    ProjectionMatch that = (ProjectionMatch) o;
    return Objects.equals(cursorBuildSpec, that.cursorBuildSpec) && Objects.equals(remapColumns, that.remapColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(cursorBuildSpec, remapColumns);
  }

  @Override
  public String toString()
  {
    return "ProjectionMatch{" +
           "cursorBuildSpec=" + cursorBuildSpec +
           ", remapColumns=" + remapColumns +
           '}';
  }
}
