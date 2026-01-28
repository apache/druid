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

package org.apache.druid.query.policy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.CursorBuildSpec;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Represents a basic row filter policy restriction.
 */
public class RowFilterPolicy implements Policy
{
  private final DimFilter rowFilter;

  @JsonCreator
  RowFilterPolicy(@Nonnull @JsonProperty("rowFilter") DimFilter rowFilter)
  {
    this.rowFilter = Preconditions.checkNotNull(rowFilter, "rowFilter can't be null");
  }

  public static RowFilterPolicy from(@Nonnull DimFilter rowFilter)
  {
    return new RowFilterPolicy(rowFilter);
  }

  @JsonProperty
  public DimFilter getRowFilter()
  {
    return rowFilter;
  }

  @Override
  public CursorBuildSpec visit(CursorBuildSpec spec)
  {
    return CursorBuildSpec.builder(spec).andFilter(rowFilter.toFilter()).build();
  }

  @Override
  public String toString()
  {
    return "RowFilterPolicy{" + "rowFilter=" + rowFilter + '}';
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
    RowFilterPolicy that = (RowFilterPolicy) o;
    return Objects.equals(rowFilter, that.rowFilter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rowFilter);
  }

}
