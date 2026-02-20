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

package org.apache.druid.segment.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Spec containing transform configs for Compaction Task.
 * This class mimics JSON field names for fields supported in compaction task with
 * the corresponding fields in {@link TransformSpec}, but omits actual transforms since compaction may only apply
 * filtering transforms.
 * This is done for end-user ease of use. Basically, end-user will use the same syntax / JSON structure to set
 * transform configs for Compaction task as they would for any other ingestion task.
 */
public class CompactionTransformSpec
{
  @Nullable
  public static CompactionTransformSpec of(@Nullable TransformSpec transformSpec)
  {
    if (transformSpec == null) {
      return null;
    }
    if (TransformSpec.NONE.equals(transformSpec)) {
      return null;
    }

    return new CompactionTransformSpec(transformSpec.getFilter(), VirtualColumns.EMPTY);
  }

  @Nullable private final DimFilter filter;
  private final VirtualColumns virtualColumns;

  @JsonCreator
  public CompactionTransformSpec(
      @JsonProperty("filter") final DimFilter filter,
      @JsonProperty("virtualColumns") @Nullable final VirtualColumns virtualColumns
  )
  {
    this.filter = filter;
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
  }

  @JsonProperty
  @Nullable
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
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
    CompactionTransformSpec that = (CompactionTransformSpec) o;
    return Objects.equals(filter, that.filter)
        && Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(filter, virtualColumns);
  }

  @Override
  public String toString()
  {
    return "CompactionTransformSpec{" +
           "filter=" + filter +
           ", virtualColumns=" + virtualColumns +
           '}';
  }
}
