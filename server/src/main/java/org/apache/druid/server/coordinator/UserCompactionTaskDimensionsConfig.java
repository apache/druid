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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.java.util.common.parsers.ParserUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Spec containing dimension configs for Compaction Task.
 * This class mimics JSON field names for fields supported in compaction task with
 * the corresponding fields in {@link org.apache.druid.data.input.impl.DimensionsSpec}.
 * This is done for end-user ease of use. Basically, end-user will use the same syntax / JSON structure to set
 * dimension configs for Compaction task as they would for any other ingestion task.
 */
public class UserCompactionTaskDimensionsConfig
{
  @Nullable private final List<DimensionSchema> dimensions;
  @Nullable private final Set<String> dimensionExclusions;

  @JsonCreator
  public UserCompactionTaskDimensionsConfig(
      @Nullable @JsonProperty("dimensions") List<DimensionSchema> dimensions,
      @Nullable @JsonProperty("dimensionExclusions") Set<String> dimensionExclusions
  )
  {
    this.dimensions = dimensions;
    this.dimensionExclusions = dimensionExclusions;

    if (this.dimensions != null && this.dimensionExclusions != null) {
      Preconditions.checkArgument(
          Sets.intersection(Sets.newHashSet(this.dimensions), dimensionExclusions).isEmpty(),
          "dimensions and dimensions exclusions cannot overlap"
      );
    }
    if (this.dimensions != null) {
      List<String> dimensionNames = this.dimensions.stream().map(DimensionSchema::getName).collect(Collectors.toList());
      ParserUtils.validateFields(dimensionNames);
    }
    if (this.dimensionExclusions != null) {
      ParserUtils.validateFields(this.dimensionExclusions);
    }
  }

  @Nullable
  public List<DimensionSchema> getDimensions()
  {
    return dimensions;
  }

  @Nullable
  public Set<String> getDimensionExclusions()
  {
    return dimensionExclusions;
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
    UserCompactionTaskDimensionsConfig that = (UserCompactionTaskDimensionsConfig) o;
    return Objects.equals(dimensions, that.dimensions) &&
           Objects.equals(dimensionExclusions, that.dimensionExclusions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimensions, dimensionExclusions);
  }

  @Override
  public String toString()
  {
    return "UserCompactionTaskDimensionsConfig{" +
           "dimensions=" + dimensions +
           ", dimensionExclusions=" + dimensionExclusions +
           '}';
  }
}
