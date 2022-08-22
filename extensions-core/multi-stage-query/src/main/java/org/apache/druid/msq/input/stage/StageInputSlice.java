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

package org.apache.druid.msq.input.stage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.input.InputSlice;

import java.util.Objects;

/**
 * Input slice representing some partitions of a stage.
 *
 * Sliced from {@link StageInputSpec} by {@link StageInputSpecSlicer}.
 */
@JsonTypeName("stage")
public class StageInputSlice implements InputSlice
{
  private final int stage;
  private final ReadablePartitions partitions;

  @JsonCreator
  public StageInputSlice(
      @JsonProperty("stage") int stageNumber,
      @JsonProperty("partitions") ReadablePartitions partitions
  )
  {
    this.stage = stageNumber;
    this.partitions = Preconditions.checkNotNull(partitions, "partitions");
  }

  @JsonProperty("stage")
  public int getStageNumber()
  {
    return stage;
  }

  @JsonProperty("partitions")
  public ReadablePartitions getPartitions()
  {
    return partitions;
  }

  @Override
  public int fileCount()
  {
    return 0;
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
    StageInputSlice that = (StageInputSlice) o;
    return stage == that.stage && Objects.equals(partitions, that.partitions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stage, partitions);
  }

  @Override
  public String toString()
  {
    return "StageInputSpec{" +
           "stage=" + stage +
           ", partitions=" + partitions +
           '}';
  }
}
