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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.IngestionSpec;

public class SinglePhaseParallelIndexIngestionSpec
    extends IngestionSpec<SinglePhaseParallelIndexIOConfig, SinglePhaseParallelIndexTuningConfig>
{
  private final DataSchema dataSchema;
  private final SinglePhaseParallelIndexIOConfig ioConfig;
  private final SinglePhaseParallelIndexTuningConfig tuningConfig;

  @JsonCreator
  public SinglePhaseParallelIndexIngestionSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") SinglePhaseParallelIndexIOConfig ioConfig,
      @JsonProperty("tuningConfig") SinglePhaseParallelIndexTuningConfig tuningConfig
  )
  {
    super(dataSchema, ioConfig, tuningConfig);

    this.dataSchema = dataSchema;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig == null ? SinglePhaseParallelIndexTuningConfig.defaultConfig() : tuningConfig;
  }

  @Override
  @JsonProperty("dataSchema")
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @Override
  @JsonProperty("ioConfig")
  public SinglePhaseParallelIndexIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @Override
  @JsonProperty("tuningConfig")
  public SinglePhaseParallelIndexTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }
}
