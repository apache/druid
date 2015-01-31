/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.IngestionSpec;

/**
 */
public class HadoopIngestionSpec extends IngestionSpec<HadoopIOConfig, HadoopTuningConfig>
{
  private final DataSchema dataSchema;
  private final HadoopIOConfig ioConfig;
  private final HadoopTuningConfig tuningConfig;

  @JsonCreator
  public HadoopIngestionSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") HadoopIOConfig ioConfig,
      @JsonProperty("tuningConfig") HadoopTuningConfig tuningConfig
  )
  {
    super(dataSchema, ioConfig, tuningConfig);

    this.dataSchema = dataSchema;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig == null ? HadoopTuningConfig.makeDefaultTuningConfig() : tuningConfig;
  }

  @JsonProperty("dataSchema")
  @Override
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty("ioConfig")
  @Override
  public HadoopIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @JsonProperty("tuningConfig")
  @Override
  public HadoopTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  public HadoopIngestionSpec withDataSchema(DataSchema schema)
  {
    return new HadoopIngestionSpec(
        schema,
        ioConfig,
        tuningConfig
    );
  }

  public HadoopIngestionSpec withIOConfig(HadoopIOConfig config)
  {
    return new HadoopIngestionSpec(
        dataSchema,
        config,
        tuningConfig
    );
  }

  public HadoopIngestionSpec withTuningConfig(HadoopTuningConfig config)
  {
    return new HadoopIngestionSpec(
        dataSchema,
        ioConfig,
        config
    );
  }
}
