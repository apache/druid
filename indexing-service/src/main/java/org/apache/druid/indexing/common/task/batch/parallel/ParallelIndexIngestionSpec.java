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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.FirehoseFactoryToInputSourceAdaptor;
import org.apache.druid.indexer.Checks;
import org.apache.druid.indexer.Property;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.IngestionSpec;

public class ParallelIndexIngestionSpec extends IngestionSpec<ParallelIndexIOConfig, ParallelIndexTuningConfig>
{
  private final DataSchema dataSchema;
  private final ParallelIndexIOConfig ioConfig;
  private final ParallelIndexTuningConfig tuningConfig;

  @JsonCreator
  public ParallelIndexIngestionSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") ParallelIndexIOConfig ioConfig,
      @JsonProperty("tuningConfig") ParallelIndexTuningConfig tuningConfig
  )
  {
    super(dataSchema, ioConfig, tuningConfig);

    if (dataSchema.getParserMap() != null && ioConfig.getInputSource() != null) {
      if (!(ioConfig.getInputSource() instanceof FirehoseFactoryToInputSourceAdaptor)) {
        throw new IAE("Cannot use parser and inputSource together. Try using inputFormat instead of parser.");
      }
    }
    if (ioConfig.getInputSource() != null && ioConfig.getInputSource().needsFormat()) {
      Checks.checkOneNotNullOrEmpty(
          ImmutableList.of(
              new Property<>("parser", dataSchema.getParserMap()),
              new Property<>("inputFormat", ioConfig.getInputFormat())
          )
      );
    }

    this.dataSchema = dataSchema;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig == null ? ParallelIndexTuningConfig.defaultConfig() : tuningConfig;
  }

  @Override
  @JsonProperty("dataSchema")
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @Override
  @JsonProperty("ioConfig")
  public ParallelIndexIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @Override
  @JsonProperty("tuningConfig")
  public ParallelIndexTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }
}
