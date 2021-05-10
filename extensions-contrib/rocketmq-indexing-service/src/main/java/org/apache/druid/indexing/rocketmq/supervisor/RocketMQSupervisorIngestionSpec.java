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

package org.apache.druid.indexing.rocketmq.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIngestionSpec;
import org.apache.druid.segment.indexing.DataSchema;

public class RocketMQSupervisorIngestionSpec extends SeekableStreamSupervisorIngestionSpec
{
  private final DataSchema dataSchema;
  private final RocketMQSupervisorIOConfig ioConfig;
  private final RocketMQSupervisorTuningConfig tuningConfig;

  @JsonCreator
  public RocketMQSupervisorIngestionSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") RocketMQSupervisorIOConfig ioConfig,
      @JsonProperty("tuningConfig") RocketMQSupervisorTuningConfig tuningConfig
  )
  {
    super(dataSchema, ioConfig, tuningConfig);
    this.dataSchema = dataSchema;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig == null ? RocketMQSupervisorTuningConfig.defaultConfig() : tuningConfig;
  }

  @Override
  @JsonProperty("dataSchema")
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @Override
  @JsonProperty("ioConfig")
  public RocketMQSupervisorIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @Override
  @JsonProperty("tuningConfig")
  public RocketMQSupervisorTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }
}
