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

package org.apache.druid.indexing.overlord.sampler;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.client.indexing.SamplerSpec;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.FirehoseFactoryToInputSourceAdaptor;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;

public class IndexTaskSamplerSpec implements SamplerSpec
{
  @Nullable
  private final DataSchema dataSchema;
  private final InputSource inputSource;
  /**
   * InputFormat can be null if {@link InputSource#needsFormat()} = false.
   */
  @Nullable
  private final InputFormat inputFormat;
  @Nullable
  private final SamplerConfig samplerConfig;
  private final InputSourceSampler inputSourceSampler;

  @JsonCreator
  public IndexTaskSamplerSpec(
      @JsonProperty("spec") final IndexTask.IndexIngestionSpec ingestionSpec,
      @JsonProperty("samplerConfig") @Nullable final SamplerConfig samplerConfig,
      @JacksonInject InputSourceSampler inputSourceSampler
  )
  {
    this.dataSchema = Preconditions.checkNotNull(ingestionSpec, "[spec] is required").getDataSchema();

    Preconditions.checkNotNull(ingestionSpec.getIOConfig(), "[spec.ioConfig] is required");

    if (ingestionSpec.getIOConfig().getInputSource() != null) {
      this.inputSource = ingestionSpec.getIOConfig().getInputSource();
      if (ingestionSpec.getIOConfig().getInputSource().needsFormat()) {
        this.inputFormat = Preconditions.checkNotNull(
            ingestionSpec.getIOConfig().getInputFormat(),
            "[spec.ioConfig.inputFormat] is required"
        );
      } else {
        this.inputFormat = null;
      }
    } else {
      final FirehoseFactory firehoseFactory = Preconditions.checkNotNull(
          ingestionSpec.getIOConfig().getFirehoseFactory(),
          "[spec.ioConfig.firehose] is required"
      );
      if (!(firehoseFactory instanceof FiniteFirehoseFactory)) {
        throw new IAE("firehose should be an instance of FiniteFirehoseFactory");
      }
      this.inputSource = new FirehoseFactoryToInputSourceAdaptor(
          (FiniteFirehoseFactory) firehoseFactory,
          ingestionSpec.getDataSchema().getParser()
      );
      this.inputFormat = null;
    }

    this.samplerConfig = samplerConfig;
    this.inputSourceSampler = inputSourceSampler;
  }

  @Override
  public SamplerResponse sample()
  {
    return inputSourceSampler.sample(inputSource, inputFormat, dataSchema, samplerConfig);
  }
}
