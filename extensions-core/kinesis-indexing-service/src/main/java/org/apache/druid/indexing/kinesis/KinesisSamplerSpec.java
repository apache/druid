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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.name.Named;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorIOConfig;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorSpec;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorTuningConfig;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamSamplerSpec;

import javax.annotation.Nullable;

public class KinesisSamplerSpec extends SeekableStreamSamplerSpec
{
  private final AWSCredentialsConfig awsCredentialsConfig;

  @JsonCreator
  public KinesisSamplerSpec(
      @JsonProperty("spec") final KinesisSupervisorSpec ingestionSpec,
      @JsonProperty("samplerConfig") @Nullable final SamplerConfig samplerConfig,
      @JacksonInject InputSourceSampler inputSourceSampler,
      @JacksonInject @Named(KinesisIndexingServiceModule.AWS_SCOPE) AWSCredentialsConfig awsCredentialsConfig
  )
  {
    super(ingestionSpec, samplerConfig, inputSourceSampler);

    this.awsCredentialsConfig = awsCredentialsConfig;
  }

  @Override
  protected KinesisRecordSupplier createRecordSupplier()
  {
    KinesisSupervisorIOConfig ioConfig = (KinesisSupervisorIOConfig) KinesisSamplerSpec.this.ioConfig;
    KinesisSupervisorTuningConfig tuningConfig = ((KinesisSupervisorTuningConfig) KinesisSamplerSpec.this.tuningConfig);

    return new KinesisRecordSupplier(
        KinesisRecordSupplier.getAmazonKinesisClient(
            ioConfig.getEndpoint(),
            awsCredentialsConfig,
            ioConfig.getAwsAssumedRoleArn(),
            ioConfig.getAwsExternalId()
        ),
        ioConfig.getRecordsPerFetch(),
        ioConfig.getFetchDelayMillis(),
        1,
        ioConfig.isDeaggregate(),
        tuningConfig.getRecordBufferSize(),
        tuningConfig.getRecordBufferOfferTimeout(),
        tuningConfig.getRecordBufferFullWait(),
        tuningConfig.getFetchSequenceNumberTimeout(),
        tuningConfig.getMaxRecordsPerPoll(),
        ioConfig.isUseEarliestSequenceNumber()
    );
  }
}
