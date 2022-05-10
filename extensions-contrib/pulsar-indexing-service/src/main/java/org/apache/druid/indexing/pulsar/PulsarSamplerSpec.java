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
 *
 */

package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.indexing.overlord.sampler.SamplerConfig;
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamSamplerSpec;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.segment.indexing.TuningConfig;

import javax.annotation.Nullable;

public class PulsarSamplerSpec extends SeekableStreamSamplerSpec
{
  private static final String READER_NAME = "druid-pulsar-indexing-sampler";

  @JsonCreator
  public PulsarSamplerSpec(
      @JsonProperty("spec") final SeekableStreamSupervisorSpec ingestionSpec,
      @JsonProperty("samplerConfig") @Nullable final SamplerConfig samplerConfig,
      @JacksonInject InputSourceSampler inputSourceSampler)
  {
    super(ingestionSpec, samplerConfig, inputSourceSampler);
  }

  @Override
  protected RecordSupplier createRecordSupplier()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      PulsarSupervisorIOConfig pulsarIOConfig = (PulsarSupervisorIOConfig) PulsarSamplerSpec.this.ioConfig;

      return new PulsarRecordSupplier(pulsarIOConfig.getServiceUrl(),
          READER_NAME,
          TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY,
          pulsarIOConfig.getAuthPluginClassName(),
          pulsarIOConfig.getAuthParams(),
          pulsarIOConfig.getOperationTimeoutMs(),
          pulsarIOConfig.getStatsIntervalSeconds(),
          pulsarIOConfig.getNumIoThreads(),
          pulsarIOConfig.getNumListenerThreads(),
          pulsarIOConfig.getUseTcpNoDelay(),
          pulsarIOConfig.getUseTls(),
          pulsarIOConfig.getTlsTrustCertsFilePath(),
          pulsarIOConfig.getTlsAllowInsecureConnection(),
          pulsarIOConfig.getTlsHostnameVerificationEnable(),
          pulsarIOConfig.getConcurrentLookupRequest(),
          pulsarIOConfig.getMaxLookupRequest(),
          pulsarIOConfig.getMaxNumberOfRejectedRequestPerConnection(),
          pulsarIOConfig.getKeepAliveIntervalSeconds(),
          pulsarIOConfig.getConnectionTimeoutMs(),
          pulsarIOConfig.getRequestTimeoutMs(),
          pulsarIOConfig.getMaxBackoffIntervalNanos());
    } finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }

  }
}
