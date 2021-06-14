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

package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.pulsar.PulsarRecordEntity;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;

import java.util.Map;

public class PulsarIndexTask extends SeekableStreamIndexTask<Integer, String, PulsarRecordEntity>
{
  private static final String TYPE = "index_pulsar";

  private final PulsarIndexTaskIOConfig ioConfig;

  @JsonCreator
  public PulsarIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") PulsarIndexTaskTuningConfig tuningConfig,
      @JsonProperty("ioConfig") PulsarIndexTaskIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ObjectMapper configMapper
  )
  {
    super(
        getOrMakeId(id, dataSchema.getDataSource(), TYPE),
        taskResource,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        getFormattedGroupId(dataSchema.getDataSource(), TYPE)
    );
    this.ioConfig = ioConfig;

    Preconditions.checkArgument(
        ioConfig.getStartSequenceNumbers().getExclusivePartitions().isEmpty(),
        "All startSequenceNumbers must be inclusive"
    );
  }

  @Override
  protected SeekableStreamIndexTaskRunner<Integer, String, PulsarRecordEntity> createTaskRunner()
  {
    //noinspection unchecked
    return new PulsarIndexTaskRunner(
        this,
        dataSchema.getParser(),
        authorizerMapper,
        lockGranularityToUse
    );
  }

  @Override
  protected PulsarRecordSupplierTask newTaskRecordSupplier()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      int maxRowsInMemory = TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY;

      if (tuningConfig != null) {
        maxRowsInMemory = tuningConfig.getMaxRowsInMemory();
      }

      return new PulsarRecordSupplierTask(
          // TODO(jpg): Determine if we want to give readers better names
          getId(),
          ioConfig.getServiceUrl(),
          ioConfig.getAuthPluginClassName(),
          ioConfig.getAuthParams(),
          ioConfig.getOperationTimeoutMs(),
          ioConfig.getStatsIntervalSeconds(),
          ioConfig.getNumIoThreads(),
          ioConfig.getNumListenerThreads(),
          ioConfig.isUseTcpNoDelay(),
          ioConfig.isUseTls(),
          ioConfig.getTlsTrustCertsFilePath(),
          ioConfig.isTlsAllowInsecureConnection(),
          ioConfig.isTlsHostnameVerificationEnable(),
          ioConfig.getConcurrentLookupRequest(),
          ioConfig.getMaxLookupRequest(),
          ioConfig.getMaxNumberOfRejectedRequestPerConnection(),
          ioConfig.getKeepAliveIntervalSeconds(),
          ioConfig.getConnectionTimeoutMs(),
          ioConfig.getRequestTimeoutMs(),
          ioConfig.getMaxBackoffIntervalNanos(),
          maxRowsInMemory
      );
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Override
  @JsonProperty
  public PulsarIndexTaskTuningConfig getTuningConfig()
  {
    return (PulsarIndexTaskTuningConfig) super.getTuningConfig();
  }

  @Override
  @JsonProperty("ioConfig")
  public PulsarIndexTaskIOConfig getIOConfig()
  {
    return (PulsarIndexTaskIOConfig) super.getIOConfig();
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean supportsQueries()
  {
    return true;
  }
}
