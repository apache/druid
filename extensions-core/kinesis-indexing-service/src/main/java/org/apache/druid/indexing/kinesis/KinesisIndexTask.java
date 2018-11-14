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
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;

import java.util.Map;

public class KinesisIndexTask extends SeekableStreamIndexTask<String, String>
{
  private final KinesisIOConfig ioConfig;
  private final KinesisTuningConfig tuningConfig;

  @JsonCreator
  public KinesisIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KinesisTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KinesisIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(
        id,
        taskResource,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        chatHandlerProvider,
        authorizerMapper,
        rowIngestionMetersFactory,
        "index_kinesis"
    );

    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig;
  }

  @Override
  protected RecordSupplier<String, String> getRecordSupplier()
  {
    int fetchThreads = tuningConfig.getFetchThreads() != null
                       ? tuningConfig.getFetchThreads()
                       : Math.max(1, ioConfig.getStartPartitions().getPartitionSequenceNumberMap().size());

    return new KinesisRecordSupplier(
        ioConfig.getEndpoint(),
        ioConfig.getAwsAccessKeyId(),
        ioConfig.getAwsSecretAccessKey(),
        ioConfig.getRecordsPerFetch(),
        ioConfig.getFetchDelayMillis(),
        fetchThreads,
        ioConfig.getAwsAssumedRoleArn(),
        ioConfig.getAwsExternalId(),
        ioConfig.isDeaggregate(),
        tuningConfig.getRecordBufferSize(),
        tuningConfig.getRecordBufferOfferTimeout(),
        tuningConfig.getRecordBufferFullWait(),
        tuningConfig.getFetchSequenceNumberTimeout(),
        tuningConfig.getMaxRecordsPerPoll()
    );
  }

  @Override
  protected SeekableStreamIndexTaskRunner<String, String> createTaskRunner()
  {
    return new KinesisIndexTaskRunner(
        this,
        parser,
        authorizerMapper,
        chatHandlerProvider,
        savedParseExceptions,
        rowIngestionMetersFactory
    );
  }

  @Override
  @JsonProperty("ioConfig")
  public KinesisIOConfig getIOConfig()
  {
    return (KinesisIOConfig) super.getIOConfig();
  }
}
