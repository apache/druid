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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.name.Named;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.segment.indexing.DataSchema;

import java.util.Map;

public class KinesisIndexTask extends SeekableStreamIndexTask<String, String, ByteEntity>
{
  private static final String TYPE = "index_kinesis";

  private final AWSCredentialsConfig awsCredentialsConfig;

  @JsonCreator
  public KinesisIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KinesisIndexTaskTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KinesisIndexTaskIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject @Named(KinesisIndexingServiceModule.AWS_SCOPE) AWSCredentialsConfig awsCredentialsConfig
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
    this.awsCredentialsConfig = awsCredentialsConfig;
  }

  @Override
  protected SeekableStreamIndexTaskRunner<String, String, ByteEntity> createTaskRunner()
  {
    //noinspection unchecked
    return new KinesisIndexTaskRunner(
        this,
        dataSchema.getParser(),
        authorizerMapper,
        lockGranularityToUse
    );
  }

  @Override
  protected KinesisRecordSupplier newTaskRecordSupplier()
      throws RuntimeException
  {
    KinesisIndexTaskIOConfig ioConfig = ((KinesisIndexTaskIOConfig) super.ioConfig);
    KinesisIndexTaskTuningConfig tuningConfig = ((KinesisIndexTaskTuningConfig) super.tuningConfig);
    int fetchThreads = tuningConfig.getFetchThreads() != null
                       ? tuningConfig.getFetchThreads()
                       : Runtime.getRuntime().availableProcessors() * 2;

    Preconditions.checkArgument(
        fetchThreads > 0,
        "Must have at least one background fetch thread for the record supplier"
    );
    return new KinesisRecordSupplier(
        KinesisRecordSupplier.getAmazonKinesisClient(
            ioConfig.getEndpoint(),
            awsCredentialsConfig,
            ioConfig.getAwsAssumedRoleArn(),
            ioConfig.getAwsExternalId()
        ),
        ioConfig.getRecordsPerFetch(),
        ioConfig.getFetchDelayMillis(),
        fetchThreads,
        ioConfig.isDeaggregate(),
        tuningConfig.getRecordBufferSize(),
        tuningConfig.getRecordBufferOfferTimeout(),
        tuningConfig.getRecordBufferFullWait(),
        tuningConfig.getFetchSequenceNumberTimeout(),
        tuningConfig.getMaxRecordsPerPoll(),
        false
    );
  }

  @Override
  @JsonProperty("ioConfig")
  public KinesisIndexTaskIOConfig getIOConfig()
  {
    return (KinesisIndexTaskIOConfig) super.getIOConfig();
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

  @VisibleForTesting
  AWSCredentialsConfig getAwsCredentialsConfig()
  {
    return awsCredentialsConfig;
  }
}
