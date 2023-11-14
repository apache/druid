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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.name.Named;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.utils.RuntimeInfo;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class KinesisIndexTask extends SeekableStreamIndexTask<String, String, ByteEntity>
{
  private static final String TYPE = "index_kinesis";
  private static final Logger log = new Logger(KinesisIndexTask.class);

  private final boolean useListShards;
  private final AWSCredentialsConfig awsCredentialsConfig;
  private RuntimeInfo runtimeInfo;

  @JsonCreator
  public KinesisIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KinesisIndexTaskTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KinesisIndexTaskIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("useListShards") boolean useListShards,
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
    this.useListShards = useListShards;
    this.awsCredentialsConfig = awsCredentialsConfig;
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox)
  {
    this.runtimeInfo = toolbox.getAdjustedRuntimeInfo();
    return super.runTask(toolbox);
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
  protected KinesisRecordSupplier newTaskRecordSupplier(final TaskToolbox toolbox)
      throws RuntimeException
  {
    KinesisIndexTaskIOConfig ioConfig = ((KinesisIndexTaskIOConfig) super.ioConfig);
    KinesisIndexTaskTuningConfig tuningConfig = ((KinesisIndexTaskTuningConfig) super.tuningConfig);
    final int recordBufferSizeBytes =
        tuningConfig.getRecordBufferSizeBytesOrDefault(runtimeInfo.getMaxHeapSizeBytes());
    final int fetchThreads = computeFetchThreads(runtimeInfo, recordBufferSizeBytes, tuningConfig.getFetchThreads());
    final int maxRecordsPerPoll = tuningConfig.getMaxRecordsPerPollOrDefault();

    log.info(
        "Starting record supplier with fetchThreads [%d], fetchDelayMillis [%d], "
        + "recordBufferSizeBytes [%d], maxRecordsPerPoll [%d]",
        fetchThreads,
        ioConfig.getFetchDelayMillis(),
        recordBufferSizeBytes,
        maxRecordsPerPoll
    );

    return new KinesisRecordSupplier(
        KinesisRecordSupplier.getAmazonKinesisClient(
            ioConfig.getEndpoint(),
            awsCredentialsConfig,
            ioConfig.getAwsAssumedRoleArn(),
            ioConfig.getAwsExternalId()
        ),
        ioConfig.getFetchDelayMillis(),
        fetchThreads,
        recordBufferSizeBytes,
        tuningConfig.getRecordBufferOfferTimeout(),
        tuningConfig.getRecordBufferFullWait(),
        maxRecordsPerPoll,
        false,
        useListShards
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

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    return Collections.singleton(new ResourceAction(
        new Resource(KinesisIndexingServiceModule.SCHEME, ResourceType.EXTERNAL),
        Action.READ
    ));
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

  @VisibleForTesting
  static int computeFetchThreads(
      final RuntimeInfo runtimeInfo,
      final long recordBufferSizeBytes,
      final Integer configuredFetchThreads
  )
  {
    int fetchThreads;
    if (configuredFetchThreads != null) {
      fetchThreads = configuredFetchThreads;
    } else {
      fetchThreads = runtimeInfo.getAvailableProcessors() * 2;
    }

    // assume that each fetchThread return 10MB (assummed size of aggregated record = 1MB, and
    // records per fetch is 10000 max), and cap fetchThreads at this amount. Don't fail if specified
    // to be greater than this as to not cause failure for older configurations, but log warning
    // if fetchThreads lowered because of this.
    int maxFetchThreads = Math.max(1, (int) (recordBufferSizeBytes / 10_000_000L));
    if (fetchThreads > maxFetchThreads) {
      log.warn("fetchThreads [%d] being lowered to [%d]", fetchThreads, maxFetchThreads);
      fetchThreads = maxFetchThreads;
    }

    Preconditions.checkArgument(
        fetchThreads > 0,
        "Must have at least one background fetch thread for the record supplier"
    );

    return fetchThreads;
  }
}
