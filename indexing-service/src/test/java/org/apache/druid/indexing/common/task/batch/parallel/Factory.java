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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.InlineFirehoseFactory;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class Factory
{
  static final String AUTOMATIC_ID = null;
  static final String ID = "id";
  static final String GROUP_ID = "group-id";
  static final TaskResource TASK_RESOURCE = null;
  static final String SUPERVISOR_TASK_ID = "supervisor-task-id";
  static final int NUM_ATTEMPTS = 1;
  static final Map<String, Object> CONTEXT = Collections.emptyMap();
  static final IndexingServiceClient INDEXING_SERVICE_CLIENT = null;
  static final IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> TASK_CLIENT_FACTORY = null;
  static final AppenderatorsManager APPENDERATORS_MANAGER = null;
  static final HttpClient SHUFFLE_CLIENT = null;
  static final List<Interval> INPUT_INTERVALS = Collections.singletonList(Intervals.ETERNITY);
  static final String TASK_EXECUTOR_HOST = "task-executor-host";
  static final int TASK_EXECUTOR_PORT = 1;
  static final boolean USE_HTTPS = true;
  static final Interval INTERVAL = Intervals.ETERNITY;
  static final int NUM_ROWS = 2;
  static final long SIZE_BYTES = 3;
  static final int PARTITION_ID = 4;
  static final String HOST = "host";
  static final int PORT = 1;
  static final String SUBTASK_ID = "subtask-id";
  private static final ObjectMapper NESTED_OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final String SCHEMA_TIME = "time";
  private static final String SCHEMA_DIMENSION = "dim";
  private static final String DATASOURCE = "datasource";

  static final HashBasedNumberedShardSpec HASH_BASED_NUMBERED_SHARD_SPEC = new HashBasedNumberedShardSpec(
      PARTITION_ID,
      PARTITION_ID + 1,
      Collections.singletonList("dim"),
      Factory.NESTED_OBJECT_MAPPER
  );

  static ObjectMapper createObjectMapper()
  {
    InjectableValues injectableValues = new InjectableValues.Std()
        .addValue(IndexingServiceClient.class, INDEXING_SERVICE_CLIENT)
        .addValue(IndexTaskClientFactory.class, TASK_CLIENT_FACTORY)
        .addValue(AppenderatorsManager.class, APPENDERATORS_MANAGER)
        .addValue(ObjectMapper.class, NESTED_OBJECT_MAPPER)
        .addValue(HttpClient.class, SHUFFLE_CLIENT);

    ObjectMapper objectMapper = new JacksonModule().jsonMapper().setInjectableValues(injectableValues);

    List<? extends Module> firehoseModule = new FirehoseModule().getJacksonModules();
    firehoseModule.forEach(objectMapper::registerModule);

    return objectMapper;
  }

  @SuppressWarnings("SameParameterValue")
  static class TuningConfigBuilder
  {
    private PartitionsSpec partitionsSpec =
        new HashedPartitionsSpec(null, 2, null);
    private boolean forceGuaranteedRollup = true;
    private boolean logParseExceptions = false;
    private int maxParseExceptions = Integer.MAX_VALUE;

    TuningConfigBuilder partitionsSpec(PartitionsSpec partitionsSpec)
    {
      this.partitionsSpec = partitionsSpec;
      return this;
    }

    TuningConfigBuilder forceGuaranteedRollup(boolean forceGuaranteedRollup)
    {
      this.forceGuaranteedRollup = forceGuaranteedRollup;
      return this;
    }

    TuningConfigBuilder logParseExceptions(boolean logParseExceptions)
    {
      this.logParseExceptions = logParseExceptions;
      return this;
    }

    TuningConfigBuilder maxParseExceptions(int maxParseExceptions)
    {
      this.maxParseExceptions = maxParseExceptions;
      return this;
    }

    ParallelIndexTuningConfig build()
    {
      return new ParallelIndexTuningConfig(
          1,
          null,
          3,
          4L,
          5L,
          6,
          null,
          partitionsSpec,
          null,
          null,
          10,
          forceGuaranteedRollup,
          false,
          14L,
          null,
          null,
          16,
          17,
          18L,
          Duration.ZERO,
          20,
          21,
          22,
          logParseExceptions,
          maxParseExceptions,
          25
      );
    }
  }

  static DataSchema createDataSchema(List<Interval> granularitySpecInputIntervals)
  {
    GranularitySpec granularitySpec = new ArbitraryGranularitySpec(Granularities.DAY, granularitySpecInputIntervals);

    Map<String, Object> parser = NESTED_OBJECT_MAPPER.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec(SCHEMA_TIME, "auto", null),
                new DimensionsSpec(
                    DimensionsSpec.getDefaultSchemas(ImmutableList.of(SCHEMA_DIMENSION)),
                    null,
                    null
                ),
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    return new DataSchema(
        DATASOURCE,
        parser,
        null,
        granularitySpec,
        null,
        NESTED_OBJECT_MAPPER
    );
  }

  static ParallelIndexIngestionSpec createIngestionSpec(
      InlineFirehoseFactory inlineFirehoseFactory,
      ParallelIndexTuningConfig tuningConfig,
      DataSchema dataSchema
  )
  {
    ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(inlineFirehoseFactory, false);

    return new ParallelIndexIngestionSpec(dataSchema, ioConfig, tuningConfig);
  }

  static class SingleDimensionPartitionsSpecBuilder
  {
    @Nullable
    private String partitionDimension = SCHEMA_DIMENSION;
    private boolean assumeGrouped = false;

    SingleDimensionPartitionsSpecBuilder partitionDimension(@Nullable String partitionDimension)
    {
      this.partitionDimension = partitionDimension;
      return this;
    }

    SingleDimensionPartitionsSpecBuilder assumeGrouped(boolean assumeGrouped)
    {
      this.assumeGrouped = assumeGrouped;
      return this;
    }

    SingleDimensionPartitionsSpec build()
    {
      return new SingleDimensionPartitionsSpec(
          1,
          null,
          partitionDimension,
          assumeGrouped
      );
    }
  }

  static IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> createTaskClientFactory()
  {
    return (taskInfoProvider, callerId, numThreads, httpTimeout, numRetries) -> createTaskClient();
  }

  private static ParallelIndexSupervisorTaskClient createTaskClient()
  {
    ParallelIndexSupervisorTaskClient taskClient = EasyMock.niceMock(ParallelIndexSupervisorTaskClient.class);
    EasyMock.replay(taskClient);
    return taskClient;
  }

  static String createRow(long timestamp, Object dimensionValue)
  {
    try {
      return NESTED_OBJECT_MAPPER.writeValueAsString(ImmutableMap.of(
          SCHEMA_TIME, timestamp,
          SCHEMA_DIMENSION, dimensionValue
      ));
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
