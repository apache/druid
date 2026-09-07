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

package org.apache.druid.indexing.rabbitstream.supervisor;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpecUpdateAction;
import org.apache.druid.indexing.rabbitstream.RabbitStreamIndexTaskModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class RabbitStreamSupervisorTuningConfigTest
{
  private final ObjectMapper mapper;

  public RabbitStreamSupervisorTuningConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules((Iterable<Module>) new RabbitStreamIndexTaskModule().getJacksonModules());
  }

  @Test
  public void testRequireRestartWhenRabbitTaskTuningChanges()
  {
    final RabbitStreamSupervisorSpec oldSpec = supervisorSpec(tuningConfig(15, 16, 17));

    // getActionOnUpdateTo is invoked on the running (old) spec with the proposed spec as argument.
    Assertions.assertEquals(
        SupervisorSpecUpdateAction.RESTART_SUPERVISOR_AND_TASKS,
        oldSpec.getActionOnUpdateTo(supervisorSpec(tuningConfig(20, 16, 17)))
    );
    Assertions.assertEquals(
        SupervisorSpecUpdateAction.RESTART_SUPERVISOR_AND_TASKS,
        oldSpec.getActionOnUpdateTo(supervisorSpec(tuningConfig(15, 20, 17)))
    );
    Assertions.assertEquals(
        SupervisorSpecUpdateAction.RESTART_SUPERVISOR_AND_TASKS,
        oldSpec.getActionOnUpdateTo(supervisorSpec(tuningConfig(15, 16, 20)))
    );
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\"type\": \"rabbit\"}";

    RabbitStreamSupervisorTuningConfig config = (RabbitStreamSupervisorTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class)),
        TuningConfig.class);

    Assertions.assertNull(config.getBasePersistDirectory());
    Assertions.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assertions.assertEquals(150000, config.getMaxRowsInMemory());
    Assertions.assertEquals(5_000_000, config.getMaxRowsPerSegment().intValue());
    Assertions.assertEquals(new Period("PT10M"), config.getIntermediatePersistPeriod());
    Assertions.assertEquals(0, config.getMaxPendingPersists());
    // Assertions.assertEquals(IndexSpec.getDefault(), config.getIndexSpec());
    Assertions.assertEquals(false, config.isReportParseExceptions());
    Assertions.assertEquals(java.time.Duration.ofMinutes(15).toMillis(), config.getHandoffConditionTimeout());
    Assertions.assertNull(config.getWorkerThreads());
    Assertions.assertEquals(8L, (long) config.getChatRetries());
    Assertions.assertEquals(Duration.standardSeconds(10), config.getHttpTimeout());
    Assertions.assertEquals(Duration.standardSeconds(80), config.getShutdownTimeout());
    Assertions.assertEquals(Duration.standardSeconds(120), config.getRepartitionTransitionDuration());
    Assertions.assertEquals(100, config.getMaxRecordsPerPollOrDefault());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"rabbit\",\n"
        + "  \"basePersistDirectory\": \"/tmp/xxx\",\n"
        + "  \"maxRowsInMemory\": 100,\n"
        + "  \"maxRowsPerSegment\": 100,\n"
        + "  \"intermediatePersistPeriod\": \"PT1H\",\n"
        + "  \"maxPendingPersists\": 100,\n"
        + "  \"reportParseExceptions\": true,\n"
        + "  \"handoffConditionTimeout\": 100,\n"
        + "  \"workerThreads\": 12,\n"
        + "  \"chatRetries\": 14,\n"
        + "  \"httpTimeout\": \"PT15S\",\n"
        + "  \"shutdownTimeout\": \"PT95S\",\n"
        + "  \"repartitionTransitionDuration\": \"PT500S\",\n"
        + "  \"appendableIndexSpec\": { \"type\" : \"onheap\" },\n"
        + "  \"recordBufferSize\": 15,\n"
        + "  \"recordBufferOfferTimeout\": 16,\n"
        + "  \"maxRecordsPerPoll\": 17\n"
        + "}";

    RabbitStreamSupervisorTuningConfig config = (RabbitStreamSupervisorTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class)),
        TuningConfig.class);

    Assertions.assertNull(config.getBasePersistDirectory());
    Assertions.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assertions.assertEquals(100, config.getMaxRowsInMemory());
    Assertions.assertEquals(100, config.getMaxRowsPerSegment().intValue());
    Assertions.assertEquals(new Period("PT1H"), config.getIntermediatePersistPeriod());
    Assertions.assertEquals(100, config.getMaxPendingPersists());
    Assertions.assertEquals(true, config.isReportParseExceptions());
    Assertions.assertEquals(100, config.getHandoffConditionTimeout());
    Assertions.assertEquals(12, (int) config.getWorkerThreads());
    Assertions.assertEquals(14L, (long) config.getChatRetries());
    Assertions.assertEquals(15, (int) config.getRecordBufferSizeConfigured());
    Assertions.assertEquals(16, (int) config.getRecordBufferOfferTimeout());
    Assertions.assertEquals(17, (int) config.getMaxRecordsPerPollConfigured());
    Assertions.assertEquals(Duration.standardSeconds(15), config.getHttpTimeout());
    Assertions.assertEquals(Duration.standardSeconds(95), config.getShutdownTimeout());
    Assertions.assertEquals(Duration.standardSeconds(120), config.getRepartitionTransitionDuration());
  }

  private RabbitStreamSupervisorSpec supervisorSpec(final RabbitStreamSupervisorTuningConfig tuningConfig)
  {
    return new RabbitStreamSupervisorSpec.Builder()
        .id("id")
        .dataSchema(dataSchema())
        .ioConfig(ioConfig())
        .tuningConfig(tuningConfig)
        .build();
  }

  private DataSchema dataSchema()
  {
    return DataSchema.builder()
                     .withDataSource("testDS")
                     .withTimestamp(new TimestampSpec("timestamp", "iso", null))
                     .withDimensions(DimensionsSpec.EMPTY)
                     .withAggregators(new CountAggregatorFactory("rows"))
                     .withGranularity(
                         new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.NONE,
                             List.of()
                         )
                     )
                     .build();
  }

  private RabbitStreamSupervisorIOConfig ioConfig()
  {
    return new RabbitStreamIOConfigBuilder()
        .withStream("stream")
        .withUri("rabbit://localhost")
        .withTaskCount(1)
        .withTaskDuration(new Period("PT1H"))
        .build();
  }

  private RabbitStreamSupervisorTuningConfig tuningConfig(
      final Integer recordBufferSize,
      final Integer recordBufferOfferTimeout,
      final Integer maxRecordsPerPoll
  )
  {
    return mapper.convertValue(
        Map.of(
            "type", "rabbit",
            "recordBufferSize", recordBufferSize,
            "recordBufferOfferTimeout", recordBufferOfferTimeout,
            "maxRecordsPerPoll", maxRecordsPerPoll
        ),
        RabbitStreamSupervisorTuningConfig.class
    );
  }

}
