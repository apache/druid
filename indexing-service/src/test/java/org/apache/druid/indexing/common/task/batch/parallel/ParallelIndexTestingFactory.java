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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.timeline.partition.BuildingHashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashPartitionFunction;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper for creating objects for testing parallel indexing.
 */
class ParallelIndexTestingFactory
{
  static final String AUTOMATIC_ID = null;
  static final String ID = "id";
  static final String GROUP_ID = "group-id";
  static final TaskResource TASK_RESOURCE = null;
  static final String SUPERVISOR_TASK_ID = "supervisor-task-id";
  static final String SUBTASK_SPEC_ID = "subtask-spec-id";
  static final int NUM_ATTEMPTS = 1;
  static final Map<String, Object> CONTEXT = Collections.emptyMap();
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
  private static final TestUtils TEST_UTILS = new TestUtils();
  private static final ObjectMapper NESTED_OBJECT_MAPPER = TEST_UTILS.getTestObjectMapper();
  private static final String SCHEMA_TIME = "time";
  private static final String SCHEMA_DIMENSION = "dim";
  private static final String DATASOURCE = "datasource";

  static final BuildingHashBasedNumberedShardSpec HASH_BASED_NUMBERED_SHARD_SPEC = new BuildingHashBasedNumberedShardSpec(
      PARTITION_ID,
      PARTITION_ID,
      PARTITION_ID + 1,
      Collections.singletonList("dim"),
      HashPartitionFunction.MURMUR3_32_ABS,
      ParallelIndexTestingFactory.NESTED_OBJECT_MAPPER
  );

  static ObjectMapper createObjectMapper()
  {
    return TEST_UTILS.getTestObjectMapper();
  }

  static DataSchema createDataSchema(List<Interval> granularitySpecInputIntervals)
  {
    GranularitySpec granularitySpec = new ArbitraryGranularitySpec(Granularities.DAY, granularitySpecInputIntervals);
    TimestampSpec timestampSpec = new TimestampSpec(SCHEMA_TIME, "auto", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        DimensionsSpec.getDefaultSchemas(ImmutableList.of(SCHEMA_DIMENSION))
    );

    return DataSchema.builder()
                     .withDataSource(DATASOURCE)
                     .withTimestamp(timestampSpec)
                     .withDimensions(dimensionsSpec)
                     .withGranularity(granularitySpec)
                     .withObjectMapper(NESTED_OBJECT_MAPPER)
                     .build();
  }

  static ParallelIndexIngestionSpec createIngestionSpec(
      InputSource inputSource,
      InputFormat inputFormat,
      ParallelIndexTuningConfig tuningConfig,
      DataSchema dataSchema
  )
  {
    ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(inputSource, inputFormat, false, false);

    return new ParallelIndexIngestionSpec(dataSchema, ioConfig, tuningConfig);
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

  static String createRowFromMap(long timestamp, Map<String, Object> fields)
  {
    HashMap<String, Object> row = new HashMap<>(fields);
    row.put(SCHEMA_TIME, timestamp);
    try {
      return NESTED_OBJECT_MAPPER.writeValueAsString(row);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
