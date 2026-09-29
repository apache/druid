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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.task.TuningConfigBuilder;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;

@ParameterizedClass(name = "partitionLocation = {0}")
@MethodSource("data")
public class PartialSegmentMergeIngestionSpecTest
{
  private static final ObjectMapper OBJECT_MAPPER = ParallelIndexTestingFactory.createObjectMapper();

  public static Iterable<?> data()
  {
    return Arrays.asList(
        GENERIC_PARTITION_LOCATION,
        DEEP_STORE_PARTITION_LOCATION
    );
  }

  @Parameter
  public PartitionLocation partitionLocation;

  private static final GenericPartitionLocation GENERIC_PARTITION_LOCATION = new GenericPartitionLocation(
      ParallelIndexTestingFactory.HOST,
      ParallelIndexTestingFactory.PORT,
      ParallelIndexTestingFactory.USE_HTTPS,
      ParallelIndexTestingFactory.SUBTASK_ID,
      ParallelIndexTestingFactory.INTERVAL,
      ParallelIndexTestingFactory.HASH_BASED_NUMBERED_SHARD_SPEC
  );

  private static final DeepStoragePartitionLocation DEEP_STORE_PARTITION_LOCATION = new DeepStoragePartitionLocation(
      ParallelIndexTestingFactory.SUBTASK_ID,
      ParallelIndexTestingFactory.INTERVAL,
      ParallelIndexTestingFactory.HASH_BASED_NUMBERED_SHARD_SPEC,
      ImmutableMap.of()
  );

  private PartialSegmentMergeIngestionSpec target;

  @BeforeEach
  public void setup()
  {
    PartialSegmentMergeIOConfig ioConfig =
        new PartialSegmentMergeIOConfig(Collections.singletonList(partitionLocation));
    HashedPartitionsSpec partitionsSpec =
        new HashedPartitionsSpec(null, 1, Collections.emptyList());
    target = new PartialSegmentMergeIngestionSpec(
        ParallelIndexTestingFactory.createDataSchema(ParallelIndexTestingFactory.INPUT_INTERVALS),
        ioConfig,
        TuningConfigBuilder.forParallelIndexTask()
                           .withForceGuaranteedRollup(true)
                           .withPartitionsSpec(partitionsSpec)
                           .build()
    );
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(OBJECT_MAPPER, target);
  }
}
