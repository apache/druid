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
import com.fasterxml.jackson.databind.jsontype.NamedType;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.task.TuningConfigBuilder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.indexing.TuningConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

public class ParallelIndexTuningConfigTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @BeforeEach
  public void setup()
  {
    mapper.registerSubtypes(new NamedType(ParallelIndexTuningConfig.class, "index_parallel"));
  }

  @Test
  public void testSerdeDefault() throws IOException
  {
    verifyConfigSerde(ParallelIndexTuningConfig.defaultConfig());
  }

  @Test
  public void testSerdeWithNullMaxRowsPerSegment() throws IOException
  {
    final ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
        .forParallelIndexTask()
        .withMaxRowsInMemory(10)
        .withMaxBytesInMemory(1000L)
        .withPartitionsSpec(new DynamicPartitionsSpec(100, 1000L))
        .withForceGuaranteedRollup(false)
        .build();
    verifyConfigSerde(tuningConfig);
  }

  @Test
  public void testSerdeWithMaxNumConcurrentSubTasks() throws IOException
  {
    final ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
        .forParallelIndexTask()
        .withMaxNumConcurrentSubTasks(250)
        .build();
    verifyConfigSerde(tuningConfig);
  }

  @Test
  public void testSerdeWithMaxNumSubTasks() throws IOException
  {
    final ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
        .forParallelIndexTask()
        .withMaxNumSubTasks(250)
        .build();
    verifyConfigSerde(tuningConfig);
  }

  @Test
  public void testConfigWithBothMaxNumSubTasksAndMaxNumConcurrentSubTasksIsInvalid()
  {
    final int maxNumSubTasks = 250;
    final IllegalArgumentException exception = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TuningConfigBuilder
            .forParallelIndexTask()
            .withMaxNumSubTasks(maxNumSubTasks)
            .withMaxNumConcurrentSubTasks(maxNumSubTasks)
            .build()
    );
    Assertions.assertTrue(exception.getMessage().contains("Can't use both maxNumSubTasks and maxNumConcurrentSubTasks"));
  }

  @Test
  public void testConstructorWithHashedPartitionsSpecAndNonForceGuaranteedRollupFailToCreate()
  {
    final IllegalArgumentException exception = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TuningConfigBuilder
            .forParallelIndexTask()
            .withPartitionsSpec(new HashedPartitionsSpec(null, 10, null))
            .withForceGuaranteedRollup(false)
            .build()
    );
    Assertions.assertTrue(exception.getMessage().contains("DynamicPartitionsSpec must be used for best-effort rollup"));
  }

  @Test
  public void testConstructorWithSingleDimensionPartitionsSpecAndNonForceGuaranteedRollupFailToCreate()
  {
    final IllegalArgumentException exception = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TuningConfigBuilder
            .forParallelIndexTask()
            .withPartitionsSpec(new DimensionRangePartitionsSpec(null, 100, Collections.singletonList("dim1"), false))
            .withForceGuaranteedRollup(false)
            .build()
    );
    Assertions.assertTrue(exception.getMessage().contains("DynamicPartitionsSpec must be used for best-effort rollup"));
  }

  @Test
  public void testConstructorWithDynamicPartitionsSpecAndForceGuaranteedRollupFailToCreate()
  {
    final IllegalArgumentException exception = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TuningConfigBuilder
            .forParallelIndexTask()
            .withPartitionsSpec(new DynamicPartitionsSpec(100, null))
            .withForceGuaranteedRollup(true)
            .build()
    );
    Assertions.assertTrue(exception.getMessage().contains("cannot be used for perfect rollup"));
  }

  private void verifyConfigSerde(ParallelIndexTuningConfig tuningConfig) throws IOException
  {
    final byte[] json = mapper.writeValueAsBytes(tuningConfig);
    final ParallelIndexTuningConfig fromJson =
        (ParallelIndexTuningConfig) mapper.readValue(json, TuningConfig.class);
    Assertions.assertEquals(fromJson, tuningConfig);
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ParallelIndexTuningConfig.class)
                  .usingGetClass()
                  .withPrefabValues(
                      IndexSpec.class,
                      IndexSpec.getDefault(),
                      IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build()
                  )
                  .verify();
  }
}
