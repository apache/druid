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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.indexing.TuningConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class CompactionTuningConfigTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setup()
  {
    mapper.registerSubtypes(new NamedType(CompactionTask.CompactionTuningConfig.class, "compcation"));
  }

  @Test
  public void testSerdeDefault() throws IOException
  {
    final CompactionTask.CompactionTuningConfig tuningConfig =
        CompactionTask.CompactionTuningConfig.defaultConfig();
    final byte[] json = mapper.writeValueAsBytes(tuningConfig);
    final ParallelIndexTuningConfig fromJson =
        (CompactionTask.CompactionTuningConfig) mapper.readValue(json, TuningConfig.class);
    Assert.assertEquals(fromJson, tuningConfig);
  }

  @Test
  public void testConfigWithNonZeroAwaitSegmentAvailabilityTimeoutThrowsException()
  {
    final Exception e = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> TuningConfigBuilder.forCompactionTask()
                                 .withAwaitSegmentAvailabilityTimeoutMillis(5L)
                                 .build()
    );
    Assert.assertEquals(
        "awaitSegmentAvailabilityTimeoutMillis is not supported for Compcation Task",
        e.getMessage()
    );
  }

  @Test
  public void testConfigWithZeroAwaitSegmentAvailabilityTimeoutMillis()
  {
    final CompactionTask.CompactionTuningConfig tuningConfig = TuningConfigBuilder
        .forCompactionTask()
        .withAwaitSegmentAvailabilityTimeoutMillis(0L)
        .build();
    Assert.assertEquals(0L, tuningConfig.getAwaitSegmentAvailabilityTimeoutMillis());
  }

  @Test
  public void testDefaultAwaitSegmentAvailabilityTimeoutMillis()
  {
    final CompactionTask.CompactionTuningConfig tuningConfig =
        TuningConfigBuilder.forCompactionTask().build();
    Assert.assertEquals(0L, tuningConfig.getAwaitSegmentAvailabilityTimeoutMillis());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(CompactionTask.CompactionTuningConfig.class)
                  .withPrefabValues(
                      IndexSpec.class,
                      IndexSpec.DEFAULT,
                      IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build()
                  )
                  .usingGetClass()
                  .verify();
  }
}
