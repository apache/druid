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
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.indexing.TuningConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class IndexTaskSerdeTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @BeforeClass
  public static void setup()
  {
    MAPPER.registerSubtypes(new NamedType(IndexTuningConfig.class, "index"));
  }

  @Test
  public void testSerdeTuningConfigWithDynamicPartitionsSpec() throws IOException
  {
    final IndexTuningConfig tuningConfig = TuningConfigBuilder
        .forIndexTask()
        .withPartitionsSpec(new DynamicPartitionsSpec(1000, 2000L))
        .withForceGuaranteedRollup(false)
        .build();
    assertSerdeTuningConfig(tuningConfig);
  }

  @Test
  public void testSerdeTuningConfigWithHashedPartitionsSpec() throws IOException
  {
    final IndexTuningConfig tuningConfig = TuningConfigBuilder
        .forIndexTask()
        .withPartitionsSpec(new HashedPartitionsSpec(null, 10, null))
        .withForceGuaranteedRollup(true)
        .build();
    assertSerdeTuningConfig(tuningConfig);
  }

  @Test
  public void testForceGuaranteedRollupWithDynamicPartitionsSpec()
  {
    Exception e = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> TuningConfigBuilder.forIndexTask()
                                 .withForceGuaranteedRollup(true)
                                 .withPartitionsSpec(new DynamicPartitionsSpec(1000, 2000L))
                                 .build()
    );
    Assert.assertEquals("DynamicPartitionsSpec cannot be used for perfect rollup", e.getMessage());
  }

  @Test
  public void testBestEffortRollupWithHashedPartitionsSpec()
  {
    Exception e = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> TuningConfigBuilder.forIndexTask()
                                 .withForceGuaranteedRollup(false)
                                 .withPartitionsSpec(new HashedPartitionsSpec(null, 10, null))
                                 .build()
    );
    Assert.assertEquals("DynamicPartitionsSpec must be used for best-effort rollup", e.getMessage());
  }

  private static void assertSerdeTuningConfig(IndexTuningConfig tuningConfig) throws IOException
  {
    final byte[] json = MAPPER.writeValueAsBytes(tuningConfig);
    final IndexTuningConfig fromJson = (IndexTuningConfig) MAPPER.readValue(json, TuningConfig.class);
    Assert.assertEquals(tuningConfig, fromJson);
  }
}
