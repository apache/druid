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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.partition.HashBucketShardSpec;
import org.apache.druid.timeline.partition.HashPartitionFunction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class DeepStoragePartitionStatTest
{
  private static final ObjectMapper OBJECT_MAPPER = ParallelIndexTestingFactory.createObjectMapper();

  private DeepStoragePartitionStat target;

  @Before
  public void setup()
  {
    target = new DeepStoragePartitionStat(
        ParallelIndexTestingFactory.INTERVAL,
        new HashBucketShardSpec(
            ParallelIndexTestingFactory.PARTITION_ID,
            ParallelIndexTestingFactory.PARTITION_ID + 1,
            Collections.singletonList("dim"),
            HashPartitionFunction.MURMUR3_32_ABS,
            new ObjectMapper()
        ),
        ImmutableMap.of("path", "/dummy/index.zip")
    );
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(OBJECT_MAPPER, target);
  }

  @Test
  public void hasPartitionIdThatMatchesSecondaryPartition()
  {
    Assert.assertEquals(target.getSecondaryPartition().getBucketId(), target.getBucketId());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(DeepStoragePartitionStat.class)
                  .withNonnullFields("interval", "shardSpec", "loadSpec")
                  .usingGetClass()
                  .verify();
  }
}
