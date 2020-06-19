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
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GenericPartitionLocationTest
{
  private static final ObjectMapper OBJECT_MAPPER = ParallelIndexTestingFactory.createObjectMapper();

  private GenericPartitionLocation target;

  @Before
  public void setup()
  {
    target = new GenericPartitionLocation(
        ParallelIndexTestingFactory.HOST,
        ParallelIndexTestingFactory.PORT,
        ParallelIndexTestingFactory.USE_HTTPS,
        ParallelIndexTestingFactory.SUBTASK_ID,
        ParallelIndexTestingFactory.INTERVAL,
        ParallelIndexTestingFactory.HASH_BASED_NUMBERED_SHARD_SPEC
    );
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(OBJECT_MAPPER, target);
  }

  @Test
  public void hasPartitionIdThatMatchesShardSpec()
  {
    Assert.assertEquals(ParallelIndexTestingFactory.PARTITION_ID, target.getBucketId());
  }
}
