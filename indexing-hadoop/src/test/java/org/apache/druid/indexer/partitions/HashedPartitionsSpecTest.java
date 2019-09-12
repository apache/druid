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

package org.apache.druid.indexer.partitions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class HashedPartitionsSpecTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testHashedPartitionsSpec()
  {
    {
      final PartitionsSpec partitionsSpec = jsonReadWriteRead(
          "{"
          + "   \"targetPartitionSize\":100,"
          + "   \"type\":\"hashed\""
          + "}",
          PartitionsSpec.class
      );
      Assert.assertTrue("partitionsSpec", partitionsSpec instanceof HashedPartitionsSpec);
      final HashedPartitionsSpec hadoopHashedPartitionsSpec = (HashedPartitionsSpec) partitionsSpec;

      Assert.assertEquals(
          "isDeterminingPartitions",
          hadoopHashedPartitionsSpec.needsDeterminePartitions(true),
          true
      );

      Assert.assertEquals(
          "getTargetPartitionSize",
          hadoopHashedPartitionsSpec.getMaxRowsPerSegment().intValue(),
          100
      );

      Assert.assertEquals(
          "getPartitionDimensions",
          hadoopHashedPartitionsSpec.getPartitionDimensions(),
          ImmutableList.of()
      );
    }
  }

  @Test
  public void testHashedPartitionsSpecShardCount()
  {
    final PartitionsSpec partitionsSpec = jsonReadWriteRead(
        "{"
        + "   \"type\":\"hashed\","
        + "   \"numShards\":2"
        + "}",
        PartitionsSpec.class
    );
    Assert.assertTrue("partitionsSpec", partitionsSpec instanceof HashedPartitionsSpec);
    final HashedPartitionsSpec hadoopHashedPartitionsSpec = (HashedPartitionsSpec) partitionsSpec;

    Assert.assertEquals(
        "isDeterminingPartitions",
        hadoopHashedPartitionsSpec.needsDeterminePartitions(true),
        false
    );

    Assert.assertNull(
        "getTargetPartitionSize",
        hadoopHashedPartitionsSpec.getMaxRowsPerSegment()
    );

    Assert.assertEquals(
        "shardCount",
        hadoopHashedPartitionsSpec.getNumShards().intValue(),
        2
    );

    Assert.assertEquals(
        "getPartitionDimensions",
        hadoopHashedPartitionsSpec.getPartitionDimensions(),
        ImmutableList.of()
    );

  }
  
  private <T> T jsonReadWriteRead(String s, Class<T> klass)
  {
    try {
      return JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsBytes(JSON_MAPPER.readValue(s, klass)), klass);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
