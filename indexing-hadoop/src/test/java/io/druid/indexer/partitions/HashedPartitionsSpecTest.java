/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer.partitions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class HashedPartitionsSpecTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testHashedPartitionsSpec() throws Exception
  {
    {
      final PartitionsSpec partitionsSpec;

      try {
        partitionsSpec = jsonReadWriteRead(
            "{"
            + "   \"targetPartitionSize\":100,"
            + "   \"type\":\"hashed\""
            + "}",
            PartitionsSpec.class
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      Assert.assertEquals(
          "isDeterminingPartitions",
          partitionsSpec.isDeterminingPartitions(),
          true
      );

      Assert.assertEquals(
          "getTargetPartitionSize",
          partitionsSpec.getTargetPartitionSize(),
          100
      );

      Assert.assertEquals(
          "getMaxPartitionSize",
          partitionsSpec.getMaxPartitionSize(),
          150
      );

      Assert.assertTrue("partitionsSpec", partitionsSpec instanceof HashedPartitionsSpec);
    }
  }

  @Test
  public void testHashedPartitionsSpecShardCount() throws Exception
  {
    final PartitionsSpec partitionsSpec;

    try {
      partitionsSpec = jsonReadWriteRead(
          "{"
          + "   \"type\":\"hashed\","
          + "   \"numShards\":2"
          + "}",
          PartitionsSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    Assert.assertEquals(
        "isDeterminingPartitions",
        partitionsSpec.isDeterminingPartitions(),
        false
    );

    Assert.assertEquals(
        "getTargetPartitionSize",
        partitionsSpec.getTargetPartitionSize(),
        -1
    );

    Assert.assertEquals(
        "getMaxPartitionSize",
        partitionsSpec.getMaxPartitionSize(),
        -1
    );

    Assert.assertEquals(
        "shardCount",
        partitionsSpec.getNumShards(),
        2
    );

    Assert.assertTrue("partitionsSpec", partitionsSpec instanceof HashedPartitionsSpec);
  }
  
  private <T> T jsonReadWriteRead(String s, Class<T> klass)
  {
    try {
      return jsonMapper.readValue(jsonMapper.writeValueAsBytes(jsonMapper.readValue(s, klass)), klass);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
