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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class HashBasedNumberedPartialShardSpecTest
{
  private static final ObjectMapper MAPPER = ShardSpecTestUtils.initObjectMapper();

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(HashBasedNumberedPartialShardSpec.class)
                  .usingGetClass()
                  .withNonnullFields("partitionDimensions", "numBuckets")
                  .verify();
  }

  @Test
  public void testSerde() throws IOException
  {
    final HashBasedNumberedPartialShardSpec expected = new HashBasedNumberedPartialShardSpec(
        ImmutableList.of("dim1", "dim2"),
        1,
        3,
        HashPartitionFunction.MURMUR3_32_ABS
    );
    final byte[] json = MAPPER.writeValueAsBytes(expected);
    final HashBasedNumberedPartialShardSpec fromJson = (HashBasedNumberedPartialShardSpec) MAPPER.readValue(
        json,
        PartialShardSpec.class
    );
    Assertions.assertEquals(expected, fromJson);
  }

  @Test
  public void testJsonPropertyNames() throws IOException
  {
    final HashBasedNumberedPartialShardSpec expected = new HashBasedNumberedPartialShardSpec(
        ImmutableList.of("dim1", "dim2"),
        1,
        3,
        HashPartitionFunction.MURMUR3_32_ABS
    );
    final byte[] json = MAPPER.writeValueAsBytes(expected);
    //noinspection unchecked
    final Map<String, Object> map = MAPPER.readValue(json, Map.class);
    Assertions.assertEquals(5, map.size());
    Assertions.assertEquals(HashBasedNumberedPartialShardSpec.TYPE, map.get("type"));
    Assertions.assertEquals(expected.getPartitionDimensions(), map.get("partitionDimensions"));
    Assertions.assertEquals(expected.getBucketId(), map.get("bucketId"));
    Assertions.assertEquals(expected.getNumBuckets(), map.get("numPartitions"));
    Assertions.assertEquals(expected.getBucketId(), map.get("bucketId"));
    Assertions.assertEquals(expected.getPartitionFunction().toString(), map.get("partitionFunction"));
  }

  @Test
  public void testComplete()
  {
    final HashBasedNumberedPartialShardSpec partialShardSpec = new HashBasedNumberedPartialShardSpec(
        ImmutableList.of("dim"),
        2,
        4,
        null
    );
    final ShardSpec shardSpec = partialShardSpec.complete(MAPPER, 1, 3);
    Assertions.assertEquals(
        new HashBasedNumberedShardSpec(1, 3, 2, 4, ImmutableList.of("dim"), null, MAPPER),
        shardSpec
    );
  }
}
