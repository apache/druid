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
import org.junit.Assert;
import org.junit.Test;

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
        3
    );
    final byte[] json = MAPPER.writeValueAsBytes(expected);
    final HashBasedNumberedPartialShardSpec fromJson = (HashBasedNumberedPartialShardSpec) MAPPER.readValue(
        json,
        PartialShardSpec.class
    );
    Assert.assertEquals(expected, fromJson);
  }

  @Test
  public void testJsonPropertyNames() throws IOException
  {
    final HashBasedNumberedPartialShardSpec expected = new HashBasedNumberedPartialShardSpec(
        ImmutableList.of("dim1", "dim2"),
        1,
        3
    );
    final byte[] json = MAPPER.writeValueAsBytes(expected);
    //noinspection unchecked
    final Map<String, Object> map = MAPPER.readValue(json, Map.class);
    Assert.assertEquals(4, map.size());
    Assert.assertEquals(HashBasedNumberedPartialShardSpec.TYPE, map.get("type"));
    Assert.assertEquals(expected.getPartitionDimensions(), map.get("partitionDimensions"));
    Assert.assertEquals(expected.getBucketId(), map.get("bucketId"));
    Assert.assertEquals(expected.getNumBuckets(), map.get("numPartitions"));
    Assert.assertEquals(expected.getBucketId(), map.get("bucketId"));
  }

  @Test
  public void testComplete()
  {
    final HashBasedNumberedPartialShardSpec partialShardSpec = new HashBasedNumberedPartialShardSpec(
        ImmutableList.of("dim"),
        2,
        4
    );
    final ShardSpec shardSpec = partialShardSpec.complete(MAPPER, 1, 3);
    Assert.assertEquals(
        new HashBasedNumberedShardSpec(1, 3, 2, 4, ImmutableList.of("dim"), MAPPER),
        shardSpec
    );
  }
}
