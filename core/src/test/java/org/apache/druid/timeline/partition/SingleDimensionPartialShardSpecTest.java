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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SingleDimensionPartialShardSpecTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SingleDimensionPartialShardSpec.class)
                  .usingGetClass()
                  .withNonnullFields("partitionDimension", "bucketId", "numBuckets")
                  .verify();
  }

  @Test
  public void testSerde() throws IOException
  {
    final SingleDimensionPartialShardSpec expected = new SingleDimensionPartialShardSpec(
        "partitionKey",
        3,
        "start",
        "end",
        10
    );
    final ObjectMapper mapper = ShardSpecTestUtils.initObjectMapper();
    final byte[] json = mapper.writeValueAsBytes(expected);
    final SingleDimensionPartialShardSpec fromJson = (SingleDimensionPartialShardSpec) mapper.readValue(
        json,
        PartialShardSpec.class
    );
    Assert.assertEquals(expected, fromJson);
  }

  @Test
  public void testComplete()
  {
    final SingleDimensionPartialShardSpec partialShardSpec = new SingleDimensionPartialShardSpec(
        "dim",
        2,
        "end2",
        null,
        3
    );
    final ShardSpec shardSpec = partialShardSpec.complete(new ObjectMapper(), 1, 2);
    Assert.assertEquals(new SingleDimensionShardSpec("dim", "end2", null, 1, 2), shardSpec);
  }
}
