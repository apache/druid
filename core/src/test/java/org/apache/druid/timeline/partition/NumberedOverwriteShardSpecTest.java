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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

public class NumberedOverwriteShardSpecTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(NumberedOverwriteShardSpec.class).usingGetClass().verify();
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper mapper = ShardSpecTestUtils.initObjectMapper();
    mapper.registerSubtypes(new NamedType(NumberedOverwriteShardSpec.class, NumberedOverwriteShardSpec.TYPE));
    final NumberedOverwriteShardSpec original = new NumberedOverwriteShardSpec(
        PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + 2,
        0,
        10,
        (short) 1,
        (short) 3
    );
    final String json = mapper.writeValueAsString(original);
    final NumberedOverwriteShardSpec fromJson = (NumberedOverwriteShardSpec) mapper.readValue(json, ShardSpec.class);
    Assert.assertEquals(original, fromJson);
  }

  @Test
  public void testSharePartitionSpace()
  {
    final NumberedOverwriteShardSpec shardSpec = new NumberedOverwriteShardSpec(
        PartitionIds.NON_ROOT_GEN_START_PARTITION_ID,
        0,
        3,
        (short) 1,
        (short) 1
    );
    Assert.assertFalse(shardSpec.sharePartitionSpace(NumberedPartialShardSpec.instance()));
    Assert.assertFalse(shardSpec.sharePartitionSpace(new HashBasedNumberedPartialShardSpec(null, 0, 1)));
    Assert.assertFalse(shardSpec.sharePartitionSpace(new SingleDimensionPartialShardSpec("dim", 0, null, null, 1)));
    Assert.assertTrue(shardSpec.sharePartitionSpace(new NumberedOverwritePartialShardSpec(0, 2, 1)));
  }
}
