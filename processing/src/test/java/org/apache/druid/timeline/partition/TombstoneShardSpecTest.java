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

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;


public class TombstoneShardSpecTest
{

  final TombstoneShardSpec tombstoneShardSpec = new TombstoneShardSpec();

  @Test
  public void getPartitionNum()
  {
    assertEquals(0, tombstoneShardSpec.getPartitionNum());
  }

  @Test
  public void getLookup()
  {
    ShardSpecLookup shardSpecLookup = tombstoneShardSpec.getLookup(Collections.singletonList(tombstoneShardSpec));
    Assert.assertEquals(tombstoneShardSpec, shardSpecLookup.getShardSpec(1, null));
  }

  @Test
  public void getDomainDimensions()
  {
    Assert.assertTrue(tombstoneShardSpec.getDomainDimensions().isEmpty());
  }

  @Test
  public void possibleInDomain()
  {
    Assert.assertTrue(tombstoneShardSpec.possibleInDomain(Collections.emptyMap()));
  }

  @Test
  public void getNumCorePartitions()
  {
    assertEquals(0, tombstoneShardSpec.getNumCorePartitions());
  }

  @Test
  public void getType()
  {
    Assert.assertEquals(ShardSpec.Type.TOMBSTONE, tombstoneShardSpec.getType());
  }

  @Test
  public void createChunk()
  {
    Assert.assertTrue(tombstoneShardSpec.createChunk(new Object()) != null);
  }

  // just to increase branch coverage
  @Test
  public void equalsTest()
  {
    TombstoneShardSpec tombstoneShardSpecOther = tombstoneShardSpec;
    Assert.assertTrue(tombstoneShardSpec.equals(tombstoneShardSpecOther));
    tombstoneShardSpecOther = null;
    Assert.assertFalse(tombstoneShardSpec.equals(tombstoneShardSpecOther));
    TombstoneShardSpec newTombostoneShardSepc = new TombstoneShardSpec();
    Assert.assertTrue(tombstoneShardSpec.equals(newTombostoneShardSepc));
  }
}
