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
import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RangeBucketShardSpecTest
{
  @Test
  public void testConvert()
  {
    Assert.assertEquals(
        new BuildingSingleDimensionShardSpec(1, "dim", "start", "end", 5),
        new RangeBucketShardSpec(1, "dim", "start", "end").convert(5)
    );
  }

  @Test
  public void testCreateChunk()
  {
    Assert.assertEquals(
        new NumberedPartitionChunk<>(1, 0, "test"),
        new RangeBucketShardSpec(1, "dim", "start", "end").createChunk("test")
    );
  }

  @Test
  public void testShardSpecLookup()
  {
    final List<ShardSpec> shardSpecs = ImmutableList.of(
        new RangeBucketShardSpec(0, "dim", null, "c"),
        new RangeBucketShardSpec(1, "dim", "f", "i"),
        new RangeBucketShardSpec(2, "dim", "i", null)
    );
    final ShardSpecLookup lookup = shardSpecs.get(0).getLookup(shardSpecs);
    final long currentTime = DateTimes.nowUtc().getMillis();
    Assert.assertEquals(
        shardSpecs.get(0),
        lookup.getShardSpec(
            currentTime,
            new MapBasedInputRow(
                currentTime,
                ImmutableList.of("dim"), ImmutableMap.of("dim", "a", "time", currentTime)
            )
        )
    );
    Assert.assertEquals(
        shardSpecs.get(1),
        lookup.getShardSpec(
            currentTime,
            new MapBasedInputRow(
                currentTime,
                ImmutableList.of("dim"), ImmutableMap.of("dim", "g", "time", currentTime)
            )
        )
    );
    Assert.assertEquals(
        shardSpecs.get(2),
        lookup.getShardSpec(
            currentTime,
            new MapBasedInputRow(
                currentTime,
                ImmutableList.of("dim"), ImmutableMap.of("dim", "k", "time", currentTime)
            )
        )
    );
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper mapper = ShardSpecTestUtils.initObjectMapper();
    mapper.registerSubtypes(new NamedType(RangeBucketShardSpec.class, RangeBucketShardSpec.TYPE));
    mapper.setInjectableValues(new Std().addValue(ObjectMapper.class, mapper));
    final RangeBucketShardSpec original = new RangeBucketShardSpec(1, "dim", "start", "end");
    final String json = mapper.writeValueAsString(original);
    final RangeBucketShardSpec fromJson = (RangeBucketShardSpec) mapper.readValue(json, ShardSpec.class);
    Assert.assertEquals(original, fromJson);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(RangeBucketShardSpec.class).usingGetClass().verify();
  }
}
