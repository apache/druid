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
import nl.jqno.equalsverifier.Warning;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SingleDimensionRangeBucketShardSpecTest
{
  @Test
  public void testConvert()
  {
    Assert.assertEquals(
        new BuildingSingleDimensionShardSpec(1, "dim", "start", "end", 5),
        new SingleDimensionRangeBucketShardSpec(1, "dim", "start", "end").convert(5)
    );
  }

  @Test
  public void testCreateChunk()
  {
    Assert.assertEquals(
        new NumberedPartitionChunk<>(1, 0, "test"),
        new SingleDimensionRangeBucketShardSpec(1, "dim", "start", "end").createChunk("test")
    );
  }

  @Test
  public void testShardSpecLookup()
  {
    final List<ShardSpec> shardSpecs = ImmutableList.of(
        new SingleDimensionRangeBucketShardSpec(0, "dim", null, "c"),
        new SingleDimensionRangeBucketShardSpec(1, "dim", "f", "i"),
        new SingleDimensionRangeBucketShardSpec(2, "dim", "i", null)
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
    mapper.registerSubtypes(new NamedType(SingleDimensionRangeBucketShardSpec.class, ShardSpec.Type.BUCKET_SINGLE_DIM));
    mapper.setInjectableValues(new Std().addValue(ObjectMapper.class, mapper));
    final SingleDimensionRangeBucketShardSpec original = new SingleDimensionRangeBucketShardSpec(1, "dim", "start", "end");
    final String json = mapper.writeValueAsString(original);
    ShardSpec shardSpec = mapper.readValue(json, ShardSpec.class);
    Assert.assertEquals(ShardSpec.Type.BUCKET_SINGLE_DIM, shardSpec.getType());

    final SingleDimensionRangeBucketShardSpec fromJson = (SingleDimensionRangeBucketShardSpec) shardSpec;
    Assert.assertEquals(original, fromJson);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SingleDimensionRangeBucketShardSpec.class)
                  .usingGetClass()
                  .suppress(Warning.ALL_FIELDS_SHOULD_BE_USED)
                  .verify();
  }
}
