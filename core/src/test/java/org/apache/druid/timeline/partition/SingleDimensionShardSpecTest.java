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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
public class SingleDimensionShardSpecTest
{
  private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testPossibleInDomain()
  {
    Map<String, RangeSet<String>> domain1 = ImmutableMap.of("dim1", rangeSet(ImmutableList.of(Range.lessThan("abc"))));
    Map<String, RangeSet<String>> domain2 = ImmutableMap.of("dim1", rangeSet(ImmutableList.of(Range.singleton("e"))),
                                                            "dim2", rangeSet(ImmutableList.of(Range.singleton("na")))
    );
    ShardSpec shard1 = makeSpec("dim1", null, "abc");
    ShardSpec shard2 = makeSpec("dim1", "abc", "def");
    ShardSpec shard3 = makeSpec("dim1", "def", null);
    ShardSpec shard4 = makeSpec("dim2", null, "hello");
    ShardSpec shard5 = makeSpec("dim2", "hello", "jk");
    ShardSpec shard6 = makeSpec("dim2", "jk", "na");
    ShardSpec shard7 = makeSpec("dim2", "na", null);
    Assert.assertTrue(shard1.possibleInDomain(domain1));
    Assert.assertFalse(shard2.possibleInDomain(domain1));
    Assert.assertFalse(shard3.possibleInDomain(domain1));
    Assert.assertTrue(shard4.possibleInDomain(domain1));
    Assert.assertTrue(shard5.possibleInDomain(domain1));
    Assert.assertTrue(shard6.possibleInDomain(domain1));
    Assert.assertTrue(shard7.possibleInDomain(domain1));
    Assert.assertFalse(shard1.possibleInDomain(domain2));
    Assert.assertFalse(shard2.possibleInDomain(domain2));
    Assert.assertTrue(shard3.possibleInDomain(domain2));
    Assert.assertFalse(shard4.possibleInDomain(domain2));
    Assert.assertFalse(shard5.possibleInDomain(domain2));
    Assert.assertTrue(shard6.possibleInDomain(domain2));
    Assert.assertTrue(shard7.possibleInDomain(domain2));
  }

  @Test
  public void testSharePartitionSpace()
  {
    final SingleDimensionShardSpec shardSpec = makeSpec("start", "end");
    Assert.assertTrue(shardSpec.sharePartitionSpace(NumberedPartialShardSpec.instance()));
    Assert.assertTrue(shardSpec.sharePartitionSpace(new HashBasedNumberedPartialShardSpec(null, 0, 1, null)));
    Assert.assertTrue(shardSpec.sharePartitionSpace(new SingleDimensionPartialShardSpec("dim", 0, null, null, 1)));
    Assert.assertFalse(shardSpec.sharePartitionSpace(new NumberedOverwritePartialShardSpec(0, 2, 1)));
  }

  @Test
  public void testSerde() throws IOException
  {
    testSerde(new SingleDimensionShardSpec("dim", null, null, 10, null));
    testSerde(new SingleDimensionShardSpec("dim", "abc", null, 5, 10));
    testSerde(new SingleDimensionShardSpec("dim", null, "xyz", 10, 1));
    testSerde(new SingleDimensionShardSpec("dim", "abc", "xyz", 10, null));
  }

  @Test
  public void testDeserialize() throws JsonProcessingException
  {
    final String json = "{\"type\": \"single\","
                        + " \"dimension\": \"dim\","
                        + " \"start\": \"abc\","
                        + "\"end\": \"xyz\","
                        + "\"partitionNum\": 5,"
                        + "\"numCorePartitions\": 10}";
    ShardSpec shardSpec = OBJECT_MAPPER.readValue(json, ShardSpec.class);
    Assert.assertTrue(shardSpec instanceof SingleDimensionShardSpec);
    Assert.assertEquals(ShardSpec.Type.SINGLE, shardSpec.getType());

    SingleDimensionShardSpec singleDimShardSpec = (SingleDimensionShardSpec) shardSpec;
    Assert.assertEquals(
        new SingleDimensionShardSpec("dim", "abc", "xyz", 5, 10),
        singleDimShardSpec
    );
  }

  @Test
  public void testShardSpecLookup()
  {
    final List<ShardSpec> shardSpecs = ImmutableList.of(
        new SingleDimensionShardSpec("dim", null, "c", 1, 1),
        new SingleDimensionShardSpec("dim", "c", "h", 2, 1),
        new SingleDimensionShardSpec("dim", "h", null, 3, 1)
    );
    final ShardSpecLookup lookup = shardSpecs.get(0).getLookup(shardSpecs);
    final long currentTime = DateTimes.nowUtc().getMillis();
    Assert.assertEquals(
        shardSpecs.get(0),
        lookup.getShardSpec(
            currentTime,
            new MapBasedInputRow(
                currentTime,
                Collections.singletonList("dim"),
                ImmutableMap.of("dim", "a", "time", currentTime)
            )
        )
    );

    Assert.assertEquals(
        shardSpecs.get(0),
        lookup.getShardSpec(
            currentTime,
            new MapBasedInputRow(
                currentTime,
                Collections.singletonList("dim"),
                ImmutableMap.of("time", currentTime)
            )
        )
    );

    Assert.assertEquals(
        shardSpecs.get(0),
        lookup.getShardSpec(
            currentTime,
            new MapBasedInputRow(
                currentTime,
                Collections.singletonList("dim"),
                ImmutableMap.of("dim", Arrays.asList("a", "b"), "time", currentTime)
            )
        )
    );

    Assert.assertEquals(
        shardSpecs.get(1),
        lookup.getShardSpec(
            currentTime,
            new MapBasedInputRow(
                currentTime,
                Collections.singletonList("dim"),
                ImmutableMap.of("dim", "g", "time", currentTime)
            )
        )
    );

    Assert.assertEquals(
        shardSpecs.get(2),
        lookup.getShardSpec(
            currentTime,
            new MapBasedInputRow(
                currentTime,
                Collections.singletonList("dim"),
                ImmutableMap.of("dim", "k", "time", currentTime)
            )
        )
    );
  }

  private void testSerde(SingleDimensionShardSpec shardSpec) throws IOException
  {
    String json = OBJECT_MAPPER.writeValueAsString(shardSpec);
    SingleDimensionShardSpec deserializedSpec = OBJECT_MAPPER.readValue(json, SingleDimensionShardSpec.class);
    Assert.assertEquals(shardSpec, deserializedSpec);
  }

  private static RangeSet<String> rangeSet(List<Range<String>> ranges)
  {
    ImmutableRangeSet.Builder<String> builder = ImmutableRangeSet.builder();
    for (Range<String> range : ranges) {
      builder.add(range);
    }
    return builder.build();
  }

  private SingleDimensionShardSpec makeSpec(String start, String end)
  {
    return makeSpec("billy", start, end);
  }

  private SingleDimensionShardSpec makeSpec(String dimension, String start, String end)
  {
    return new SingleDimensionShardSpec(dimension, start, end, 0, SingleDimensionShardSpec.UNKNOWN_NUM_CORE_PARTITIONS);
  }
}
