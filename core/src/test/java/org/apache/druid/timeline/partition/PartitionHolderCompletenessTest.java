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
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class PartitionHolderCompletenessTest
{
  @Parameterized.Parameters(name = "{1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{
            ImmutableList.of(
                new NumberedShardSpec(1, 3),
                new NumberedShardSpec(0, 3),
                new NumberedShardSpec(2, 3)
            ),
            NumberedShardSpec.class.getSimpleName()
        },
        new Object[]{
            // Simulate empty hash buckets
            ImmutableList.of(
                new HashBasedNumberedShardSpec(2, 3, 3, 5, null, null, new ObjectMapper()),
                new HashBasedNumberedShardSpec(0, 3, 0, 5, null, null, new ObjectMapper()),
                new HashBasedNumberedShardSpec(1, 3, 2, 5, null, null, new ObjectMapper())
            ),
            HashBasedNumberedShardSpec.class.getSimpleName()
        },
        new Object[]{
            // Simulate empty range buckets
            ImmutableList.of(
                new SingleDimensionShardSpec("dim", null, "aaa", 0, 3),
                new SingleDimensionShardSpec("dim", "ttt", "zzz", 2, 3),
                new SingleDimensionShardSpec("dim", "bbb", "fff", 1, 3)
            ),
            StringUtils.format(
                "%s with empty buckets",
                SingleDimensionShardSpec.class.getSimpleName()
            )
        },
        new Object[]{
            // Simulate old format segments with missing numCorePartitions
            ImmutableList.of(
                new SingleDimensionShardSpec("dim", "bbb", "fff", 1, null),
                new SingleDimensionShardSpec("dim", "fff", null, 2, null),
                new SingleDimensionShardSpec("dim", null, "bbb", 0, null)
            ),
            StringUtils.format(
                "%s with missing numCorePartitions",
                SingleDimensionShardSpec.class.getSimpleName()
            )
        },
        new Object[]{
            // Simulate empty range buckets with MultiDimensionShardSpec
            ImmutableList.of(
                new DimensionRangeShardSpec(
                    Collections.singletonList("dim"),
                    null,
                    StringTuple.create("aaa"),
                    0,
                    3
                ),
                new DimensionRangeShardSpec(
                    Collections.singletonList("dim"),
                    StringTuple.create("ttt"),
                    StringTuple.create("zzz"),
                    2,
                    3
                ),
                new DimensionRangeShardSpec(
                    Collections.singletonList("dim"),
                    StringTuple.create("bbb"),
                    StringTuple.create("fff"),
                    1,
                    3
                )
            ),
            StringUtils.format(
                "%s with empty buckets",
                DimensionRangeShardSpec.class.getSimpleName()
            )
        },
        new Object[]{
            // Simulate old format segments with missing numCorePartitions
            ImmutableList.of(
                new DimensionRangeShardSpec(
                    Collections.singletonList("dim"),
                    StringTuple.create("bbb"),
                    StringTuple.create("fff"),
                    1,
                    null
                ),
                new DimensionRangeShardSpec(
                    Collections.singletonList("dim"),
                    StringTuple.create("fff"),
                    null,
                    2,
                    null
                ),
                new DimensionRangeShardSpec(
                    Collections.singletonList("dim"),
                    null,
                    StringTuple.create("bbb"),
                    0,
                    null
                )
            ),
            StringUtils.format(
                "%s with missing numCorePartitions",
                DimensionRangeShardSpec.class.getSimpleName()
            )
        }
    );
  }

  private final List<ShardSpec> shardSpecs;

  public PartitionHolderCompletenessTest(List<ShardSpec> shardSpecs, String paramName)
  {
    this.shardSpecs = shardSpecs;
  }

  @Test
  public void testIsComplete()
  {
    final PartitionHolder<OvershadowableInteger> holder = new PartitionHolder<>(
        shardSpecs.get(0).createChunk(new OvershadowableInteger("version", shardSpecs.get(0).getPartitionNum(), 0))
    );
    for (int i = 0; i < shardSpecs.size() - 1; i++) {
      Assert.assertFalse(holder.isComplete());
      final ShardSpec shardSpec = shardSpecs.get(i + 1);
      holder.add(shardSpec.createChunk(new OvershadowableInteger("version", shardSpec.getPartitionNum(), 0)));
    }
    Assert.assertTrue(holder.isComplete());
    Assert.assertTrue(holder.hasData());
  }

  @Test
  public void testHasNoData()
  {
    final DataSegment tombstone = DataSegment.builder()
                                             .dataSource("foo")
                                             .version("1")
                                             .interval(Intervals.of("2021-01-01/P1D"))
                                             .shardSpec(new TombstoneShardSpec())
                                             .size(1)
                                             .build();
    final PartitionChunk<DataSegment> partitionChunk = new TombstonePartitionedChunk<>(tombstone);
    final PartitionHolder<DataSegment> partitionHolder = new PartitionHolder<DataSegment>(partitionChunk);
    Assert.assertFalse(partitionHolder.hasData());
  }
}
