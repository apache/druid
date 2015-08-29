/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.shard;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.TestUtil;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.ShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class NumberedShardSpecTest
{
  @Test
  public void testSerdeRoundTrip() throws Exception
  {
    final ShardSpec spec = TestUtil.MAPPER.readValue(
        TestUtil.MAPPER.writeValueAsBytes(new NumberedShardSpec(1, 2)),
        ShardSpec.class
    );
    Assert.assertEquals(1, spec.getPartitionNum());
    Assert.assertEquals(2, ((NumberedShardSpec) spec).getPartitions());
  }

  @Test
  public void testSerdeBackwardsCompat() throws Exception
  {
    final ShardSpec spec = TestUtil.MAPPER.readValue(
        "{\"type\": \"numbered\", \"partitions\": 2, \"partitionNum\": 1}",
        ShardSpec.class
    );
    Assert.assertEquals(1, spec.getPartitionNum());
    Assert.assertEquals(2, ((NumberedShardSpec) spec).getPartitions());
  }

  @Test
  public void testPartitionChunks()
  {
    final List<ShardSpec> specs = ImmutableList.<ShardSpec>of(
        new NumberedShardSpec(0, 3),
        new NumberedShardSpec(1, 3),
        new NumberedShardSpec(2, 3)
    );

    final List<PartitionChunk<String>> chunks = Lists.transform(
        specs,
        new Function<ShardSpec, PartitionChunk<String>>()
        {
          @Override
          public PartitionChunk<String> apply(ShardSpec shardSpec)
          {
            return shardSpec.createChunk("rofl");
          }
        }
    );

    Assert.assertEquals(0, chunks.get(0).getChunkNumber());
    Assert.assertEquals(1, chunks.get(1).getChunkNumber());
    Assert.assertEquals(2, chunks.get(2).getChunkNumber());

    Assert.assertTrue(chunks.get(0).isStart());
    Assert.assertFalse(chunks.get(1).isStart());
    Assert.assertFalse(chunks.get(2).isStart());

    Assert.assertFalse(chunks.get(0).isEnd());
    Assert.assertFalse(chunks.get(1).isEnd());
    Assert.assertTrue(chunks.get(2).isEnd());

    Assert.assertTrue(chunks.get(0).abuts(chunks.get(1)));
    Assert.assertTrue(chunks.get(1).abuts(chunks.get(2)));

    Assert.assertFalse(chunks.get(0).abuts(chunks.get(0)));
    Assert.assertFalse(chunks.get(0).abuts(chunks.get(2)));
    Assert.assertFalse(chunks.get(1).abuts(chunks.get(0)));
    Assert.assertFalse(chunks.get(1).abuts(chunks.get(1)));
    Assert.assertFalse(chunks.get(2).abuts(chunks.get(0)));
    Assert.assertFalse(chunks.get(2).abuts(chunks.get(1)));
    Assert.assertFalse(chunks.get(2).abuts(chunks.get(2)));
  }
}
