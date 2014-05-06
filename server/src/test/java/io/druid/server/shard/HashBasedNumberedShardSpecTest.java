/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.shard;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import io.druid.TestUtil;
import io.druid.data.input.InputRow;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.ShardSpec;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HashBasedNumberedShardSpecTest
{
  @Test
  public void testSerdeRoundTrip() throws Exception
  {

    final ShardSpec spec = TestUtil.MAPPER.readValue(
        TestUtil.MAPPER.writeValueAsBytes(new HashBasedNumberedShardSpec(1, 2)),
        ShardSpec.class
    );
    Assert.assertEquals(1, spec.getPartitionNum());
    Assert.assertEquals(2, ((HashBasedNumberedShardSpec) spec).getPartitions());
  }

  @Test
  public void testSerdeBackwardsCompat() throws Exception
  {
    final ShardSpec spec = TestUtil.MAPPER.readValue(
        "{\"type\": \"hashed\", \"partitions\": 2, \"partitionNum\": 1}",
        ShardSpec.class
    );
    Assert.assertEquals(1, spec.getPartitionNum());
    Assert.assertEquals(2, ((HashBasedNumberedShardSpec) spec).getPartitions());
  }

  @Test
  public void testPartitionChunks()
  {
    final List<ShardSpec> specs = ImmutableList.<ShardSpec>of(
        new HashBasedNumberedShardSpec(0, 3),
        new HashBasedNumberedShardSpec(1, 3),
        new HashBasedNumberedShardSpec(2, 3)
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

  @Test
  public void testIsInChunk()
  {

    List<ShardSpec> specs = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      specs.add(new HashOverridenShardSpec(i, 3));
    }


    assertExistsInOneSpec(specs, new HashInputRow(Integer.MIN_VALUE));
    assertExistsInOneSpec(specs, new HashInputRow(Integer.MAX_VALUE));
    assertExistsInOneSpec(specs, new HashInputRow(0));
    assertExistsInOneSpec(specs, new HashInputRow(1000));
    assertExistsInOneSpec(specs, new HashInputRow(-1000));

  }

  public boolean assertExistsInOneSpec(List<ShardSpec> specs, InputRow row)
  {
    for (ShardSpec spec : specs) {
      if (spec.isInChunk(row)) {
        return true;
      }
    }
    throw new ISE("None of the partition matches");
  }

  public static class HashOverridenShardSpec extends HashBasedNumberedShardSpec
  {
    public HashOverridenShardSpec(
        int partitionNum,
        int partitions
    )
    {
      super(partitionNum, partitions);
    }

    @Override
    protected int hash(InputRow inputRow)
    {
      return inputRow.hashCode();
    }
  }

  public static class HashInputRow implements InputRow
  {
    private final int hashcode;

    HashInputRow(int hashcode)
    {
      this.hashcode = hashcode;
    }

    @Override
    public int hashCode()
    {
      return hashcode;
    }

    @Override
    public List<String> getDimensions()
    {
      return null;
    }

    @Override
    public long getTimestampFromEpoch()
    {
      return 0;
    }

    @Override
    public List<String> getDimension(String s)
    {
      return null;
    }

    @Override
    public Object getRaw(String s)
    {
      return null;
    }

    @Override
    public float getFloatMetric(String s)
    {
      return 0;
    }
  }
}
