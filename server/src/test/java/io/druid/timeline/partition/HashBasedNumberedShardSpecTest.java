/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.timeline.partition;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.druid.TestUtil;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.ISE;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HashBasedNumberedShardSpecTest
{
  @Test
  public void testSerdeRoundTrip() throws Exception
  {

    final ShardSpec spec = TestUtil.MAPPER.readValue(
        TestUtil.MAPPER.writeValueAsBytes(new HashBasedNumberedShardSpec(1, 2, ImmutableList.of("visitor_id"), TestUtil.MAPPER)),
        ShardSpec.class
    );
    Assert.assertEquals(1, spec.getPartitionNum());
    Assert.assertEquals(2, ((HashBasedNumberedShardSpec) spec).getPartitions());
    Assert.assertEquals(ImmutableList.of("visitor_id"), ((HashBasedNumberedShardSpec) spec).getPartitionDimensions());
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

    final ShardSpec specWithPartitionDimensions = TestUtil.MAPPER.readValue(
        "{\"type\": \"hashed\", \"partitions\": 2, \"partitionNum\": 1, \"partitionDimensions\":[\"visitor_id\"]}",
        ShardSpec.class
    );
    Assert.assertEquals(1, specWithPartitionDimensions.getPartitionNum());
    Assert.assertEquals(2, ((HashBasedNumberedShardSpec) specWithPartitionDimensions).getPartitions());
    Assert.assertEquals(ImmutableList.of("visitor_id"), ((HashBasedNumberedShardSpec) specWithPartitionDimensions).getPartitionDimensions());
  }

  @Test
  public void testPartitionChunks()
  {
    final List<ShardSpec> specs = ImmutableList.<ShardSpec>of(
        new HashBasedNumberedShardSpec(0, 3, null, TestUtil.MAPPER),
        new HashBasedNumberedShardSpec(1, 3, null, TestUtil.MAPPER),
        new HashBasedNumberedShardSpec(2, 3, null, TestUtil.MAPPER)
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

  @Test
  public void testGetGroupKey() throws Exception
  {
    final HashBasedNumberedShardSpec shardSpec1 = new HashBasedNumberedShardSpec(
        1,
        2,
        ImmutableList.of("visitor_id"),
        TestUtil.MAPPER
    );
    final DateTime time = new DateTime();
    final InputRow inputRow = new MapBasedInputRow(
        time,
        ImmutableList.of("visitor_id", "cnt"),
        ImmutableMap.<String, Object>of("visitor_id", "v1", "cnt", 10)
    );
    Assert.assertEquals(ImmutableList.of(Lists.newArrayList("v1")), shardSpec1.getGroupKey(time.getMillis(), inputRow));

    final HashBasedNumberedShardSpec shardSpec2 = new HashBasedNumberedShardSpec(1, 2, null, TestUtil.MAPPER);
    Assert.assertEquals(ImmutableList.of(
        time.getMillis(),
        ImmutableMap.of(
            "cnt",
            Lists.newArrayList(10),
            "visitor_id",
            Lists.newArrayList("v1")
        )
    ).toString(), shardSpec2.getGroupKey(time.getMillis(), inputRow).toString());
  }

  public boolean assertExistsInOneSpec(List<ShardSpec> specs, InputRow row)
  {
    for (ShardSpec spec : specs) {
      if (spec.isInChunk(row.getTimestampFromEpoch(), row)) {
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
      super(partitionNum, partitions, null, TestUtil.MAPPER);
    }

    @Override
    protected int hash(long timestamp, InputRow inputRow)
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
    public DateTime getTimestamp()
    {
      return new DateTime(0);
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

    @Override
    public long getLongMetric(String s)
    {
      return 0L;
    }

    @Override
    public int compareTo(Row o)
    {
      return 0;
    }
  }

}
