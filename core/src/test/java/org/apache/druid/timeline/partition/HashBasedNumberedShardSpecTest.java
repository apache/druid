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
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HashBasedNumberedShardSpecTest
{
  private final ObjectMapper objectMapper = ShardSpecTestUtils.initObjectMapper();

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(HashBasedNumberedShardSpec.class)
                  .withIgnoredFields("jsonMapper")
                  .withPrefabValues(ObjectMapper.class, new ObjectMapper(), new ObjectMapper())
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerdeRoundTrip() throws Exception
  {
    final ShardSpec spec = objectMapper.readValue(
        objectMapper.writeValueAsBytes(
            new HashBasedNumberedShardSpec(
                1,
                2,
                1,
                3,
                ImmutableList.of("visitor_id"),
                objectMapper
            )
        ),
        ShardSpec.class
    );
    Assert.assertEquals(1, spec.getPartitionNum());
    Assert.assertEquals(2, spec.getNumCorePartitions());
    Assert.assertEquals(1, ((HashBasedNumberedShardSpec) spec).getBucketId());
    Assert.assertEquals(3, ((HashBasedNumberedShardSpec) spec).getNumBuckets());
    Assert.assertEquals(ImmutableList.of("visitor_id"), ((HashBasedNumberedShardSpec) spec).getPartitionDimensions());
  }

  @Test
  public void testSerdeBackwardsCompat() throws Exception
  {
    final ShardSpec spec = objectMapper.readValue(
        "{\"type\": \"hashed\", \"partitions\": 2, \"partitionNum\": 1}",
        ShardSpec.class
    );
    Assert.assertEquals(1, spec.getPartitionNum());
    Assert.assertEquals(2, spec.getNumCorePartitions());

    final ShardSpec specWithPartitionDimensions = objectMapper.readValue(
        "{\"type\": \"hashed\", \"partitions\": 2, \"partitionNum\": 1, \"partitionDimensions\":[\"visitor_id\"]}",
        ShardSpec.class
    );
    Assert.assertEquals(1, specWithPartitionDimensions.getPartitionNum());
    Assert.assertEquals(2, specWithPartitionDimensions.getNumCorePartitions());
    Assert.assertEquals(2, ((HashBasedNumberedShardSpec) specWithPartitionDimensions).getNumBuckets());
    Assert.assertEquals(
        ImmutableList.of("visitor_id"),
        ((HashBasedNumberedShardSpec) specWithPartitionDimensions).getPartitionDimensions()
    );
  }

  @Test
  public void testPartitionChunks()
  {
    final List<ShardSpec> specs = ImmutableList.of(
        new HashBasedNumberedShardSpec(0, 3, 0, 3, null, objectMapper),
        new HashBasedNumberedShardSpec(1, 3, 1, 3, null, objectMapper),
        new HashBasedNumberedShardSpec(2, 3, 2, 3, null, objectMapper)
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
  public void testIsInChunkWithMorePartitionsBeyondNumBucketsReturningTrue()
  {
    final int numBuckets = 3;
    final List<ShardSpec> specs = IntStream.range(0, 10)
                                           .mapToObj(i -> new HashOverridenShardSpec(i, numBuckets))
                                           .collect(Collectors.toList());

    for (int i = 0; i < 10; i++) {
      final InputRow row = new HashInputRow(numBuckets * 10000 + i);
      Assert.assertTrue(specs.get(i).isInChunk(row.getTimestampFromEpoch(), row));
    }
  }

  @Test
  public void testGetGroupKey()
  {
    final List<String> partitionDimensions1 = ImmutableList.of("visitor_id");
    final DateTime time = DateTimes.nowUtc();
    final InputRow inputRow = new MapBasedInputRow(
        time,
        ImmutableList.of("visitor_id", "cnt"),
        ImmutableMap.of("visitor_id", "v1", "cnt", 10)
    );
    Assert.assertEquals(
        ImmutableList.of(Collections.singletonList("v1")),
        HashBasedNumberedShardSpec.getGroupKey(partitionDimensions1, time.getMillis(), inputRow)
    );

    Assert.assertEquals(
        ImmutableList.of(
        time.getMillis(),
        ImmutableMap.of("cnt", Collections.singletonList(10), "visitor_id", Collections.singletonList("v1")))
                     .toString(),
        // empty list when partitionDimensions is null
        HashBasedNumberedShardSpec.getGroupKey(ImmutableList.of(), time.getMillis(), inputRow).toString()
    );
  }

  @Test
  public void testSharePartitionSpace()
  {
    final HashBasedNumberedShardSpec shardSpec = new HashBasedNumberedShardSpec(
        1,
        2,
        1,
        3,
        ImmutableList.of("visitor_id"),
        objectMapper
    );
    Assert.assertTrue(shardSpec.sharePartitionSpace(NumberedPartialShardSpec.instance()));
    Assert.assertTrue(shardSpec.sharePartitionSpace(new HashBasedNumberedPartialShardSpec(null, 0, 1)));
    Assert.assertTrue(shardSpec.sharePartitionSpace(new SingleDimensionPartialShardSpec("dim", 0, null, null, 1)));
    Assert.assertFalse(shardSpec.sharePartitionSpace(new NumberedOverwritePartialShardSpec(0, 2, 1)));
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

  public class HashOverridenShardSpec extends HashBasedNumberedShardSpec
  {
    public HashOverridenShardSpec(int partitionNum, int partitions)
    {
      super(partitionNum, partitions, partitionNum, partitions, null, objectMapper);
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
      return DateTimes.EPOCH;
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
    public Number getMetric(String metric)
    {
      return 0;
    }

    @Override
    public int compareTo(Row o)
    {
      return 0;
    }
  }

}
