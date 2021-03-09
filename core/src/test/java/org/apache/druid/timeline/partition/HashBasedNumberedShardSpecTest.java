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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.com.google.common.base.Function;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.com.google.common.collect.Lists;
import org.apache.druid.com.google.common.collect.Range;
import org.apache.druid.com.google.common.collect.RangeSet;
import org.apache.druid.com.google.common.collect.TreeRangeSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
                HashPartitionFunction.MURMUR3_32_ABS,
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
    Assert.assertEquals(
        HashPartitionFunction.MURMUR3_32_ABS,
        ((HashBasedNumberedShardSpec) spec).getPartitionFunction()
    );
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
    Assert.assertNull(((HashBasedNumberedShardSpec) specWithPartitionDimensions).getPartitionFunction());
  }

  @Test
  public void testPartitionChunks()
  {
    final List<ShardSpec> specs = ImmutableList.of(
        new HashBasedNumberedShardSpec(0, 3, 0, 3, null, null, objectMapper),
        new HashBasedNumberedShardSpec(1, 3, 1, 3, null, null, objectMapper),
        new HashBasedNumberedShardSpec(2, 3, 2, 3, null, null, objectMapper)
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

  private HashPartitioner createHashPartitionerForHashInputRow(int numBuckets)
  {
    return new HashPartitioner(
        objectMapper,
        HashPartitionFunction.MURMUR3_32_ABS,
        ImmutableList.of(),
        numBuckets
    )
    {
      @Override
      int hash(final long timestamp, final InputRow inputRow)
      {
        return Math.abs(inputRow.hashCode() % numBuckets);
      }
    };
  }

  @Test
  public void testIsInChunk()
  {
    List<HashBasedNumberedShardSpec> specs = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      specs.add(newShardSpecForTesting(i, 3));
    }
    final HashPartitioner hashPartitioner = createHashPartitionerForHashInputRow(3);

    Assert.assertTrue(existsInOneSpec(specs, hashPartitioner, new HashInputRow(Integer.MIN_VALUE)));
    Assert.assertTrue(existsInOneSpec(specs, hashPartitioner, new HashInputRow(Integer.MAX_VALUE)));
    Assert.assertTrue(existsInOneSpec(specs, hashPartitioner, new HashInputRow(0)));
    Assert.assertTrue(existsInOneSpec(specs, hashPartitioner, new HashInputRow(1000)));
    Assert.assertTrue(existsInOneSpec(specs, hashPartitioner, new HashInputRow(-1000)));
  }

  @Test
  public void testIsInChunkWithMorePartitionsBeyondNumBucketsReturningTrue()
  {
    final int numBuckets = 3;
    final List<HashBasedNumberedShardSpec> specs = IntStream.range(0, 10)
                                                            .mapToObj(i -> newShardSpecForTesting(i, numBuckets))
                                                            .collect(Collectors.toList());
    final HashPartitioner hashPartitioner = createHashPartitionerForHashInputRow(numBuckets);

    for (int i = 0; i < 10; i++) {
      final InputRow row = new HashInputRow(numBuckets * 10000 + i);
      Assert.assertTrue(isInChunk(specs.get(i), hashPartitioner, row.getTimestampFromEpoch(), row));
    }
  }

  @Test
  public void testExtractKeys()
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
        new HashPartitioner(
            objectMapper,
            HashPartitionFunction.MURMUR3_32_ABS,
            partitionDimensions1,
            0 // not used
        ).extractKeys(time.getMillis(), inputRow)
    );

    Assert.assertEquals(
        ImmutableList.of(
            time.getMillis(),
            ImmutableMap.of("cnt", Collections.singletonList(10), "visitor_id", Collections.singletonList("v1"))
        ).toString(),
        // empty list when partitionDimensions is null
        new HashPartitioner(
            objectMapper,
            HashPartitionFunction.MURMUR3_32_ABS,
            ImmutableList.of(),
            0 // not used
        ).extractKeys(time.getMillis(), inputRow).toString()
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
        null,
        objectMapper
    );
    Assert.assertTrue(shardSpec.sharePartitionSpace(NumberedPartialShardSpec.instance()));
    Assert.assertTrue(shardSpec.sharePartitionSpace(new HashBasedNumberedPartialShardSpec(null, 0, 1, null)));
    Assert.assertTrue(shardSpec.sharePartitionSpace(new SingleDimensionPartialShardSpec("dim", 0, null, null, 1)));
    Assert.assertFalse(shardSpec.sharePartitionSpace(new NumberedOverwritePartialShardSpec(0, 2, 1)));
  }

  @Test
  public void testPossibleInDomainWithNullHashPartitionFunctionReturnAll()
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closed("123", "123"));
    final Map<String, RangeSet<String>> domain = ImmutableMap.of("visitor_id", rangeSet);

    final int numBuckets = 3;
    final List<HashBasedNumberedShardSpec> shardSpecs = new ArrayList<>();
    for (int i = 0; i < numBuckets; i++) {
      shardSpecs.add(
          new HashBasedNumberedShardSpec(
              i,
              numBuckets,
              i,
              numBuckets,
              ImmutableList.of("visitor_id"),
              null,
              objectMapper
          )
      );
    }
    Assert.assertEquals(numBuckets, shardSpecs.stream().filter(s -> s.possibleInDomain(domain)).count());
  }

  @Test
  public void testPossibleInDomainWithoutPartitionDimensionsReturnAll()
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closed("123", "123"));
    final Map<String, RangeSet<String>> domain = ImmutableMap.of("visitor_id", rangeSet);

    final int numBuckets = 3;
    final List<HashBasedNumberedShardSpec> shardSpecs = new ArrayList<>();
    for (int i = 0; i < numBuckets; i++) {
      shardSpecs.add(
          new HashBasedNumberedShardSpec(
              i,
              numBuckets,
              i,
              numBuckets,
              ImmutableList.of(),
              HashPartitionFunction.MURMUR3_32_ABS,
              objectMapper
          )
      );
    }
    Assert.assertEquals(numBuckets, shardSpecs.stream().filter(s -> s.possibleInDomain(domain)).count());
  }

  @Test
  public void testPossibleInDomainFilterOnPartitionDimensionsReturnPrunedShards()
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closed("123", "123"));
    final Map<String, RangeSet<String>> domain = ImmutableMap.of("visitor_id", rangeSet);

    final int numBuckets = 3;
    final List<HashBasedNumberedShardSpec> shardSpecs = new ArrayList<>();
    for (int i = 0; i < numBuckets; i++) {
      shardSpecs.add(
          new HashBasedNumberedShardSpec(
              i,
              numBuckets,
              i,
              numBuckets,
              ImmutableList.of("visitor_id"),
              HashPartitionFunction.MURMUR3_32_ABS,
              objectMapper
          )
      );
    }
    Assert.assertEquals(1, shardSpecs.stream().filter(s -> s.possibleInDomain(domain)).count());
  }

  @Test
  public void testPossibleInDomainFilterOnNonPartitionDimensionsReturnAll()
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closed("123", "123"));
    final Map<String, RangeSet<String>> domain1 = ImmutableMap.of("vistor_id_1", rangeSet);
    final int numBuckets = 3;
    final List<HashBasedNumberedShardSpec> shardSpecs = new ArrayList<>();
    for (int i = 0; i < numBuckets; i++) {
      shardSpecs.add(
          new HashBasedNumberedShardSpec(
              i,
              numBuckets,
              i,
              numBuckets,
              ImmutableList.of("visitor_id"),
              HashPartitionFunction.MURMUR3_32_ABS,
              objectMapper
          )
      );
    }
    Assert.assertEquals(shardSpecs.size(), shardSpecs.stream().filter(s -> s.possibleInDomain(domain1)).count());
  }

  public boolean existsInOneSpec(
      List<? extends HashBasedNumberedShardSpec> specs,
      HashPartitioner hashPartitioner,
      InputRow row
  )
  {
    for (HashBasedNumberedShardSpec spec : specs) {
      if (isInChunk(spec, hashPartitioner, row.getTimestampFromEpoch(), row)) {
        return true;
      }
    }
    return false;
  }

  private boolean isInChunk(
      HashBasedNumberedShardSpec shardSpec,
      HashPartitioner hashPartitioner,
      long timestamp,
      InputRow inputRow
  )
  {
    final int bucketId = hashPartitioner.hash(timestamp, inputRow);
    return bucketId == shardSpec.getBucketId();
  }

  private HashBasedNumberedShardSpec newShardSpecForTesting(int partitionNum, int partitions)
  {
    return new HashBasedNumberedShardSpec(
        partitionNum,
        partitions,
        partitionNum % partitions,
        partitions,
        null,
        null,
        objectMapper
    );
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
