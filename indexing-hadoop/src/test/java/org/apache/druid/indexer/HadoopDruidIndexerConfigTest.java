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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.indexer.partitions.DimensionBasedPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HadoopDruidIndexerConfigTest
{
  private static final ObjectMapper JSON_MAPPER;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    JSON_MAPPER.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, JSON_MAPPER));
  }

  @Test
  public void testHashedBucketSelection()
  {
    List<HadoopyShardSpec> shardSpecs = new ArrayList<>();
    final int partitionCount = 10;
    for (int i = 0; i < partitionCount; i++) {
      shardSpecs.add(new HadoopyShardSpec(
          new HashBasedNumberedShardSpec(i, partitionCount, i, partitionCount, null, new DefaultObjectMapper()),
          i
      ));
    }

    HadoopIngestionSpec spec = new HadoopIngestionSpecBuilder()
        .shardSpecs(ImmutableMap.of(DateTimes.of("2010-01-01T01:00:00").getMillis(), shardSpecs))
        .build();
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromSpec(spec);
    final List<String> dims = Arrays.asList("diM1", "dIM2");
    final ImmutableMap<String, Object> values = ImmutableMap.of(
        "Dim1",
        "1",
        "DiM2",
        "2",
        "dim1",
        "3",
        "dim2",
        "4"
    );
    final long timestamp = DateTimes.of("2010-01-01T01:00:01").getMillis();
    final Bucket expectedBucket = config.getBucket(new MapBasedInputRow(timestamp, dims, values)).get();
    final long nextBucketTimestamp = Granularities.MINUTE.bucketEnd(DateTimes.utc(timestamp)).getMillis();
    // check that all rows having same set of dims and truncated timestamp hash to same bucket
    for (int i = 0; timestamp + i < nextBucketTimestamp; i++) {
      Assert.assertEquals(
          expectedBucket.partitionNum,
          config.getBucket(new MapBasedInputRow(timestamp + i, dims, values)).get().partitionNum
      );
    }

  }

  @Test
  public void testNoneShardSpecBucketSelection()
  {
    Map<Long, List<HadoopyShardSpec>> shardSpecs = ImmutableMap.of(
        DateTimes.of("2010-01-01T01:00:00").getMillis(),
        Collections.singletonList(new HadoopyShardSpec(
            NoneShardSpec.instance(),
            1
        )),
        DateTimes.of("2010-01-01T02:00:00").getMillis(),
        Collections.singletonList(new HadoopyShardSpec(
            NoneShardSpec.instance(),
            2
        ))
    );
    HadoopIngestionSpec spec = new HadoopIngestionSpecBuilder()
        .shardSpecs(shardSpecs)
        .build();
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromSpec(spec);
    final List<String> dims = Arrays.asList("diM1", "dIM2");
    final ImmutableMap<String, Object> values = ImmutableMap.of(
        "Dim1",
        "1",
        "DiM2",
        "2",
        "dim1",
        "3",
        "dim2",
        "4"
    );
    final long ts1 = DateTimes.of("2010-01-01T01:00:01").getMillis();
    Assert.assertEquals(1, config.getBucket(new MapBasedInputRow(ts1, dims, values)).get().getShardNum());

    final long ts2 = DateTimes.of("2010-01-01T02:00:01").getMillis();
    Assert.assertEquals(2, config.getBucket(new MapBasedInputRow(ts2, dims, values)).get().getShardNum());
  }

  @Test
  public void testGetTargetPartitionSizeWithHashedPartitions()
  {
    HadoopIngestionSpec spec = new HadoopIngestionSpecBuilder()
        .partitionsSpec(HashedPartitionsSpec.defaultSpec())
        .build();
    HadoopDruidIndexerConfig config = new HadoopDruidIndexerConfig(spec);
    int targetPartitionSize = config.getTargetPartitionSize();
    Assert.assertEquals(PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT, targetPartitionSize);
  }

  @Test
  public void testGetTargetPartitionSizeWithSingleDimensionPartitionsTargetRowsPerSegment()
  {
    int targetRowsPerSegment = 123;
    SingleDimensionPartitionsSpec partitionsSpec = new SingleDimensionPartitionsSpec(
        targetRowsPerSegment,
        null,
        null,
        false

    );
    HadoopIngestionSpec spec = new HadoopIngestionSpecBuilder()
        .partitionsSpec(partitionsSpec)
        .build();
    HadoopDruidIndexerConfig config = new HadoopDruidIndexerConfig(spec);
    int targetPartitionSize = config.getTargetPartitionSize();
    Assert.assertEquals(targetRowsPerSegment, targetPartitionSize);
  }

  @Test
  public void testGetTargetPartitionSizeWithSingleDimensionPartitionsMaxRowsPerSegment()
  {
    int maxRowsPerSegment = 456;
    SingleDimensionPartitionsSpec partitionsSpec = new SingleDimensionPartitionsSpec(
        null,
        maxRowsPerSegment,
        null,
        false
    );
    HadoopIngestionSpec spec = new HadoopIngestionSpecBuilder()
        .partitionsSpec(partitionsSpec)
        .build();
    HadoopDruidIndexerConfig config = new HadoopDruidIndexerConfig(spec);
    int targetPartitionSize = config.getTargetPartitionSize();
    Assert.assertEquals(maxRowsPerSegment, targetPartitionSize);
  }

  private static class HadoopIngestionSpecBuilder
  {
    private static final DataSchema DATA_SCHEMA = new DataSchema(
        "foo",
        null,
        new AggregatorFactory[0],
        new UniformGranularitySpec(
            Granularities.MINUTE,
            Granularities.MINUTE,
            ImmutableList.of(Intervals.of("2010-01-01/P1D"))
        ),
        null,
        HadoopDruidIndexerConfigTest.JSON_MAPPER
    );

    private static final HadoopIOConfig HADOOP_IO_CONFIG = new HadoopIOConfig(
        ImmutableMap.of("paths", "bar", "type", "static"),
        null,
        null
    );

    @Nullable
    private DimensionBasedPartitionsSpec partitionsSpec = null;

    private Map<Long, List<HadoopyShardSpec>> shardSpecs = Collections.emptyMap();

    HadoopIngestionSpecBuilder partitionsSpec(DimensionBasedPartitionsSpec partitionsSpec)
    {
      this.partitionsSpec = partitionsSpec;
      return this;
    }

    HadoopIngestionSpecBuilder shardSpecs(Map<Long, List<HadoopyShardSpec>> shardSpecs)
    {
      this.shardSpecs = shardSpecs;
      return this;
    }

    HadoopIngestionSpec build()
    {
      HadoopTuningConfig hadoopTuningConfig = new HadoopTuningConfig(
          null,
          null,
          partitionsSpec,
          shardSpecs,
          null,
          null,
          null,
          null,
          false,
          false,
          false,
          false,
          null,
          false,
          false,
          null,
          null,
          null,
          false,
          false,
          null,
          null,
          null,
          null
      );

      return new HadoopIngestionSpec(
          DATA_SCHEMA,
          HADOOP_IO_CONFIG,
          hadoopTuningConfig
      );
    }
  }
}
