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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 */
public class HadoopDruidIndexerConfigTest
{
  private static final ObjectMapper jsonMapper;

  static {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, jsonMapper));
  }

  @Test
  public void testHashedBucketSelection()
  {
    List<HadoopyShardSpec> specs = new ArrayList<>();
    final int partitionCount = 10;
    for (int i = 0; i < partitionCount; i++) {
      specs.add(new HadoopyShardSpec(
          new HashBasedNumberedShardSpec(i, partitionCount, null, new DefaultObjectMapper()),
          i
      ));
    }

    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        new DataSchema(
            "foo",
            null,
            new AggregatorFactory[0],
            new UniformGranularitySpec(
                Granularities.MINUTE,
                Granularities.MINUTE,
                ImmutableList.of(Intervals.of("2010-01-01/P1D"))
            ),
            null,
            jsonMapper
        ),
        new HadoopIOConfig(ImmutableMap.of("paths", "bar", "type", "static"), null, null),
        new HadoopTuningConfig(
            null,
            null,
            null,
            ImmutableMap.of(DateTimes.of("2010-01-01T01:00:00").getMillis(), specs),
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
            true
        )
    );
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
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        new DataSchema(
            "foo",
            null,
            new AggregatorFactory[0],
            new UniformGranularitySpec(
                Granularities.MINUTE,
                Granularities.MINUTE,
                ImmutableList.of(Intervals.of("2010-01-01/P1D"))
            ),
            null,
            jsonMapper
        ),
        new HadoopIOConfig(ImmutableMap.of("paths", "bar", "type", "static"), null, null),
        new HadoopTuningConfig(
            null,
            null,
            null,
            ImmutableMap.of(DateTimes.of("2010-01-01T01:00:00").getMillis(),
                                                              Collections.singletonList(new HadoopyShardSpec(
                                                                  NoneShardSpec.instance(),
                                                                  1
                                                              )),
                                                              DateTimes.of("2010-01-01T02:00:00").getMillis(),
                                                              Collections.singletonList(new HadoopyShardSpec(
                                                                  NoneShardSpec.instance(),
                                                                  2
                                                              ))
            ),
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
            true
        )
    );
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
    Assert.assertEquals(config.getBucket(new MapBasedInputRow(ts1, dims, values)).get().getShardNum(), 1);

    final long ts2 = DateTimes.of("2010-01-01T02:00:01").getMillis();
    Assert.assertEquals(config.getBucket(new MapBasedInputRow(ts2, dims, values)).get().getShardNum(), 2);

  }
}
