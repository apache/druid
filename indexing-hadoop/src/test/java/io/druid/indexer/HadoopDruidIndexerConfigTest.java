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

package io.druid.indexer;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.MapBasedInputRow;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
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
    List<HadoopyShardSpec> specs = Lists.newArrayList();
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
                ImmutableList.of(new Interval("2010-01-01/P1D"))
            ),
            jsonMapper
        ),
        new HadoopIOConfig(ImmutableMap.<String, Object>of("paths", "bar", "type", "static"), null, null),
        new HadoopTuningConfig(
            null,
            null,
            null,
            ImmutableMap.of(new DateTime("2010-01-01T01:00:00").getMillis(), specs),
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
            null
        )
    );
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromSpec(spec);
    final List<String> dims = Arrays.asList("diM1", "dIM2");
    final ImmutableMap<String, Object> values = ImmutableMap.<String, Object>of(
        "Dim1",
        "1",
        "DiM2",
        "2",
        "dim1",
        "3",
        "dim2",
        "4"
    );
    final long timestamp = new DateTime("2010-01-01T01:00:01").getMillis();
    final Bucket expectedBucket = config.getBucket(new MapBasedInputRow(timestamp, dims, values)).get();
    final long nextBucketTimestamp = Granularities.MINUTE.bucketEnd(new DateTime(timestamp)).getMillis();
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
                ImmutableList.of(new Interval("2010-01-01/P1D"))
            ),
            jsonMapper
        ),
        new HadoopIOConfig(ImmutableMap.<String, Object>of("paths", "bar", "type", "static"), null, null),
        new HadoopTuningConfig(
            null,
            null,
            null,
            ImmutableMap.<Long, List<HadoopyShardSpec>>of(new DateTime("2010-01-01T01:00:00").getMillis(),
                                                              Lists.newArrayList(new HadoopyShardSpec(
                                                                  NoneShardSpec.instance(),
                                                                  1
                                                              )),
                                                              new DateTime("2010-01-01T02:00:00").getMillis(),
                                                              Lists.newArrayList(new HadoopyShardSpec(
                                                                  NoneShardSpec.instance(),
                                                                  2
                                                              ))
            ),
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
            null
        )
    );
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromSpec(spec);
    final List<String> dims = Arrays.asList("diM1", "dIM2");
    final ImmutableMap<String, Object> values = ImmutableMap.<String, Object>of(
        "Dim1",
        "1",
        "DiM2",
        "2",
        "dim1",
        "3",
        "dim2",
        "4"
    );
    final long ts1 = new DateTime("2010-01-01T01:00:01").getMillis();
    Assert.assertEquals(config.getBucket(new MapBasedInputRow(ts1, dims, values)).get().getShardNum(), 1);

    final long ts2 = new DateTime("2010-01-01T02:00:01").getMillis();
    Assert.assertEquals(config.getBucket(new MapBasedInputRow(ts2, dims, values)).get().getShardNum(), 2);

  }
}
