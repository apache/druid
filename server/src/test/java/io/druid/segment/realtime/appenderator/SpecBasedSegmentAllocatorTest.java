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

package io.druid.segment.realtime.appenderator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.plumber.CustomVersioningPolicy;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class SpecBasedSegmentAllocatorTest
{

  @Test
  public void basicTest() throws Exception
  {
    DataSchema schema = new DataSchema(
        "dataSource",
        Collections.emptyMap(),
        new AggregatorFactory[] {},
        new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
        new DefaultObjectMapper()
    );

    ShardSpec shardSpec = new LinearShardSpec(1);
    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        0,
        null,
        null,
        null,
        new CustomVersioningPolicy("testing"),
        null,
        null,
        shardSpec,
        null,
        null,
        0,
        0,
        null,
        null,
        null
    );

    SegmentAllocator allocator = new SpecBasedSegmentAllocator(schema, tuningConfig);

    InputRow row = new MapBasedInputRow(
        DateTimes.of("2017-09-10T12:35:00.000Z"),
        ImmutableList.of("dim1"),
        ImmutableMap.<String, Object>of("dim1", "foo", "met1", "1")
    );

    SegmentIdentifier identifier = allocator.allocate(row, null, null);

    Assert.assertEquals("dataSource", identifier.getDataSource());
    Assert.assertEquals("testing", identifier.getVersion());
    Assert.assertEquals(shardSpec, identifier.getShardSpec());
    Assert.assertEquals(
        Intervals.of("2017-09-10T00:00:00.000Z/2017-09-11T00:00:00.000Z"),
        identifier.getInterval()
    );
  }

}
