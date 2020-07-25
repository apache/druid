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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.HashBucketShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardSpecsTest extends IngestionTestBase
{
  private final TestUtils testUtils = new TestUtils();
  private final ObjectMapper jsonMapper = testUtils.getTestObjectMapper();

  public ShardSpecsTest()
  {
  }

  @Test
  public void testShardSpecSelectionWithNullPartitionDimension()
  {
    HashBucketShardSpec spec1 = new HashBucketShardSpec(0, 2, null, jsonMapper);
    HashBucketShardSpec spec2 = new HashBucketShardSpec(1, 2, null, jsonMapper);

    Map<Interval, List<BucketNumberedShardSpec<?>>> shardSpecMap = new HashMap<>();
    shardSpecMap.put(Intervals.of("2014-01-01T00:00:00.000Z/2014-01-02T00:00:00.000Z"), ImmutableList.of(spec1, spec2));

    ShardSpecs shardSpecs = new ShardSpecs(shardSpecMap, Granularities.HOUR);
    String visitorId = "visitorId";
    String clientType = "clientType";
    long timestamp1 = DateTimes.of("2014-01-01T00:00:00.000Z").getMillis();
    InputRow row1 = new MapBasedInputRow(timestamp1,
        Lists.newArrayList(visitorId, clientType),
        ImmutableMap.of(visitorId, "0", clientType, "iphone")
    );

    long timestamp2 = DateTimes.of("2014-01-01T00:30:20.456Z").getMillis();
    InputRow row2 = new MapBasedInputRow(timestamp2,
        Lists.newArrayList(visitorId, clientType),
        ImmutableMap.of(visitorId, "0", clientType, "iphone")
    );

    long timestamp3 = DateTimes.of("2014-01-01T10:10:20.456Z").getMillis();
    InputRow row3 = new MapBasedInputRow(timestamp3,
        Lists.newArrayList(visitorId, clientType),
        ImmutableMap.of(visitorId, "0", clientType, "iphone")
    );

    ShardSpec spec3 = shardSpecs.getShardSpec(Intervals.of("2014-01-01T00:00:00.000Z/2014-01-02T00:00:00.000Z"), row1);
    ShardSpec spec4 = shardSpecs.getShardSpec(Intervals.of("2014-01-01T00:00:00.000Z/2014-01-02T00:00:00.000Z"), row2);
    ShardSpec spec5 = shardSpecs.getShardSpec(Intervals.of("2014-01-01T00:00:00.000Z/2014-01-02T00:00:00.000Z"), row3);

    Assert.assertSame(true, spec3 == spec4);
    Assert.assertSame(false, spec3 == spec5);
  }
}
