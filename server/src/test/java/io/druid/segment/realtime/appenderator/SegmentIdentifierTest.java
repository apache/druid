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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class SegmentIdentifierTest
{
  private static final String DATA_SOURCE = "foo";
  private static final Interval INTERVAL = new Interval("2000/PT1H");
  private static final String VERSION = "v1";
  private static final NumberedShardSpec SHARD_SPEC_0 = new NumberedShardSpec(0, 2);
  private static final NumberedShardSpec SHARD_SPEC_1 = new NumberedShardSpec(1, 2);
  private static final SegmentIdentifier ID_0 = new SegmentIdentifier(DATA_SOURCE, INTERVAL, VERSION, SHARD_SPEC_0);
  private static final SegmentIdentifier ID_1 = new SegmentIdentifier(DATA_SOURCE, INTERVAL, VERSION, SHARD_SPEC_1);

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.registerSubtypes(NumberedShardSpec.class);

    final SegmentIdentifier id2 = objectMapper.readValue(
        objectMapper.writeValueAsBytes(ID_1),
        SegmentIdentifier.class
    );

    Assert.assertEquals(ID_1, id2);
    Assert.assertEquals(DATA_SOURCE, id2.getDataSource());
    Assert.assertEquals(INTERVAL, id2.getInterval());
    Assert.assertEquals(VERSION, id2.getVersion());
    Assert.assertEquals(SHARD_SPEC_1.getPartitionNum(), id2.getShardSpec().getPartitionNum());
    Assert.assertEquals(SHARD_SPEC_1.getPartitions(), ((NumberedShardSpec) id2.getShardSpec()).getPartitions());
  }

  @Test
  public void testAsString()
  {
    Assert.assertEquals("foo_2000-01-01T00:00:00.000Z_2000-01-01T01:00:00.000Z_v1", ID_0.getIdentifierAsString());
    Assert.assertEquals("foo_2000-01-01T00:00:00.000Z_2000-01-01T01:00:00.000Z_v1_1", ID_1.getIdentifierAsString());
  }
}
