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

package org.apache.druid.segment.loading;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertThrows;

public class TombstoneSegmentizerFactoryTest
{

  @Test
  public void testSegmentCreation()
  {
    Interval expectedInterval = Intervals.of("2021/2022");
    TombstoneSegmentizerFactory factory = new TombstoneSegmentizerFactory();
    DataSegment tombstone = DataSegment.builder()
        .dataSource("foo")
        .interval(expectedInterval)
        .version("1")
        .shardSpec(TombstoneShardSpec.INSTANCE)
        .loadSpec(ImmutableMap.of("type", DataSegment.TOMBSTONE_LOADSPEC_TYPE))
        .size(1)
        .build();

    Segment segment = factory.factorize(tombstone, null, true, null);
    Assert.assertNotNull(segment.asStorageAdapter());
    Assert.assertEquals("foo_2021-01-01T00:00:00.000Z_2022-01-01T00:00:00.000Z_1", segment.getId().toString());
    Assert.assertEquals(expectedInterval, segment.getDataInterval());

    QueryableIndex queryableIndex = segment.asQueryableIndex();
    Assert.assertNotNull(queryableIndex);
    assertThrows(UnsupportedOperationException.class, () -> queryableIndex.getNumRows());
    assertThrows(UnsupportedOperationException.class, () -> queryableIndex.getAvailableDimensions());
    assertThrows(UnsupportedOperationException.class, () -> queryableIndex.getBitmapFactoryForDimensions());
    assertThrows(UnsupportedOperationException.class, () -> queryableIndex.getMetadata());
    assertThrows(UnsupportedOperationException.class, () -> queryableIndex.getDimensionHandlers());
    assertThrows(UnsupportedOperationException.class, () -> queryableIndex.getColumnNames());
    assertThrows(UnsupportedOperationException.class, () -> queryableIndex.getColumnHolder(null));
    Assert.assertTrue(queryableIndex.isFromTombstone());

  }
}
