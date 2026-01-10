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

package org.apache.druid.msq.input.table;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataSegmentWithLocationTest
{

  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final int TEST_VERSION = 0x9;

  @Before
  public void setUp()
  {
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT);
    MAPPER.setInjectableValues(injectableValues);
  }

  @Test
  public void testSerde_LoadableDataSegment() throws Exception
  {
    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ShardSpec shardSpec = new NumberedShardSpec(3, 0);
    final SegmentId segmentId = SegmentId.of("something", interval, "1", shardSpec);

    final Map<String, Object> loadSpec = Map.of("something", "or_other");
    final CompactionState compactionState = new CompactionState(
        new HashedPartitionsSpec(100000, null, List.of("dim1")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("dim1", "bar", "foo"))),
        List.of(new CountAggregatorFactory("count")),
        new CompactionTransformSpec(new SelectorDimFilter("dim1", "foo", null)),
        MAPPER.convertValue(Map.of(), IndexSpec.class),
        MAPPER.convertValue(Map.of(), GranularitySpec.class),
        null
    );
    final DataSegment segment = DataSegment.builder(segmentId)
                                           .loadSpec(loadSpec)
                                           .dimensions(Arrays.asList("dim1", "dim2"))
                                           .metrics(Arrays.asList("met1", "met2"))
                                           .projections(Arrays.asList("proj1", "proj2"))
                                           .shardSpec(shardSpec)
                                           .lastCompactionState(compactionState)
                                           .binaryVersion(TEST_VERSION)
                                           .size(123L)
                                           .totalRows(12)
                                           .build();
    final DruidServerMetadata serverMetadata = MAPPER.convertValue(
        Map.of("name", "server1"),
        DruidServerMetadata.class
    );
    final DataSegmentWithLocation segmentWithLocation = new DataSegmentWithLocation(segment, Set.of(serverMetadata));
    final DataSegmentWithLocation deserialized = MAPPER.readValue(
        MAPPER.writeValueAsString(segmentWithLocation),
        DataSegmentWithLocation.class
    );
    Assert.assertEquals(deserialized.toString(), segmentWithLocation.toString());
    Assert.assertEquals(Set.of(serverMetadata), segmentWithLocation.getServers());
  }
}
