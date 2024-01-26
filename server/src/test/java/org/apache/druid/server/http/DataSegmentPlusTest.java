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

package org.apache.druid.server.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class DataSegmentPlusTest
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
  public void testEquals()
  {
    EqualsVerifier.forClass(DataSegmentPlus.class)
        .withNonnullFields("dataSegment", "createdDate")
        .usingGetClass()
        .verify();
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");

    String createdDateStr = "2024-01-20T00:00:00.701Z";
    String usedStatusLastUpdatedDateStr = "2024-01-20T01:00:00.701Z";
    DateTime createdDate = DateTimes.of(createdDateStr);
    DateTime usedStatusLastUpdatedDate = DateTimes.of(usedStatusLastUpdatedDateStr);
    DataSegmentPlus segmentPlus = new DataSegmentPlus(
        new DataSegment(
            "something",
            interval,
            "1",
            loadSpec,
            Arrays.asList("dim1", "dim2"),
            Arrays.asList("met1", "met2"),
            new NumberedShardSpec(3, 0),
            new CompactionState(
                new HashedPartitionsSpec(100000, null, ImmutableList.of("dim1")),
                new DimensionsSpec(
                    DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "bar", "foo"))
                ),
                ImmutableList.of(ImmutableMap.of("type", "count", "name", "count")),
                ImmutableMap.of("filter", ImmutableMap.of("type", "selector", "dimension", "dim1", "value", "foo")),
                ImmutableMap.of(),
                ImmutableMap.of()
            ),
            TEST_VERSION,
            1
        ),
        createdDate,
        usedStatusLastUpdatedDate
    );

    final Map<String, Object> objectMap = MAPPER.readValue(
        MAPPER.writeValueAsString(segmentPlus),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    Assert.assertEquals(3, objectMap.size());
    final Map<String, Object> segmentObjectMap = MAPPER.readValue(
        MAPPER.writeValueAsString(segmentPlus.getDataSegment()),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    // verify dataSegment
    Assert.assertEquals(11, segmentObjectMap.size());
    Assert.assertEquals("something", segmentObjectMap.get("dataSource"));
    Assert.assertEquals(interval.toString(), segmentObjectMap.get("interval"));
    Assert.assertEquals("1", segmentObjectMap.get("version"));
    Assert.assertEquals(loadSpec, segmentObjectMap.get("loadSpec"));
    Assert.assertEquals("dim1,dim2", segmentObjectMap.get("dimensions"));
    Assert.assertEquals("met1,met2", segmentObjectMap.get("metrics"));
    Assert.assertEquals(ImmutableMap.of("type", "numbered", "partitionNum", 3, "partitions", 0), segmentObjectMap.get("shardSpec"));
    Assert.assertEquals(TEST_VERSION, segmentObjectMap.get("binaryVersion"));
    Assert.assertEquals(1, segmentObjectMap.get("size"));
    Assert.assertEquals(6, ((Map) segmentObjectMap.get("lastCompactionState")).size());

    // verify extra metadata
    Assert.assertEquals(createdDateStr, objectMap.get("createdDate"));
    Assert.assertEquals(usedStatusLastUpdatedDateStr, objectMap.get("usedStatusLastUpdatedDate"));

    DataSegmentPlus deserializedSegmentPlus = MAPPER.readValue(MAPPER.writeValueAsString(segmentPlus), DataSegmentPlus.class);

    // verify dataSegment
    Assert.assertEquals(segmentPlus.getDataSegment().getDataSource(), deserializedSegmentPlus.getDataSegment().getDataSource());
    Assert.assertEquals(segmentPlus.getDataSegment().getInterval(), deserializedSegmentPlus.getDataSegment().getInterval());
    Assert.assertEquals(segmentPlus.getDataSegment().getVersion(), deserializedSegmentPlus.getDataSegment().getVersion());
    Assert.assertEquals(segmentPlus.getDataSegment().getLoadSpec(), deserializedSegmentPlus.getDataSegment().getLoadSpec());
    Assert.assertEquals(segmentPlus.getDataSegment().getDimensions(), deserializedSegmentPlus.getDataSegment().getDimensions());
    Assert.assertEquals(segmentPlus.getDataSegment().getMetrics(), deserializedSegmentPlus.getDataSegment().getMetrics());
    Assert.assertEquals(segmentPlus.getDataSegment().getShardSpec(), deserializedSegmentPlus.getDataSegment().getShardSpec());
    Assert.assertEquals(segmentPlus.getDataSegment().getSize(), deserializedSegmentPlus.getDataSegment().getSize());
    Assert.assertEquals(segmentPlus.getDataSegment().getId(), deserializedSegmentPlus.getDataSegment().getId());
    Assert.assertEquals(segmentPlus.getDataSegment().getLastCompactionState(), deserializedSegmentPlus.getDataSegment().getLastCompactionState());

    // verify extra metadata
    Assert.assertEquals(segmentPlus.getCreatedDate(), deserializedSegmentPlus.getCreatedDate());
    Assert.assertEquals(segmentPlus.getUsedStatusLastUpdatedDate(), deserializedSegmentPlus.getUsedStatusLastUpdatedDate());
  }
}
