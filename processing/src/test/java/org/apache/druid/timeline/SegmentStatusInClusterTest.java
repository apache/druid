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

package org.apache.druid.timeline;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

public class SegmentStatusInClusterTest
{
  private static final ObjectMapper MAPPER = createObjectMapper();
  private static final Interval INTERVAL = Intervals.of("2011-10-01/2011-10-02");
  private static final ImmutableMap<String, Object> LOAD_SPEC = ImmutableMap.of("something", "or_other");
  private static final boolean OVERSHADOWED = true;
  private static final Integer REPLICATION_FACTOR = 2;
  private static final Integer TOTAL_ROWS = 10;
  private static final boolean REALTIME = true;
  private static final int TEST_VERSION = 0x9;
  private static final SegmentStatusInCluster SEGMENT = createSegmentForTest();

  private static ObjectMapper createObjectMapper()
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT);
    objectMapper.setInjectableValues(injectableValues);
    return objectMapper;
  }

  private static SegmentStatusInCluster createSegmentForTest()
  {
    DataSegment dataSegment = DataSegment.builder(SegmentId.of("something", INTERVAL, "1", null))
                                         .shardSpec(NoneShardSpec.instance())
                                         .dimensions(Arrays.asList("dim1", "dim2"))
                                         .metrics(Arrays.asList("met1", "met2"))
                                         .projections(Arrays.asList("proj1", "proj2"))
                                         .loadSpec(LOAD_SPEC)
                                         .binaryVersion(TEST_VERSION)
                                         .size(1)
                                         .totalRows(TOTAL_ROWS)
                                         .build();
    return new SegmentStatusInCluster(dataSegment, OVERSHADOWED, REPLICATION_FACTOR, null, REALTIME);
  }

  @Test
  public void testUnwrappedSegmentWithOvershadowedStatusDeserialization() throws Exception
  {
    final Map<String, Object> objectMap = MAPPER.readValue(
        MAPPER.writeValueAsString(SEGMENT),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    Assertions.assertEquals(16, objectMap.size());
    Assertions.assertEquals("something", objectMap.get("dataSource"));
    Assertions.assertEquals(INTERVAL.toString(), objectMap.get("interval"));
    Assertions.assertEquals("1", objectMap.get("version"));
    Assertions.assertEquals(LOAD_SPEC, objectMap.get("loadSpec"));
    Assertions.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assertions.assertEquals("met1,met2", objectMap.get("metrics"));
    Assertions.assertEquals("proj1,proj2", objectMap.get("projections"));
    Assertions.assertEquals(ImmutableMap.of("type", "none"), objectMap.get("shardSpec"));
    Assertions.assertEquals(TEST_VERSION, objectMap.get("binaryVersion"));
    Assertions.assertEquals(1, objectMap.get("size"));
    Assertions.assertEquals(OVERSHADOWED, objectMap.get("overshadowed"));
    Assertions.assertEquals(REPLICATION_FACTOR, objectMap.get("replicationFactor"));
    Assertions.assertNull(objectMap.get("numRows"));  // From SegmentStatusInCluster constructor
    Assertions.assertEquals(TOTAL_ROWS, objectMap.get("totalRows"));  // From DataSegment
    Assertions.assertEquals(REALTIME, objectMap.get("realtime"));

    final SegmentStatusInCluster deserializedSegment = MAPPER.readValue(
        MAPPER.writeValueAsString(SEGMENT),
        SegmentStatusInCluster.class
    );

    Assertions.assertEquals(SEGMENT.getDataSegment().toString(), deserializedSegment.getDataSegment().toString());
    Assertions.assertEquals(OVERSHADOWED, deserializedSegment.isOvershadowed());
    Assertions.assertEquals(REPLICATION_FACTOR, deserializedSegment.getReplicationFactor());
  }

  // Previously, the implementation of SegmentStatusInCluster had @JsonCreator/@JsonProperty and @JsonUnwrapped
  // on the same field (dataSegment), which used to work in Jackson 2.6, but does not work with Jackson 2.9:
  // https://github.com/FasterXML/jackson-databind/issues/265#issuecomment-264344051
  @Test
  public void testJsonCreatorAndJsonUnwrappedAnnotationsAreCompatible() throws Exception
  {
    String json = MAPPER.writeValueAsString(SEGMENT);
    SegmentStatusInCluster segment = MAPPER.readValue(json, SegmentStatusInCluster.class);
    Assertions.assertEquals(SEGMENT, segment);
    Assertions.assertEquals(json, MAPPER.writeValueAsString(segment));
  }
}
