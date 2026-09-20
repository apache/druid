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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

/**
 */
public class SegmentChangeRequestDropTest
{
  @Test
  public void testV1Serialization() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");

    DataSegment segment = new DataSegment(
        "something",
        interval,
        "1",
        loadSpec,
        Arrays.asList("dim1", "dim2"),
        Arrays.asList("met1", "met2"),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );

    final SegmentChangeRequestDrop segmentDrop = new SegmentChangeRequestDrop(segment);

    Map<String, Object> objectMap = mapper.readValue(
        mapper.writeValueAsString(segmentDrop),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    Assertions.assertEquals(11, objectMap.size());
    Assertions.assertEquals("drop", objectMap.get("action"));
    Assertions.assertEquals("something", objectMap.get("dataSource"));
    Assertions.assertEquals(interval.toString(), objectMap.get("interval"));
    Assertions.assertEquals("1", objectMap.get("version"));
    Assertions.assertEquals(loadSpec, objectMap.get("loadSpec"));
    Assertions.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assertions.assertEquals("met1,met2", objectMap.get("metrics"));
    Assertions.assertEquals(ImmutableMap.of("type", "none"), objectMap.get("shardSpec"));
    Assertions.assertEquals(IndexIO.CURRENT_VERSION_ID, objectMap.get("binaryVersion"));
    Assertions.assertEquals(1, objectMap.get("size"));
  }
}
