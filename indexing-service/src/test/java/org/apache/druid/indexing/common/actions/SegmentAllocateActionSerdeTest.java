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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class SegmentAllocateActionSerdeTest
{
  private final ObjectMapper objectMapper;
  private final SegmentAllocateAction target;

  public SegmentAllocateActionSerdeTest()
  {
    objectMapper = new DefaultObjectMapper();
    objectMapper.registerSubtypes(NumberedPartialShardSpec.class);

    target = new SegmentAllocateAction(
        "datasource",
        DateTimes.nowUtc(),
        Granularities.MINUTE,
        Granularities.HOUR,
        "s1",
        "prev",
        false,
        NumberedPartialShardSpec.instance(),
        LockGranularity.SEGMENT,
        null
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    final SegmentAllocateAction fromJson = (SegmentAllocateAction) objectMapper.readValue(
        objectMapper.writeValueAsBytes(target),
        TaskAction.class
    );

    Assertions.assertEquals(target.getDataSource(), fromJson.getDataSource());
    Assertions.assertEquals(target.getTimestamp(), fromJson.getTimestamp());
    Assertions.assertEquals(target.getQueryGranularity(), fromJson.getQueryGranularity());
    Assertions.assertEquals(target.getPreferredSegmentGranularity(), fromJson.getPreferredSegmentGranularity());
    Assertions.assertEquals(target.getSequenceName(), fromJson.getSequenceName());
    Assertions.assertEquals(target.getPreviousSegmentId(), fromJson.getPreviousSegmentId());
    Assertions.assertEquals(target.isSkipSegmentLineageCheck(), fromJson.isSkipSegmentLineageCheck());
    Assertions.assertEquals(TaskLockType.EXCLUSIVE, target.getTaskLockType());
  }

  @Test
  public void testJsonPropertyNames() throws IOException
  {
    final Map<String, Object> fromJson = objectMapper.readValue(
        objectMapper.writeValueAsBytes(target),
        Map.class
    );

    Assertions.assertEquals(11, fromJson.size());
    Assertions.assertEquals(SegmentAllocateAction.TYPE, fromJson.get("type"));
    Assertions.assertEquals(target.getDataSource(), fromJson.get("dataSource"));
    Assertions.assertEquals(target.getTimestamp(), DateTimes.of((String) fromJson.get("timestamp")));
    Assertions.assertEquals(
        target.getQueryGranularity(),
        Granularity.fromString((String) fromJson.get("queryGranularity"))
    );
    Assertions.assertEquals(
        target.getPreferredSegmentGranularity(),
        Granularity.fromString((String) fromJson.get("preferredSegmentGranularity"))
    );
    Assertions.assertEquals(target.getSequenceName(), fromJson.get("sequenceName"));
    Assertions.assertEquals(target.getPreviousSegmentId(), fromJson.get("previousSegmentId"));
    Assertions.assertEquals(target.isSkipSegmentLineageCheck(), fromJson.get("skipSegmentLineageCheck"));
    Assertions.assertEquals(ImmutableMap.of("type", "numbered"), fromJson.get("shardSpecFactory"));
    Assertions.assertEquals(target.getLockGranularity(), LockGranularity.valueOf((String) fromJson.get("lockGranularity")));
    Assertions.assertEquals(target.getTaskLockType(), TaskLockType.valueOf((String) fromJson.get("taskLockType")));
  }
}
