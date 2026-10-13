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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TaskReportDataTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    final TaskReportData<String, Long> data = new TaskReportData<>(
        "task1",
        Map.of("partition1", 100L),
        Map.of("partition1", 200L),
        DateTimes.nowUtc(),
        30L,
        TaskReportData.TaskType.ACTIVE,
        Map.of("partition1", 5L),
        Map.of("partition1", 1000L)
    );

    final String json = mapper.writeValueAsString(data);
    final TaskReportData<?, ?> deserialized = mapper.readValue(json, TaskReportData.class);

    Assertions.assertEquals(data.getId(), deserialized.getId());
    Assertions.assertEquals(data.getType(), deserialized.getType());
    Assertions.assertEquals(data.getRemainingSeconds(), deserialized.getRemainingSeconds());
    Assertions.assertEquals(data.getStartTime(), deserialized.getStartTime());

    // startingOffsets/currentOffsets/lag/lagMillis are bound to throwaway property names
    // (see the constructor), so they never come back from JSON.
    Assertions.assertNull(deserialized.getStartingOffsets());
    Assertions.assertNull(deserialized.getCurrentOffsets());
    Assertions.assertNull(deserialized.getLag());
    Assertions.assertNull(deserialized.getLagMillis());
  }
}
