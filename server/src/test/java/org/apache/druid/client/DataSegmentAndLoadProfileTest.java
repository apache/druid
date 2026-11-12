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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class DataSegmentAndLoadProfileTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  private static final Map<String, Object> WRAPPED_LOAD_SPEC = Map.of(
      "type", "partialProjection",
      "delegate", Map.of("type", "local", "path", "/tmp/segment"),
      "projections", List.of("proj1"),
      "fingerprint", "v1:0123456789abcdef"
  );

  private static final DataSegment SEGMENT =
      DataSegment.builder(SegmentId.of("ds", Intervals.of("2026-01-01/2026-01-02"), "v1", 0))
                 .loadSpec(WRAPPED_LOAD_SPEC)
                 .dimensions(List.of("dim1"))
                 .metrics(List.of("met1"))
                 .projections(List.of("proj1"))
                 .shardSpec(new NumberedShardSpec(0, 1))
                 .size(1000L)
                 .totalRows(10)
                 .build();

  private static final PartialLoadProfile PROFILE =
      PartialLoadProfile.forLoaded(WRAPPED_LOAD_SPEC, "v1:0123456789abcdef", 100L);

  @Test
  public void testSharesDelegateState()
  {
    final DataSegmentAndLoadProfile wrapped = new DataSegmentAndLoadProfile(SEGMENT, PROFILE);

    Assertions.assertSame(SEGMENT.getId(), wrapped.getId());
    Assertions.assertSame(SEGMENT.getLoadSpec(), wrapped.getLoadSpec());
    Assertions.assertSame(SEGMENT.getDimensions(), wrapped.getDimensions());
    Assertions.assertSame(SEGMENT.getShardSpec(), wrapped.getShardSpec());
    Assertions.assertSame(PROFILE, wrapped.profile());
    Assertions.assertEquals(SEGMENT, wrapped);
    Assertions.assertEquals(100L, wrapped.effectiveSize());
  }

  @Test
  public void testSerializesAsTheDelegate() throws Exception
  {
    // the profile travels in SegmentChangeRequestLoad wire fields, never as a segment property. Compared as maps since
    // Jackson orders the subclass's properties differently.
    Assertions.assertEquals(
        toMap(SEGMENT),
        toMap(new DataSegmentAndLoadProfile(SEGMENT, PROFILE))
    );
  }

  private static Map<String, Object> toMap(DataSegment segment) throws Exception
  {
    return MAPPER.readValue(MAPPER.writeValueAsString(segment), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
  }
}
