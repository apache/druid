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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExactProjectionPartialLoadMatcherTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  private static final Map<String, Object> BASE_LOAD_SPEC = Map.of(
      "type", "local",
      "path", "/var/druid/segments/foo"
  );

  private static final DataSegment.Builder BUILDER = DataSegment
      .builder(
          SegmentId.of(
              "test",
              Intervals.of("2024-01-01/2024-02-01"),
              DateTimes.nowUtc().toString(),
              new NumberedShardSpec(0, 0)
          )
      )
      .loadSpec(BASE_LOAD_SPEC)
      .size(0);

  @Test
  void testMatchProducesResultWhenIntersectionNonEmpty()
  {
    ExactProjectionPartialLoadMatcher matcher = new ExactProjectionPartialLoadMatcher(
        List.of("c", "a", "x")
    );
    DataSegment segment = segmentWithProjections(List.of("a", "b", "c"));

    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);

    Map<String, Object> wrapped = result.wrappedLoadSpec();
    Assertions.assertEquals("partialProjection", wrapped.get("type"));
    Assertions.assertEquals(BASE_LOAD_SPEC, wrapped.get("delegate"));
    Assertions.assertEquals(List.of("a", "c"), wrapped.get("projections"));
    Assertions.assertEquals(result.fingerprint(), wrapped.get("fingerprint"));
    Assertions.assertTrue(result.fingerprint().startsWith("v1:"));
    Assertions.assertEquals("v1:".length() + 16, result.fingerprint().length());
  }

  @Test
  void testMatchReturnsNullWhenNoIntersection()
  {
    ExactProjectionPartialLoadMatcher matcher = new ExactProjectionPartialLoadMatcher(
        List.of("x", "y")
    );
    DataSegment segment = segmentWithProjections(List.of("a", "b"));
    Assertions.assertNull(matcher.match(segment, segment.getLoadSpec()));
  }

  @Test
  void testMatchReturnsNullForProjectionAgnosticSegment()
  {
    ExactProjectionPartialLoadMatcher matcher = new ExactProjectionPartialLoadMatcher(
        List.of("a")
    );
    DataSegment segment = segmentWithProjections(null);
    Assertions.assertNull(matcher.match(segment, segment.getLoadSpec()));
  }

  @Test
  void testMatchReturnsNullForEmptyProjectionsList()
  {
    ExactProjectionPartialLoadMatcher matcher = new ExactProjectionPartialLoadMatcher(
        List.of("a")
    );
    DataSegment segment = segmentWithProjections(Collections.emptyList());
    Assertions.assertNull(matcher.match(segment, segment.getLoadSpec()));
  }

  @Test
  void testConstructorRejectsNullNames()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new ExactProjectionPartialLoadMatcher(null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("names must not be null or empty")
    );
  }

  @Test
  void testConstructorRejectsEmptyNames()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new ExactProjectionPartialLoadMatcher(Collections.emptyList())
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("names must not be null or empty")
    );
  }

  @Test
  void testMatchSortsAndDeduplicates()
  {
    ExactProjectionPartialLoadMatcher matcher = new ExactProjectionPartialLoadMatcher(
        List.of("c", "a", "a", "b")
    );
    DataSegment segment = segmentWithProjections(List.of("a", "b", "c"));
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of("a", "b", "c"), result.wrappedLoadSpec().get("projections"));
  }

  @Test
  void testSerde() throws Exception
  {
    ExactProjectionPartialLoadMatcher matcher = new ExactProjectionPartialLoadMatcher(
        List.of("a", "b")
    );
    String json = OBJECT_MAPPER.writeValueAsString(matcher);
    PartialLoadMatcher reread = OBJECT_MAPPER.readValue(json, PartialLoadMatcher.class);
    Assertions.assertEquals(matcher, reread);
    Assertions.assertInstanceOf(ExactProjectionPartialLoadMatcher.class, reread);
  }

  @Test
  void testEquals()
  {
    EqualsVerifier.forClass(ExactProjectionPartialLoadMatcher.class).usingGetClass().verify();
  }

  private static DataSegment segmentWithProjections(List<String> projections)
  {
    return BUILDER.projections(projections).build();
  }
}
