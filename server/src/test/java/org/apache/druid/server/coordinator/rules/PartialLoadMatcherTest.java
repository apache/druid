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

public class PartialLoadMatcherTest
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
  void testExactMatchProducesResultWhenIntersectionNonEmpty()
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
    Assertions.assertEquals(result.fingerprint(), wrapped.get("ruleFingerprint"));
    Assertions.assertTrue(result.fingerprint().startsWith("v1:"));
    Assertions.assertEquals("v1:".length() + 16, result.fingerprint().length());
  }

  @Test
  void testExactMatchReturnsNullWhenNoIntersection()
  {
    ExactProjectionPartialLoadMatcher matcher = new ExactProjectionPartialLoadMatcher(
        List.of("x", "y")
    );
    DataSegment segment = segmentWithProjections(List.of("a", "b"));
    Assertions.assertNull(matcher.match(segment, segment.getLoadSpec()));
  }

  @Test
  void testExactMatchReturnsNullForProjectionAgnosticSegment()
  {
    ExactProjectionPartialLoadMatcher matcher = new ExactProjectionPartialLoadMatcher(
        List.of("a")
    );
    DataSegment segment = segmentWithProjections(null);
    Assertions.assertNull(matcher.match(segment, segment.getLoadSpec()));
  }

  @Test
  void testExactMatchReturnsNullForEmptyProjectionsList()
  {
    ExactProjectionPartialLoadMatcher matcher = new ExactProjectionPartialLoadMatcher(
        List.of("a")
    );
    DataSegment segment = segmentWithProjections(Collections.emptyList());
    Assertions.assertNull(matcher.match(segment, segment.getLoadSpec()));
  }

  @Test
  void testExactConstructorRejectsNullNames()
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
  void testExactConstructorRejectsEmptyNames()
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
  void testExactMatchSortsAndDeduplicates()
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
  void testExactSerde() throws Exception
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
  void testExactEquals()
  {
    EqualsVerifier.forClass(ExactProjectionPartialLoadMatcher.class).usingGetClass().verify();
  }

  @Test
  void testWildcardConstructorRejectsNullPatterns()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new WildcardProjectionPartialLoadMatcher(null, null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("patterns must not be null or empty")
    );
  }

  @Test
  void testWildcardConstructorRejectsEmptyPatterns()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new WildcardProjectionPartialLoadMatcher(Collections.emptyList(), null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("patterns must not be null or empty")
    );
  }

  @Test
  void testWildcardMatchStarMatchesAll()
  {
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("*"),
        null
    );
    DataSegment segment = segmentWithProjections(List.of("alpha", "beta", "gamma"));

    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        List.of("alpha", "beta", "gamma"),
        result.wrappedLoadSpec().get("projections")
    );
  }

  @Test
  void testWildcardMatchPrefixGlob()
  {
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*"),
        null
    );
    DataSegment segment = segmentWithProjections(
        List.of("user_daily", "user_hourly", "session_daily")
    );
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        List.of("user_daily", "user_hourly"),
        result.wrappedLoadSpec().get("projections")
    );
  }

  @Test
  void testWildcardMatchSingleCharGlob()
  {
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("p?"),
        null
    );
    DataSegment segment = segmentWithProjections(List.of("p1", "p2", "pxx", "q1"));
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        List.of("p1", "p2"),
        result.wrappedLoadSpec().get("projections")
    );
  }

  @Test
  void testWildcardMultiplePatternsUnioned()
  {
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*", "session_d*"),
        null
    );
    DataSegment segment = segmentWithProjections(
        List.of("user_daily", "session_daily", "session_hourly", "other")
    );
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        List.of("session_daily", "user_daily"),
        result.wrappedLoadSpec().get("projections")
    );
  }

  @Test
  void testWildcardReturnsNullForProjectionAgnosticSegment()
  {
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("*"),
        null
    );
    Assertions.assertNull(matcher.match(segmentWithProjections(null), BASE_LOAD_SPEC));
    Assertions.assertNull(matcher.match(segmentWithProjections(Collections.emptyList()), BASE_LOAD_SPEC));
  }

  @Test
  void testWildcardEscapesRegexMetachars()
  {
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("a.b"),
        null
    );
    DataSegment segment = segmentWithProjections(List.of("a.b", "axb"));
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of("a.b"), result.wrappedLoadSpec().get("projections"));
  }

  @Test
  void testWildcardSerde() throws Exception
  {
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*", "p?"),
        null
    );
    String json = OBJECT_MAPPER.writeValueAsString(matcher);
    PartialLoadMatcher reread = OBJECT_MAPPER.readValue(json, PartialLoadMatcher.class);
    Assertions.assertEquals(matcher, reread);
    Assertions.assertInstanceOf(WildcardProjectionPartialLoadMatcher.class, reread);
  }

  @Test
  void testWildcardExcludeRemovesMatchedName()
  {
    // Long-retention rule that loads all user_* projections except user_daily — the latter is
    // intended to live only on a shorter-retention exact-match rule.
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*"),
        List.of("user_daily")
    );
    DataSegment segment = segmentWithProjections(
        List.of("user_daily", "user_hourly", "user_weekly")
    );
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        List.of("user_hourly", "user_weekly"),
        result.wrappedLoadSpec().get("projections")
    );
  }

  @Test
  void testWildcardExcludeNameNotMatchedByPatternIsNoop()
  {
    // Excluding a name that wouldn't have matched anyway is a no-op.
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*"),
        List.of("session_daily")
    );
    DataSegment segment = segmentWithProjections(
        List.of("user_daily", "user_hourly", "session_daily")
    );
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        List.of("user_daily", "user_hourly"),
        result.wrappedLoadSpec().get("projections")
    );
  }

  @Test
  void testWildcardExcludeAllMatchedReturnsNull()
  {
    // If excludes consume every match the result is empty; the matcher reports "does not match".
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*"),
        List.of("user_daily", "user_hourly")
    );
    DataSegment segment = segmentWithProjections(List.of("user_daily", "user_hourly"));
    Assertions.assertNull(matcher.match(segment, segment.getLoadSpec()));
  }

  @Test
  void testWildcardExcludeChangesFingerprint()
  {
    DataSegment segment = segmentWithProjections(List.of("user_daily", "user_hourly"));
    String withoutExcludes = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*"),
        null
    ).match(segment, segment.getLoadSpec()).fingerprint();
    String withExcludes = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*"),
        List.of("user_daily")
    ).match(segment, segment.getLoadSpec()).fingerprint();
    Assertions.assertNotEquals(withoutExcludes, withExcludes);
  }

  @Test
  void testWildcardSerdeWithExcludes() throws Exception
  {
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*"),
        List.of("user_daily")
    );
    String json = OBJECT_MAPPER.writeValueAsString(matcher);
    PartialLoadMatcher reread = OBJECT_MAPPER.readValue(json, PartialLoadMatcher.class);
    Assertions.assertEquals(matcher, reread);
    Assertions.assertEquals(
        List.of("user_daily"),
        ((WildcardProjectionPartialLoadMatcher) reread).getExcludes()
    );
  }

  @Test
  void testWildcardEquals()
  {
    EqualsVerifier.forClass(WildcardProjectionPartialLoadMatcher.class)
                  .withIgnoredFields("excludeSet")
                  .usingGetClass()
                  .verify();
  }

  @Test
  void testFingerprintStableAcrossEquivalentInputOrderAndDuplicates()
  {
    // Two matcher configurations resolving to the same set on the same segment should yield identical fingerprints
    // (the cascade should not thrash on equivalent rule rewordings).
    DataSegment segment = segmentWithProjections(List.of("a", "b", "c"));
    ExactProjectionPartialLoadMatcher orderA = new ExactProjectionPartialLoadMatcher(
        List.of("a", "b")
    );
    ExactProjectionPartialLoadMatcher orderB = new ExactProjectionPartialLoadMatcher(
        List.of("b", "b", "a")
    );
    String fingerprintA = orderA.match(segment, segment.getLoadSpec()).fingerprint();
    String fingerprintB = orderB.match(segment, segment.getLoadSpec()).fingerprint();
    Assertions.assertEquals(fingerprintA, fingerprintB);
  }

  @Test
  void testFingerprintDiffersAcrossDifferentResolvedSets()
  {
    DataSegment segment = segmentWithProjections(List.of("a", "b", "c"));
    String fingerprintAB = new ExactProjectionPartialLoadMatcher(List.of("a", "b"))
        .match(segment, segment.getLoadSpec()).fingerprint();
    String fingerprintAC = new ExactProjectionPartialLoadMatcher(List.of("a", "c"))
        .match(segment, segment.getLoadSpec()).fingerprint();
    Assertions.assertNotEquals(fingerprintAB, fingerprintAC);
  }

  @Test
  void testFingerprintMatchesAcrossExactAndWildcardWhenResolvedSetIdentical()
  {
    // Exact {a,b} and Wildcard {a*} resolving to the same set on this segment should produce the same fingerprint,
    // since fingerprinting is over the resolved list, not the rule text.
    DataSegment segment = segmentWithProjections(List.of("a", "ab"));
    String fingerprintExact = new ExactProjectionPartialLoadMatcher(List.of("a", "ab"))
        .match(segment, segment.getLoadSpec()).fingerprint();
    String fingerprintGlob = new WildcardProjectionPartialLoadMatcher(List.of("a*"), null)
        .match(segment, segment.getLoadSpec()).fingerprint();
    Assertions.assertEquals(fingerprintExact, fingerprintGlob);
  }

  @Test
  void testMatchResultEquals()
  {
    EqualsVerifier.forClass(PartialLoadMatcher.MatchResult.class).usingGetClass().verify();
  }

  private static DataSegment segmentWithProjections(List<String> projections)
  {
    return BUILDER.projections(projections).build();
  }
}
