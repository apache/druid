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

public class WildcardProjectionPartialLoadMatcherTest
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
  void testConstructorRejectsNullPatterns()
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
  void testConstructorRejectsEmptyPatterns()
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
  void testMatchStarMatchesAll()
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
  void testMatchPrefixGlob()
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
  void testMatchSingleCharGlob()
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
  void testMatchSuffixGlob()
  {
    // Wildcard at the start of the pattern: anchors enforced by full-string match.
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("*_daily"),
        null
    );
    DataSegment segment = segmentWithProjections(List.of("user_daily", "session_daily", "user_hourly"));
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        List.of("session_daily", "user_daily"),
        result.wrappedLoadSpec().get("projections")
    );
  }

  @Test
  void testMatchMidStringGlob()
  {
    // Wildcard between two literal fragments: full-string match required on both ends.
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*_v2"),
        null
    );
    DataSegment segment = segmentWithProjections(
        List.of("user_daily_v2", "user_hourly_v2", "user_daily", "session_daily_v2")
    );
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        List.of("user_daily_v2", "user_hourly_v2"),
        result.wrappedLoadSpec().get("projections")
    );
  }

  @Test
  void testMatchIsCaseSensitive()
  {
    // matching is case-sensitive
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("User_*"),
        null
    );
    DataSegment segment = segmentWithProjections(List.of("user_daily", "User_daily"));
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of("User_daily"), result.wrappedLoadSpec().get("projections"));
  }

  @Test
  void testMatchReturnsNullWhenNoPatternsHit()
  {
    // Segment has projections but none match any configured pattern: distinct code path from the
    // projection-agnostic-segment short-circuit.
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*"),
        null
    );
    DataSegment segment = segmentWithProjections(List.of("session_daily", "session_hourly", "other"));
    Assertions.assertNull(matcher.match(segment, segment.getLoadSpec()));
  }

  @Test
  void testMultiplePatternsUnioned()
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
  void testReturnsNullForProjectionAgnosticSegment()
  {
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("*"),
        null
    );
    Assertions.assertNull(matcher.match(segmentWithProjections(null), BASE_LOAD_SPEC));
    Assertions.assertNull(matcher.match(segmentWithProjections(Collections.emptyList()), BASE_LOAD_SPEC));
  }

  @Test
  void testEscapesRegexMetachars()
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
  void testEscapesAsterisk()
  {
    // \\* in the pattern matches a literal asterisk, not the glob wildcard.
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("foo\\*bar"),
        null
    );
    DataSegment segment = segmentWithProjections(List.of("foo*bar", "fooXYZbar", "foo*bar*baz"));
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of("foo*bar"), result.wrappedLoadSpec().get("projections"));
  }

  @Test
  void testEscapesQuestionMark()
  {
    // \\? in the pattern matches a literal question mark, not the glob single-char wildcard.
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("foo\\?bar"),
        null
    );
    DataSegment segment = segmentWithProjections(List.of("foo?bar", "fooXbar"));
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of("foo?bar"), result.wrappedLoadSpec().get("projections"));
  }

  @Test
  void testEscapesBackslash()
  {
    // \\\\ in the Java string is \\ in the pattern, which represents a single literal backslash.
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("foo\\\\bar"),
        null
    );
    DataSegment segment = segmentWithProjections(List.of("foo\\bar", "foobar"));
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of("foo\\bar"), result.wrappedLoadSpec().get("projections"));
  }

  @Test
  void testRejectsTrailingBackslash()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new WildcardProjectionPartialLoadMatcher(List.of("foo\\"), null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("ends with an unescaped backslash")
    );
  }

  @Test
  void testRejectsTrailingBackslashInExcludePatterns()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> new WildcardProjectionPartialLoadMatcher(List.of("*"), List.of("foo\\"))
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("ends with an unescaped backslash")
    );
  }

  @Test
  void testSerde() throws Exception
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
  void testExcludeLiteralRemovesMatchedName()
  {
    // Long-retention rule that loads all user_* projections except user_daily, the latter is intended to live only on
    // a shorter-retention exact-match rule. A literal name is a zero-wildcard glob, so the same excludePatterns field
    // covers this case.
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
  void testExcludePatternRemovesMatchingNames()
  {
    // Broad rule that loads everything except names handled by a different, more specific rule.
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("*"),
        List.of("user_*")
    );
    DataSegment segment = segmentWithProjections(
        List.of("user_daily", "user_hourly", "session_daily", "other")
    );
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        List.of("other", "session_daily"),
        result.wrappedLoadSpec().get("projections")
    );
  }

  @Test
  void testMultipleExcludePatternsUnioned()
  {
    // Multiple excludePatterns are OR'd: a name matching any one of them is excluded.
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("*"),
        List.of("user_*", "session_temp_*")
    );
    DataSegment segment = segmentWithProjections(
        List.of("user_daily", "session_temp_x", "session_daily", "other")
    );
    PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        List.of("other", "session_daily"),
        result.wrappedLoadSpec().get("projections")
    );
  }

  @Test
  void testExcludeNotMatchedIsNoop()
  {
    // Excluding a pattern that doesn't match anything in the segment is a no-op.
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*"),
        List.of("session_*")
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
  void testExcludeAllMatchedReturnsNull()
  {
    // If excludePatterns consume every match the result is empty; the matcher reports "does not match".
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("user_*"),
        List.of("user_*")
    );
    DataSegment segment = segmentWithProjections(List.of("user_daily", "user_hourly"));
    Assertions.assertNull(matcher.match(segment, segment.getLoadSpec()));
  }

  @Test
  void testExcludeChangesFingerprint()
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
  void testSerdeWithExcludePatterns() throws Exception
  {
    WildcardProjectionPartialLoadMatcher matcher = new WildcardProjectionPartialLoadMatcher(
        List.of("*"),
        List.of("user_*")
    );
    String json = OBJECT_MAPPER.writeValueAsString(matcher);
    PartialLoadMatcher reread = OBJECT_MAPPER.readValue(json, PartialLoadMatcher.class);
    Assertions.assertEquals(matcher, reread);
    Assertions.assertEquals(
        List.of("user_*"),
        ((WildcardProjectionPartialLoadMatcher) reread).getExcludePatterns()
    );
  }

  @Test
  void testEquals()
  {
    EqualsVerifier.forClass(WildcardProjectionPartialLoadMatcher.class)
                  .withIgnoredFields("compiledPatterns", "compiledExcludePatterns")
                  .usingGetClass()
                  .verify();
  }

  private static DataSegment segmentWithProjections(List<String> projections)
  {
    return BUILDER.projections(projections).build();
  }
}
