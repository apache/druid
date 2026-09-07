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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Cross-cutting tests for the {@link PartialLoadMatcher} interface itself: fingerprint stability across matcher
 * implementations, the {@link PartialLoadMatcher.MatchResult} value type, and {@link UnknownPartialLoadMatcher}
 * (the {@code defaultImpl} fallback for unrecognized matcher types). Per-matcher behavior tests live in their own
 * test classes (see {@link ExactProjectionPartialLoadMatcherTest},
 * {@link WildcardProjectionPartialLoadMatcherTest}).
 */
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

  @Test
  void testUnknownTypeDeserializesToFallback() throws Exception
  {
    // A matcher type unknown to this Druid version should fall back to UnknownPartialLoadMatcher rather than fail to
    // parse, so the rules cascade can keep working when an older coordinator reads a rule authored on a newer version.
    String json = "{\"type\": \"someFutureMatcherType\", \"someFutureField\": [\"x\", \"y\"]}";
    PartialLoadMatcher matcher = OBJECT_MAPPER.readValue(json, PartialLoadMatcher.class);
    Assertions.assertInstanceOf(UnknownPartialLoadMatcher.class, matcher);
  }

  @Test
  void testUnknownMatcherDoesNotApply()
  {
    // An unknown matcher returns null from match() so the rule's appliesTo defers to the rule's onCannotMatch
    // behavior (FALL_THROUGH continues the cascade; FULL_LOAD applies the rule as a full load).
    UnknownPartialLoadMatcher matcher = new UnknownPartialLoadMatcher();
    DataSegment segment = segmentWithProjections(List.of("a", "b"));
    Assertions.assertNull(matcher.match(segment, segment.getLoadSpec()));
  }

  @Test
  void testUnknownMatcherEquals()
  {
    Assertions.assertEquals(new UnknownPartialLoadMatcher(), new UnknownPartialLoadMatcher());
    Assertions.assertEquals(
        new UnknownPartialLoadMatcher().hashCode(),
        new UnknownPartialLoadMatcher().hashCode()
    );
  }

  private static DataSegment segmentWithProjections(List<String> projections)
  {
    return BUILDER.projections(projections).build();
  }
}
