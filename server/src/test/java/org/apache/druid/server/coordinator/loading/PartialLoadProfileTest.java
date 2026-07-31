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

package org.apache.druid.server.coordinator.loading;

import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartialLoadProfileTest
{
  private static final String FINGERPRINT = "v1:0123456789abcdef";

  private static final Map<String, Object> ORIGINAL_DELEGATE =
      ImmutableMap.of("type", "local", "path", "/var/druid/segments/foo");

  private static final Map<String, Object> MIGRATED_DELEGATE =
      ImmutableMap.of("type", "local", "path", "/mnt/relocated/segments/foo");

  private static final Map<String, Object> WRAPPED = ImmutableMap.of(
      "type", "partialProjection",
      "delegate", ORIGINAL_DELEGATE,
      "projections", List.of("user_daily", "user_hourly"),
      "fingerprint", FINGERPRINT
  );

  @Test
  public void testForRequest()
  {
    PartialLoadProfile profile = PartialLoadProfile.forRequest(WRAPPED, FINGERPRINT);
    Assertions.assertEquals(WRAPPED, profile.wrappedLoadSpec());
    Assertions.assertEquals(FINGERPRINT, profile.fingerprint());
    Assertions.assertNull(profile.loadedBytes());
  }

  @Test
  public void testForRequestRejectsNullWrappedLoadSpec()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> PartialLoadProfile.forRequest(null, FINGERPRINT)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("wrappedLoadSpec must not be null or empty")
    );
  }

  @Test
  public void testForRequestRejectsEmptyWrappedLoadSpec()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> PartialLoadProfile.forRequest(Map.of(), FINGERPRINT)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("wrappedLoadSpec must not be null or empty")
    );
  }

  @Test
  public void testForLoaded()
  {
    PartialLoadProfile profile = PartialLoadProfile.forLoaded(WRAPPED, FINGERPRINT, 12345L);
    Assertions.assertEquals(WRAPPED, profile.wrappedLoadSpec());
    Assertions.assertEquals(FINGERPRINT, profile.fingerprint());
    Assertions.assertEquals(12345L, profile.loadedBytes());
  }

  @Test
  public void testForLoadedRejectsEmptyWrappedLoadSpec()
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            DruidException.class,
            () -> PartialLoadProfile.forLoaded(Map.of(), FINGERPRINT, 100L)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("wrappedLoadSpec must not be null or empty")
    );
  }

  @Test
  public void testFingerprintRequired()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> PartialLoadProfile.forRequest(WRAPPED, null)
    );
  }

  @Test
  public void testDefensiveCopyOfWrappedLoadSpec()
  {
    Map<String, Object> mutable = new HashMap<>();
    mutable.put("type", "partialProjection");
    PartialLoadProfile profile = PartialLoadProfile.forRequest(mutable, FINGERPRINT);
    mutable.put("extra", "added-after");
    Assertions.assertFalse(profile.wrappedLoadSpec().containsKey("extra"));
  }

  @Test
  public void testAsRequestForRebasesDelegateOntoTheSegmentsCurrentLoadSpec()
  {
    // A profile read off a server carries the location the segment had when that server was asked to load. Reissuing
    // it must point at where the segment lives now, keeping the selection and fingerprint that identify the request.
    final PartialLoadProfile loaded = PartialLoadProfile.forLoaded(WRAPPED, FINGERPRINT, 12345L);

    final PartialLoadProfile request = loaded.asRequestFor(segmentWithLoadSpec(MIGRATED_DELEGATE));

    Assertions.assertEquals(MIGRATED_DELEGATE, request.wrappedLoadSpec().get("delegate"));
    Assertions.assertEquals(FINGERPRINT, request.fingerprint());
    Assertions.assertEquals(List.of("user_daily", "user_hourly"), request.wrappedLoadSpec().get("projections"));
    Assertions.assertEquals("partialProjection", request.wrappedLoadSpec().get("type"));
    Assertions.assertNull(request.loadedBytes(), "a request carries no realized footprint");
  }

  @Test
  public void testAsRequestForKeepsDelegateWhenSegmentIsUnmoved()
  {
    final PartialLoadProfile request = PartialLoadProfile.forRequest(WRAPPED, FINGERPRINT);
    Assertions.assertSame(request, request.asRequestFor(segmentWithLoadSpec(ORIGINAL_DELEGATE)));
  }

  @Test
  public void testAsRequestForKeepsDelegateWhenSegmentHasNoLoadSpec()
  {
    // Nothing better to point the wrapper at, so the existing delegate rides through rather than being replaced by an
    // empty one the historical would reject.
    final PartialLoadProfile loaded = PartialLoadProfile.forLoaded(WRAPPED, FINGERPRINT, 12345L);

    final PartialLoadProfile request = loaded.asRequestFor(segmentWithLoadSpec(null));

    Assertions.assertEquals(ORIGINAL_DELEGATE, request.wrappedLoadSpec().get("delegate"));
    Assertions.assertNull(request.loadedBytes());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(PartialLoadProfile.class)
                  .withNonnullFields("wrappedLoadSpec", "fingerprint")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testInterningSharesReferenceForEquivalentProfiles()
  {
    // Two semantically-identical forLoaded calls (same wrappedLoadSpec contents, same fingerprint, same loadedBytes)
    // should resolve to the same instance via the static interner. This is the win that lets multiple replicas of the
    // same partial load share the heavy wrappedLoadSpec map by reference.
    Map<String, Object> a = new HashMap<>(WRAPPED);
    Map<String, Object> b = new HashMap<>(WRAPPED);
    PartialLoadProfile pa = PartialLoadProfile.forLoaded(a, FINGERPRINT, 12345L);
    PartialLoadProfile pb = PartialLoadProfile.forLoaded(b, FINGERPRINT, 12345L);
    Assertions.assertSame(pa, pb);

    // Different loadedBytes ⇒ different profile, no sharing.
    PartialLoadProfile pc = PartialLoadProfile.forLoaded(WRAPPED, FINGERPRINT, 99999L);
    Assertions.assertNotSame(pa, pc);

    // Different fingerprint ⇒ different profile, no sharing.
    PartialLoadProfile pd = PartialLoadProfile.forLoaded(WRAPPED, "v1:differentfingerprint", 12345L);
    Assertions.assertNotSame(pa, pd);
  }

  private static DataSegment segmentWithLoadSpec(@Nullable Map<String, Object> loadSpec)
  {
    return DataSegment.builder(SegmentId.of("wiki", Intervals.of("2025/2026"), "v1", 0))
                      .shardSpec(new NumberedShardSpec(0, 1))
                      .loadSpec(loadSpec)
                      .size(100L)
                      .build();
  }
}
