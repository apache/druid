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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link DruidServer}'s partial-load profile bookkeeping: profile-aware {@code currSize} accounting,
 * profile retrieval, and balanced add/remove of profile state.
 */
public class DruidServerPartialLoadTest
{
  private static final String FINGERPRINT = "v1:0123456789abcdef";

  private static DruidServer newServer()
  {
    return new DruidServer(
        "test",
        "localhost:8083",
        null,
        100_000_000L,
        100_000_000L,
        ServerType.HISTORICAL,
        "_default_tier",
        0
    );
  }

  private static DataSegment buildSegment(String dataSource, String version, long size)
  {
    return new DataSegment(
        dataSource,
        Intervals.of("2024-01-01/2024-02-01"),
        version,
        ImmutableMap.of("type", "local", "path", "/var/druid/segments/foo"),
        null,
        null,
        NoneShardSpec.instance(),
        9,
        size
    );
  }

  @Test
  void testAddSegmentWithoutProfileUsesSegmentSizeForCurrSize()
  {
    DruidServer server = newServer();
    DataSegment segment = buildSegment("ds", "v1", 1000L);
    server.addDataSegment(segment);
    Assertions.assertEquals(1000L, server.getCurrSize());
    Assertions.assertNull(server.getPartialLoadProfile(segment.getId()));
  }

  @Test
  void testAddSegmentWithProfileUsesLoadedBytesForCurrSize()
  {
    DruidServer server = newServer();
    DataSegment segment = buildSegment("ds", "v1", 1000L);
    PartialLoadProfile profile = PartialLoadProfile.forLoaded(
        ImmutableMap.of("type", "partialProjection", "fingerprint", FINGERPRINT),
        FINGERPRINT,
        300L
    );
    server.addDataSegment(segment, profile);
    Assertions.assertEquals(300L, server.getCurrSize());
    Assertions.assertEquals(profile, server.getPartialLoadProfile(segment.getId()));
  }

  @Test
  void testRemoveSegmentSubtractsProfileLoadedBytes()
  {
    DruidServer server = newServer();
    DataSegment segment = buildSegment("ds", "v1", 1000L);
    PartialLoadProfile profile = PartialLoadProfile.forLoaded(
        ImmutableMap.of("type", "partialProjection", "fingerprint", FINGERPRINT),
        FINGERPRINT,
        300L
    );
    server.addDataSegment(segment, profile);
    server.removeDataSegment(segment.getId());
    Assertions.assertEquals(0L, server.getCurrSize());
    Assertions.assertNull(server.getPartialLoadProfile(segment.getId()));
  }

  @Test
  void testFullFallbackProfileUsesFullSegmentSize()
  {
    // Historical was asked to partial-load but fell back to full download; profile.loadedBytes equals
    // segment.getSize, so currSize accounting reflects the full footprint.
    DruidServer server = newServer();
    DataSegment segment = buildSegment("ds", "v1", 1000L);
    PartialLoadProfile profile = PartialLoadProfile.forFullFallback(FINGERPRINT, segment.getSize());
    server.addDataSegment(segment, profile);
    Assertions.assertEquals(1000L, server.getCurrSize());
    Assertions.assertEquals(profile, server.getPartialLoadProfile(segment.getId()));
    Assertions.assertTrue(profile.isFullFallback());
  }

  @Test
  void testMixedFullAndPartialReplicasAccount()
  {
    // Two segments on the same server: one full-loaded, one partial. currSize sums correctly.
    DruidServer server = newServer();
    DataSegment fullSegment = buildSegment("ds", "v1", 1000L);
    DataSegment partialSegment = buildSegment("ds", "v2", 5000L);
    PartialLoadProfile profile = PartialLoadProfile.forLoaded(
        ImmutableMap.of("type", "partialProjection", "fingerprint", FINGERPRINT),
        FINGERPRINT,
        500L
    );
    server.addDataSegment(fullSegment);
    server.addDataSegment(partialSegment, profile);
    Assertions.assertEquals(1500L, server.getCurrSize());
    Assertions.assertNull(server.getPartialLoadProfile(fullSegment.getId()));
    Assertions.assertEquals(profile, server.getPartialLoadProfile(partialSegment.getId()));
  }
}
