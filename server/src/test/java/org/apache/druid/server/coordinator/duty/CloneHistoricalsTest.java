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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CloneStatusManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.server.coordinator.loading.SegmentHolder;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Verifies that {@link CloneHistoricals} reproduces the source historical's load state on its clone target, including
 * the {@link PartialLoadProfile} a partial-load rule resolved to. A clone that receives the segment without the
 * profile loads the whole segment and announces the full segment size, so the two servers diverge in both on-disk
 * footprint and reported {@code curr_size}. The duty therefore compares replicas by partial-load fingerprint rather
 * than by segment id, and threads the source's wrapped load spec into the clone's load request.
 */
public class CloneHistoricalsTest
{
  private static final String TIER = "tier1";
  private static final String SOURCE_HOST = "source_host:8083";
  private static final String TARGET_HOST = "target_host:8083";

  private static final String FP_REVENUE = "v1:deadbeefcafebabe";
  private static final String FP_USERS = "v1:0123456789abcdef";

  private static final long SEGMENT_SIZE = 1000L;
  private static final long REALIZED_BYTES = 250L;

  private SegmentLoadQueueManager loadQueueManager;
  private CloneHistoricals duty;

  @BeforeEach
  public void setUp()
  {
    loadQueueManager = new SegmentLoadQueueManager(null, null);
    duty = new CloneHistoricals(loadQueueManager, new CloneStatusManager());
  }

  @Test
  public void testCloneLoadsSegmentWithTheSourcePartialLoadProfile()
  {
    final DataSegment segment = createSegment();
    final ServerHolder source = createServer(SOURCE_HOST, segment, loadedProfile(FP_REVENUE, "revenue"));
    final ServerHolder target = createServer(TARGET_HOST);

    runDuty(source, target, segment);

    final PartialLoadProfile queued = peonOf(target).getProfileFor(segment);
    Assertions.assertNotNull(queued, "Clone must be asked to load the same parts as its source");
    Assertions.assertEquals(FP_REVENUE, queued.fingerprint());
    Assertions.assertEquals(loadedProfile(FP_REVENUE, "revenue").wrappedLoadSpec(), queued.wrappedLoadSpec());
    Assertions.assertNull(queued.loadedBytes(), "Outbound request profile must not carry loadedBytes");
  }

  @Test
  public void testCloneLoadsSegmentWithTheProfileOfAnInFlightSourceLoad()
  {
    // The source's own load is still queued, so the profile lives only on the peon's in-flight holder. The clone
    // must follow the state the source is heading towards, not the state it is in.
    final DataSegment segment = createSegment();
    final PartialLoadProfile inFlight = requestProfile(FP_REVENUE, "revenue");

    final TestLoadQueuePeon sourcePeon = new TestLoadQueuePeon();
    sourcePeon.addInFlightHolder(
        new SegmentHolder(segment, SegmentAction.LOAD, inFlight, Duration.standardSeconds(10), null)
    );
    final ServerHolder source = new ServerHolder(createDruidServer(SOURCE_HOST).toImmutableDruidServer(), sourcePeon);
    final ServerHolder target = createServer(TARGET_HOST);

    runDuty(source, target, segment);

    final PartialLoadProfile queued = peonOf(target).getProfileFor(segment);
    Assertions.assertNotNull(queued, "Clone must follow an in-flight partial load on the source");
    Assertions.assertEquals(FP_REVENUE, queued.fingerprint());
  }

  @Test
  public void testCloneReloadsWhenItsFingerprintDiffersFromTheSource()
  {
    // Both servers hold the segment, but under different rules. The segment ids are identical, so only the
    // fingerprint distinguishes them.
    final DataSegment segment = createSegment();
    final ServerHolder source = createServer(SOURCE_HOST, segment, loadedProfile(FP_USERS, "users"));
    final ServerHolder target = createServer(TARGET_HOST, segment, loadedProfile(FP_REVENUE, "revenue"));

    runDuty(source, target, segment);

    final PartialLoadProfile queued = peonOf(target).getProfileFor(segment);
    Assertions.assertNotNull(queued, "Clone holding a different set of parts must be re-loaded");
    Assertions.assertEquals(FP_USERS, queued.fingerprint());
  }

  @Test
  public void testCloneReloadsAsFullLoadWhenSourceNoLongerLoadsPartially()
  {
    // Source moved off the partial-load rule and now holds the whole segment; the clone must follow it back. The
    // request goes out with no profile even though the clone is already serving the segment: the historical releases
    // the partial-load rule it holds the replica under when it receives an unwrapped load request.
    final DataSegment segment = createSegment();
    final ServerHolder source = createServer(SOURCE_HOST, segment, null);
    final ServerHolder target = createServer(TARGET_HOST, segment, loadedProfile(FP_REVENUE, "revenue"));

    runDuty(source, target, segment);

    Assertions.assertTrue(
        peonOf(target).getSegmentsToLoad().contains(segment),
        "Clone must be re-loaded when the source stops loading partially"
    );
    Assertions.assertNull(
        peonOf(target).getProfileFor(segment),
        "A full-load source must not thread a profile to the clone"
    );
    Assertions.assertTrue(peonOf(target).getSegmentsToDrop().isEmpty());
  }

  @Test
  public void testCloneWithAPartialLoadStillQueuedIsConvertedOnALaterRun()
  {
    // A partial load can be queued on top of a replica the clone already serves under a different profile, which is how
    // the historical is asked to fill in the missing parts in place. A segment with an operation already queued cannot
    // take another one, so the queued load is left to complete and the next run converts the replica it produces.
    final DataSegment segment = createSegment();
    final ServerHolder source = createServer(SOURCE_HOST, segment, null);

    final TestLoadQueuePeon targetPeon = new TestLoadQueuePeon();
    targetPeon.addInFlightHolder(new SegmentHolder(
        segment,
        SegmentAction.LOAD,
        requestProfile(FP_USERS, "users"),
        Duration.standardSeconds(10),
        null
    ));
    final DruidServer targetDruidServer = createDruidServer(TARGET_HOST);
    targetDruidServer.addDataSegment(segment, loadedProfile(FP_REVENUE, "revenue"));
    final ServerHolder target = new ServerHolder(targetDruidServer.toImmutableDruidServer(), targetPeon);

    runDuty(source, target, segment);

    Assertions.assertEquals(
        requestProfile(FP_USERS, "users"),
        targetPeon.getProfileFor(segment),
        "The queued partial load must be left alone"
    );
    Assertions.assertTrue(targetPeon.getSegmentsToDrop().isEmpty());
  }

  @Test
  public void testFullLoadSourceQueuesPlainLoadOnClone()
  {
    final DataSegment segment = createSegment();
    final ServerHolder source = createServer(SOURCE_HOST, segment, null);
    final ServerHolder target = createServer(TARGET_HOST);

    runDuty(source, target, segment);

    Assertions.assertTrue(peonOf(target).getSegmentsToLoad().contains(segment));
    Assertions.assertNull(peonOf(target).getProfileFor(segment));
  }

  @Test
  public void testNothingIsQueuedWhenCloneFingerprintMatchesTheSource()
  {
    final DataSegment segment = createSegment();
    final ServerHolder source = createServer(SOURCE_HOST, segment, loadedProfile(FP_REVENUE, "revenue"));
    final ServerHolder target = createServer(TARGET_HOST, segment, loadedProfile(FP_REVENUE, "revenue"));

    runDuty(source, target, segment);

    Assertions.assertTrue(peonOf(target).getSegmentsToLoad().isEmpty());
    Assertions.assertTrue(peonOf(target).getSegmentsToDrop().isEmpty());
  }

  @Test
  public void testNothingIsQueuedWhenCloneFellBackToAFullDownloadOfTheSameRequest()
  {
    // A clone whose historical cannot honour partial downloads announces the requested fingerprint with the full
    // segment size as its footprint. The request was satisfied, so the duty must leave it alone rather than
    // re-queueing the load on every run.
    final DataSegment segment = createSegment();
    final ServerHolder source = createServer(SOURCE_HOST, segment, loadedProfile(FP_REVENUE, "revenue"));
    final ServerHolder target = createServer(
        TARGET_HOST,
        segment,
        PartialLoadProfile.forLoaded(wrappedLoadSpec(FP_REVENUE, "revenue"), FP_REVENUE, SEGMENT_SIZE)
    );

    runDuty(source, target, segment);

    Assertions.assertTrue(peonOf(target).getSegmentsToLoad().isEmpty());
    Assertions.assertTrue(peonOf(target).getSegmentsToDrop().isEmpty());
  }

  @Test
  public void testSegmentMissingFromSourceIsDroppedFromClone()
  {
    final DataSegment segment = createSegment();
    final ServerHolder source = createServer(SOURCE_HOST);
    final ServerHolder target = createServer(TARGET_HOST, segment, loadedProfile(FP_REVENUE, "revenue"));

    runDuty(source, target, segment);

    Assertions.assertTrue(peonOf(target).getSegmentsToDrop().contains(segment));
    Assertions.assertTrue(peonOf(target).getSegmentsToLoad().isEmpty());
  }

  private void runDuty(ServerHolder source, ServerHolder target, DataSegment... usedSegments)
  {
    final DruidCluster cluster = DruidCluster.builder().addTier(TIER, source, target).build();
    final DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams
            .builder()
            .withDruidCluster(cluster)
            .withUsedSegments(usedSegments)
            .withDynamicConfigs(
                CoordinatorDynamicConfig.builder()
                                        .withCloneServers(Map.of(TARGET_HOST, SOURCE_HOST))
                                        .build()
            )
            .build();

    duty.run(params);
  }

  private static TestLoadQueuePeon peonOf(ServerHolder server)
  {
    return (TestLoadQueuePeon) server.getPeon();
  }

  private static DruidServer createDruidServer(String host)
  {
    return new DruidServer(host, host, null, 10L << 30, null, ServerType.HISTORICAL, TIER, 0);
  }

  /**
   * Creates a server holder that serves each of the given segments. A non-null profile announces the segment as a
   * partial load with that profile; a null profile announces it as a regular full load.
   */
  private static ServerHolder createServer(String host, DataSegment segment, @Nullable PartialLoadProfile profile)
  {
    final DruidServer server = createDruidServer(host);
    server.addDataSegment(segment, profile);
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }

  private static ServerHolder createServer(String host)
  {
    return new ServerHolder(createDruidServer(host).toImmutableDruidServer(), new TestLoadQueuePeon());
  }

  private static DataSegment createSegment()
  {
    return DataSegment
        .builder(
            SegmentId.of(
                TestDataSource.WIKI,
                Intervals.of("2024/2025"),
                DateTimes.nowUtc().toString(),
                new NumberedShardSpec(0, 0)
            )
        )
        .loadSpec(Map.of("type", "local", "path", "/var/druid/segments/foo"))
        .projections(List.of("revenue", "users"))
        .size(SEGMENT_SIZE)
        .build();
  }

  private static Map<String, Object> wrappedLoadSpec(String fingerprint, String projection)
  {
    return Map.of(
        "type", "partialProjection",
        "delegate", Map.of("type", "local", "path", "/var/druid/segments/foo"),
        "projections", List.of(projection),
        "fingerprint", fingerprint
    );
  }

  /**
   * The profile shape a historical announces after completing a partial load: the request it was given, plus the
   * footprint it actually materialized.
   */
  private static PartialLoadProfile loadedProfile(String fingerprint, String projection)
  {
    return PartialLoadProfile.forLoaded(wrappedLoadSpec(fingerprint, projection), fingerprint, REALIZED_BYTES);
  }

  /**
   * The profile shape the coordinator sends out with a load request: no footprint is known yet.
   */
  private static PartialLoadProfile requestProfile(String fingerprint, String projection)
  {
    return PartialLoadProfile.forRequest(wrappedLoadSpec(fingerprint, projection), fingerprint);
  }
}
