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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.metrics.NoopTaskHolder;
import org.apache.druid.metadata.segment.cache.Metric;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.server.metrics.LatchableEmitterConfig;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class MetadataSegmentViewTest
{
  private MetadataSegmentView segmentView;
  private LatchableEmitter emitter;
  private CoordinatorClient coordinatorClient;

  @BeforeEach
  public void setup()
  {
    coordinatorClient = Mockito.mock(CoordinatorClient.class);
    emitter = new LatchableEmitter("", "", new LatchableEmitterConfig(null), new NoopTaskHolder());
    segmentView = new MetadataSegmentView(
        coordinatorClient,
        new BrokerSegmentWatcherConfig(),
        new BrokerSegmentMetadataCacheConfig()
        {
          @Override
          public long getMetadataSegmentPollPeriod()
          {
            return 100;
          }
        },
        emitter
    );
  }

  @Test
  public void test_start_triggersSegmentPollFromCoordinator_ifCacheIsEnabled()
  {
    // Set up the mock CoordinatorClient to return the expected list of segments
    final List<SegmentStatusInCluster> expectedSegments = CreateDataSegments
        .ofDatasource(TestDataSource.WIKI)
        .withNumPartitions(10)
        .eachOfSizeInMb(100)
        .stream()
        .map(s -> new SegmentStatusInCluster(s, false, 1, 100L, false))
        .toList();

    Mockito.when(
        coordinatorClient.fetchAllUsedSegmentsWithOvershadowedStatus(
            ArgumentMatchers.eq(null),
            ArgumentMatchers.eq(true)
        )
    ).thenReturn(
        Futures.immediateFuture(CloseableIterators.withEmptyBaggage(expectedSegments.iterator()))
    );

    // Start the test target and wait for it to sync with the Coordinator
    segmentView.start();
    emitter.waitForEvent(event -> event.hasMetricName(Metric.SYNC_DURATION_MILLIS));

    final List<SegmentStatusInCluster> observedSegments = new ArrayList<>();
    Iterators.addAll(observedSegments, segmentView.getSegments());
    Assertions.assertEquals(10, observedSegments.size());
    Assertions.assertEquals(expectedSegments, observedSegments);
  }
}
