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

package org.apache.druid.indexing.worker.shuffle;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.worker.shuffle.ShuffleMetrics.PerDatasourceShuffleMetrics;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.Map;

public class ShuffleMetricsTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testShuffleRequested()
  {
    final ShuffleMetrics metrics = new ShuffleMetrics();
    final String supervisorTask1 = "supervisor1";
    final String supervisorTask2 = "supervisor2";
    final String supervisorTask3 = "supervisor3";
    metrics.shuffleRequested(supervisorTask1, 1024);
    metrics.shuffleRequested(supervisorTask2, 10);
    metrics.shuffleRequested(supervisorTask1, 512);
    metrics.shuffleRequested(supervisorTask3, 10000);
    metrics.shuffleRequested(supervisorTask2, 30);

    final Map<String, PerDatasourceShuffleMetrics> snapshot = metrics.snapshot();
    Assert.assertEquals(ImmutableSet.of(supervisorTask1, supervisorTask2, supervisorTask3), snapshot.keySet());

    PerDatasourceShuffleMetrics perDatasourceShuffleMetrics = snapshot.get(supervisorTask1);
    Assert.assertEquals(2, perDatasourceShuffleMetrics.getShuffleRequests());
    Assert.assertEquals(1536, perDatasourceShuffleMetrics.getShuffleBytes());

    perDatasourceShuffleMetrics = snapshot.get(supervisorTask2);
    Assert.assertEquals(2, perDatasourceShuffleMetrics.getShuffleRequests());
    Assert.assertEquals(40, perDatasourceShuffleMetrics.getShuffleBytes());

    perDatasourceShuffleMetrics = snapshot.get(supervisorTask3);
    Assert.assertEquals(1, perDatasourceShuffleMetrics.getShuffleRequests());
    Assert.assertEquals(10000, perDatasourceShuffleMetrics.getShuffleBytes());
  }

  @Test
  public void testSnapshotUnmodifiable()
  {
    expectedException.expect(UnsupportedOperationException.class);
    new ShuffleMetrics().snapshot().put("k", new PerDatasourceShuffleMetrics());
  }

  @Test
  public void testResetDatasourceMetricsAfterSnapshot()
  {
    final ShuffleMetrics shuffleMetrics = new ShuffleMetrics();
    shuffleMetrics.shuffleRequested("supervisor", 10);
    shuffleMetrics.shuffleRequested("supervisor", 10);
    shuffleMetrics.shuffleRequested("supervisor2", 10);
    shuffleMetrics.snapshot();

    Assert.assertEquals(Collections.emptyMap(), shuffleMetrics.getDatasourceMetrics());
  }
}
